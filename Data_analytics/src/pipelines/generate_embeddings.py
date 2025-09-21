#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Genera embeddings de juegos de Steam de forma paralelizable.
El script se conecta a MongoDB, carga los metadatos de los juegos, construye un
documento de texto para cada uno y genera los embeddings utilizando SentenceTransformer.
La paralelización se puede configurar para usar 'multiprocessing' o 'ray'.
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
import json
import mlflow
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

# Asegúrate de que los módulos de utils estén en el PATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils.io import write_parquet_any, makedirs_if_local
from src.utils.mlflow_utils import log_mlflow_params
from src.utils.mongo_utils import MongoLoader
from src.utils.faiss_utils import build_faiss_index, save_faiss_index
import re
import html as _html

# Función que realiza la tarea de embedding para un único worker/proceso.
def _embed_shard(docs_df: pd.DataFrame, model_name: str, normalize: bool, batch_size: int) -> pd.DataFrame:
    """Genera embeddings para un shard de datos."""

    print(f"[EMB ] Cargando modelo '{model_name}' para shard con {len(docs_df)} filas...")

    # Importar dentro de la función para evitar problemas de serialización en Ray/multiprocessing
    from sentence_transformers import SentenceTransformer
    try:
        model = SentenceTransformer(model_name)
    except Exception as e:
        print(f"Error cargando el modelo {model_name}: {e}")
        return pd.DataFrame(columns=['appid', 'embedding'])
        
    docs_list = docs_df['doc'].tolist()
    
    # Manejar el caso de una lista vacía
    if not docs_list:
        print("[EMB ] Shard vacío; devolviendo DataFrame vacío.")
        return pd.DataFrame(columns=['appid', 'embedding'])

    embeddings = model.encode(docs_list, normalize_embeddings=normalize, show_progress_bar=True, batch_size=batch_size)
    print(f"[EMB ] Shard embebido")

    docs_df['embedding'] = list(embeddings)
    return docs_df.drop(columns=['doc'])

def _embed_shard_worker(args):
    idx, shard_df, model_name, normalize, batch_size = args
    return idx, _embed_shard(shard_df, model_name, normalize, batch_size)



def _clean_text(s: str) -> str:
    """Limpieza ligera: quita HTML, URLs y normaliza espacios."""
    if not isinstance(s, str):
        s = str(s) if s is not None else ""
    # Unescape entidades HTML
    s = _html.unescape(s)
    # Quitar etiquetas HTML simples
    s = re.sub(r"<[^>]+>", " ", s)
    # Quitar URLs
    s = re.sub(r"https?://\S+|www\.\S+", " ", s)
    # Normalizar whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _join_tags(values) -> str:
    if not values:
        return ""
    return " ".join([str(t).strip().replace(" ", "_") for t in values if str(t).strip()])


def _build_doc(game: dict, doc_fields: dict, assemble_cfg: dict | None = None) -> str:
    """Construye el documento de texto con opciones de ensamblado/truncado.

    Compatibilidad hacia atrás: si assemble_cfg es None, usa el comportamiento previo
    (text_fields en orden seguido de tag_fields en orden), con el separador " \n".
    """
    assemble = assemble_cfg or {}
    use_prefixes = bool(assemble.get('use_prefixes', False))
    tags_first = bool(assemble.get('tags_first', False))
    clean = bool(assemble.get('clean_text', False))
    budgets = (assemble.get('budgets') or {})
    max_chars = assemble.get('document_max_chars')

    def maybe_clean(x: str) -> str:
        return _clean_text(x) if clean else (x if isinstance(x, str) else str(x))

    # Extraer secciones
    name = maybe_clean(game.get('name') or "") if 'name' in (doc_fields.get('text_fields') or []) else ""
    short = maybe_clean(game.get('short_description') or "") if 'short_description' in (doc_fields.get('text_fields') or []) else ""
    long = maybe_clean(game.get('detailed_description') or "") if 'detailed_description' in (doc_fields.get('text_fields') or []) else ""

    genres = _join_tags(game.get('genres', [])) if 'genres' in (doc_fields.get('tag_fields') or []) else ""
    categories = _join_tags(game.get('categories', [])) if 'categories' in (doc_fields.get('tag_fields') or []) else ""

    # Truncados por sección si hay presupuestos
    if budgets:
        if short and budgets.get('short_description'):
            short = short[: int(budgets['short_description'])]
        if long and budgets.get('detailed_description'):
            long = long[: int(budgets['detailed_description'])]

    # Ensamblar en orden deseado
    parts: list[str] = []
    if name:
        parts.append(name)

    def add_tags():
        if genres:
            parts.append(("GENRES: " + genres) if use_prefixes else genres)
        if categories:
            parts.append(("CATEGORIES: " + categories) if use_prefixes else categories)

    if tags_first:
        add_tags()

    # Textos
    # Mantener orden relativo short -> long
    if short:
        parts.append(short)
    if long:
        parts.append(long)

    if not tags_first:
        add_tags()

    # Unir y truncado global opcional
    doc = " \n".join([p for p in parts if p])
    if max_chars and isinstance(max_chars, int) and max_chars > 0:
        doc = doc[: max_chars]
    return doc

def main(cfg: dict):
    """
    Proceso principal para generar embeddings shardeados.
    """
    # 1. Cargar y preprocesar los datos
    mongo_cfg = cfg['mongo_connection']
    model_name = cfg['embedding_model']
    doc_fields = cfg['document_fields']
    
    print(f"[INFO] Conectando a MongoDB en {mongo_cfg['uri']}")
    loader = MongoLoader(mongo_cfg['uri'], mongo_cfg['database'], mongo_cfg['collection'])
    
    projection = {f: 1 for f in doc_fields.get('text_fields', []) + doc_fields.get('tag_fields', [])}
    projection['appid'] = 1
    
    print("[INFO] Cargando documentos de MongoDB y construyendo texto...")
    docs = []
    for game in loader.load_data(projection=projection):
        appid = game.get('appid')
        if not appid: continue
        doc = _build_doc(game, doc_fields, cfg.get('assemble'))
        if doc:
            docs.append({'appid': appid, 'doc': doc})
            
    if not docs:
        print("[WARN] No se encontraron documentos para procesar. Abortando.")
        return

    df = pd.DataFrame(docs)
    df['appid'] = df['appid'].astype(str)
    
    # 2. Configurar la paralelización
    para_cfg = cfg.get('parallelization', {})
    method = para_cfg.get('method', 'multiprocessing')
    n_shards = para_cfg.get('n_shards', 4)
    
    shards_list = np.array_split(df, n_shards)
    batch_size = int(cfg.get('embedding_batch_size', 256))
    
    print(f"[INFO] Generando embeddings en paralelo con '{method}' en {n_shards} shards...")

    output_uri = Path(cfg['output_paths']['embeddings_sharded_uri'])
    makedirs_if_local(output_uri)

    faiss_cfg = cfg.get('faiss_index', {})
    build_faiss = faiss_cfg.get('enabled', True)
    index_type = faiss_cfg.get('index', 'FlatIP')
    use_gpu = bool(faiss_cfg.get('use_gpu', False))
    faiss_index = None
    ids_accum = []
    total_embeddings = 0

    consolidated_path = Path('data/processed/embeddings.parquet')
    makedirs_if_local(consolidated_path.parent)
    parquet_writer = None

    def _process_shard(idx: int, res_df: pd.DataFrame) -> None:
        nonlocal faiss_index, parquet_writer, total_embeddings
        if res_df is None or res_df.empty:
            print(f"[WARN] Shard {idx} vacío; se omite.")
            return
        res_df = res_df.copy()
        res_df['appid'] = res_df['appid'].astype(str)

        shard_path = output_uri / f"part-{idx:04d}.parquet"
        makedirs_if_local(shard_path.parent)
        write_parquet_any(res_df, shard_path)
        print(f"[OK] Shard {idx} guardado en -> {shard_path}")
        try:
            mlflow.log_artifact(str(shard_path))
        except Exception:
            pass

        if build_faiss:
            vectors = np.vstack([np.asarray(e, dtype=np.float32) for e in res_df['embedding']])
            if faiss_index is None:
                faiss_index = build_faiss_index(vectors, index_type=index_type, use_gpu=use_gpu)
            else:
                faiss_index.add(vectors)
        ids_accum.extend(res_df['appid'].tolist())
        total_embeddings += len(res_df)

        table = pa.Table.from_pandas(res_df, preserve_index=False)
        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(str(consolidated_path), table.schema)
        parquet_writer.write_table(table)

    if method == 'ray':
        import ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)

        @ray.remote
        def ray_embed_shard(shard_df):
            return _embed_shard(shard_df, model_name, cfg.get('normalize_embeddings', True), batch_size)

        futures = [ray_embed_shard.remote(shard) for shard in shards_list]
        for idx, res_df in enumerate(ray.get(futures)):
            _process_shard(idx, res_df)
        ray.shutdown()

    elif method == 'multiprocessing':
        from multiprocessing import Pool
        tasks = [
            (i, shard, model_name, cfg.get('normalize_embeddings', True), batch_size)
            for i, shard in enumerate(shards_list)
        ]
        with Pool(n_shards) as pool:
            for idx, res_df in pool.imap_unordered(_embed_shard_worker, tasks):
                _process_shard(idx, res_df)
    elif method == 'spark':
        # Backend Spark para grandes volúmenes: procesa por particiones con mapInPandas
        from pyspark.sql import SparkSession
        from pyspark.sql import types as T
        spark_cfg = (para_cfg.get('spark') or {})
        num_partitions = int(spark_cfg.get('partitions', 200))
        driver_mem = spark_cfg.get('driver_memory', '6g')
        executor_mem = spark_cfg.get('executor_memory', '6g')


        print(f"[INFO] Usando Spark con {num_partitions} particiones para generar embeddings...")
        spark = (
            SparkSession.builder
            .appName("EmbeddingsSpark")
            .config("spark.driver.memory", driver_mem)
            .config("spark.executor.memory", executor_mem)
            .getOrCreate()
        )

        sdf = spark.createDataFrame(df[['appid', 'doc']])
        sdf = sdf.repartition(num_partitions)

        schema = T.StructType([
            T.StructField('appid', T.StringType(), nullable=False),
            T.StructField('embedding', T.ArrayType(T.FloatType()), nullable=False),
        ])

        normalize_flag = bool(cfg.get('normalize_embeddings', True))

        def _encode_iter(iterator):
            from sentence_transformers import SentenceTransformer  # lazy load en cada proceso
            import numpy as _np
            model = SentenceTransformer(model_name)
            for pdf in iterator:
                if pdf is None or pdf.empty:
                    yield pd.DataFrame(columns=['appid', 'embedding'])
                    continue
                docs_list = pdf['doc'].tolist()
                embs = model.encode(docs_list, normalize_embeddings=normalize_flag, show_progress_bar=False, batch_size=batch_size)
                out = pd.DataFrame({
                    'appid': pdf['appid'].astype(str).tolist(),
                    'embedding': [list(map(float, e.astype(_np.float32))) for e in embs],
                })
                yield out

        encoded = sdf.mapInPandas(_encode_iter, schema=schema)

        # 3.a Escribir directamente con Spark a la ruta shardeada
        output_uri = cfg['output_paths']['embeddings_sharded_uri']
        (encoded.write.mode('overwrite')
                .option('compression', 'snappy')
                .parquet(output_uri))
        print(f"[OK] Embeddings Spark escritos en -> {output_uri}")

        # Recolectar pequeño resumen para métricas
        try:
            n_rows = encoded.count()
            mlflow.log_metric("embeddings_generated", int(n_rows))
        except Exception:
            pass

        # En modo Spark no consolidamos ni construimos FAISS por defecto (escala)
        print("[INFO] Saltando consolidado único y FAISS en modo Spark (pensado para big data).")
        spark.stop()
        return
    else:
        raise ValueError(f"Método de paralelización '{method}' no soportado.")

    if parquet_writer is not None:
        parquet_writer.close()
        try:
            mlflow.log_artifact(str(consolidated_path))
        except Exception:
            pass
    else:
        # No hubo datos; crear parquet vacío para mantener contrato
        write_parquet_any(pd.DataFrame(columns=['appid', 'embedding']), consolidated_path)

    # 4. (Opcional) Construir y guardar índice FAISS persistente para ANN/kNN
    idx_cfg = cfg.get('faiss_index', {})
    if build_faiss:
        if faiss_index is not None:
            idx_path = Path(idx_cfg.get('path', 'models/embeddings.faiss'))
            ids_path = Path(idx_cfg.get('ids_path', 'models/emb_ids.json'))
            makedirs_if_local(idx_path.parent)
            save_faiss_index(faiss_index, str(idx_path))
            ids_path.write_text(json.dumps(ids_accum, ensure_ascii=False), encoding='utf-8')
            print(f"[OK] Índice FAISS guardado en -> {idx_path}")
            print(f"[OK] IDs guardados en -> {ids_path}")
            try:
                mlflow.log_artifact(str(idx_path))
                mlflow.log_artifact(str(ids_path))
            except Exception:
                pass
        else:
            print("[WARN] No se construyó el índice FAISS porque no se generaron embeddings.")
    print("[OK] Proceso de embeddings completado.")
    try:
        mlflow.log_metric("embeddings_generated", total_embeddings)
    except Exception:
        pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="Path to the YAML config file.")
    args = parser.parse_args()

    # Cargar la configuración
    with open(args.config, 'r') as f:
        cfg = yaml.safe_load(f)

    # Iniciar la ejecución de MLflow (sin utilidades de contexto personalizadas)
    try:
        mlflow.set_experiment("Embeddings Pipeline")
        with mlflow.start_run(run_name="embeddings-sharded-parallel"):
            log_mlflow_params(cfg)
            main(cfg)
    except Exception:
        # Fallback silencioso en caso de que MLflow no esté configurado
        main(cfg)
