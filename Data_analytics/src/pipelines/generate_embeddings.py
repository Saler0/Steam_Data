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
import mlflow
import os
import sys

# Asegúrate de que los módulos de utils estén en el PATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils.io import write_parquet_any, makedirs_if_local
from src.utils.mlflow_utils import log_mlflow_params
from src.utils.mongo_utils import MongoLoader
from src.utils.faiss_utils import build_faiss_index, save_faiss_index

# Función que realiza la tarea de embedding para un único worker/proceso.
def _embed_shard(docs_df: pd.DataFrame, model_name: str, normalize: bool, batch_size: int) -> pd.DataFrame:
    """Genera embeddings para un shard de datos."""
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
        return pd.DataFrame(columns=['appid', 'embedding'])

    embeddings = model.encode(docs_list, normalize_embeddings=normalize, show_progress_bar=False, batch_size=batch_size)
    
    docs_df['embedding'] = list(embeddings)
    return docs_df.drop(columns=['doc'])

def _build_doc(game: dict, doc_fields: dict) -> str:
    """Construye un documento de texto a partir de los campos del juego."""
    parts = []
    for field in doc_fields.get('text_fields', []):
        if game.get(field):
            parts.append(str(game[field]))
    for field in doc_fields.get('tag_fields', []):
        tags = game.get(field, [])
        if tags:
            parts.append(" ".join([str(t).replace(" ", "_") for t in tags]))
    return " \n".join([p for p in parts if p])

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
        doc = _build_doc(game, doc_fields)
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

    if method == 'ray':
        import ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True)
            
        @ray.remote
        def ray_embed_shard(shard_df):
            return _embed_shard(shard_df, model_name, cfg.get('normalize_embeddings', True), batch_size)
            
        futures = [ray_embed_shard.remote(shard) for shard in shards_list]
        results = ray.get(futures)
        ray.shutdown()
        
    elif method == 'multiprocessing':
        from multiprocessing import Pool
        with Pool(n_shards) as pool:
            results = pool.starmap(_embed_shard, [(shard, model_name, cfg.get('normalize_embeddings', True), batch_size) for shard in shards_list])
    else:
        raise ValueError(f"Método de paralelización '{method}' no soportado.")
        
    # 3. Guardar los resultados shardeados y consolidados
    output_uri = cfg['output_paths']['embeddings_sharded_uri']

    all_embeddings_df = pd.concat(results)

    # Guardar cada shard en su propio archivo Parquet
    for i, res_df in enumerate(results):
        shard_path = Path(output_uri) / f"part-{i:04d}.parquet"
        makedirs_if_local(shard_path.parent)
        write_parquet_any(res_df, shard_path)
        print(f"[OK] Shard {i} guardado en -> {shard_path}")
        try:
            mlflow.log_artifact(str(shard_path))
        except Exception:
            pass

    # Guardar parquet consolidado para consumidores que prefieren 1 archivo
    consolidated_path = Path('data/processed/embeddings.parquet')
    makedirs_if_local(consolidated_path.parent)
    write_parquet_any(all_embeddings_df, consolidated_path)
    print(f"[OK] Embeddings consolidados en -> {consolidated_path}")
    try:
        mlflow.log_artifact(str(consolidated_path))
    except Exception:
        pass

    # 4. (Opcional) Construir y guardar índice FAISS persistente para ANN/kNN
    idx_cfg = cfg.get('faiss_index', {})
    if idx_cfg.get('enabled', True):
        index_type = idx_cfg.get('index', 'FlatIP')
        use_gpu = bool(idx_cfg.get('use_gpu', False))
        try:
            X = np.vstack(all_embeddings_df['embedding'].apply(np.asarray).to_list()).astype(np.float32)
            index = build_faiss_index(X, index_type=index_type, use_gpu=use_gpu)
            idx_path = Path(idx_cfg.get('path', 'models/embeddings.faiss'))
            ids_path = Path(idx_cfg.get('ids_path', 'models/emb_ids.json'))
            makedirs_if_local(idx_path.parent)
            save_faiss_index(index, str(idx_path))
            # Guardar mapeo de appids en orden
            ids_path.write_text(
                __import__('json').dumps(all_embeddings_df['appid'].astype(str).tolist(), ensure_ascii=False),
                encoding='utf-8'
            )
            print(f"[OK] Índice FAISS guardado en -> {idx_path}")
            print(f"[OK] IDs guardados en -> {ids_path}")
            try:
                mlflow.log_artifact(str(idx_path))
                mlflow.log_artifact(str(ids_path))
            except Exception:
                pass
        except Exception as e:
            print(f"[WARN] No se pudo construir/guardar índice FAISS: {e}")

    print("[OK] Proceso de embeddings completado.")
    try:
        mlflow.log_metric("embeddings_generated", len(all_embeddings_df))
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
