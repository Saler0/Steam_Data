#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquesta los algoritmos de clustering (KMeans o Leiden) sobre embeddings de juegos.
La elecciÃ³n de la implementaciÃ³n (local vs. Spark) y las opciones de precisiÃ³n
avanzada se controlan desde un fichero de configuraciÃ³n YAML.
"""
import argparse
import yaml
import json
from pathlib import Path
from datetime import datetime
import numpy as np
import time
import pandas as pd
import faiss
import mlflow
import igraph as ig
import leidenalg as la
from typing import Dict, Any, List
import os
from pymongo import MongoClient, ReplaceOne

import os
import sys

# AsegÃºrate de que los mÃ³dulos de utils estÃ©n en el PATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Clustering local con scikit-learn
from sklearn.cluster import KMeans

# Clustering distribuido con PySpark
from pyspark.ml.clustering import KMeans as SparkKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql.functions import col, udf, lit, explode
from pyspark.sql.types import IntegerType, ArrayType, FloatType, StructType, StructField, StringType, DoubleType
from pyspark.ml.linalg import Vectors, VectorUDT

# Utils
from src.utils.io import makedirs_if_local, write_parquet_any, write_csv_any, read_parquet_any, path_exists
from src.utils.spark_utils import get_spark_session
from src.utils.mlflow_utils import start_mlflow_run, log_mlflow_params, log_mlflow_metrics, log_mlflow_artifacts
from src.utils.faiss_utils import build_faiss_index, search_faiss_index
from src.utils.config_utils import expand_env_in_obj

ABOUT_FIELD_CANDIDATES = [
    'about',
    'about_the_game',
    'about_the_game_text',
    'about_the_game_clean',
    'about_description',
]

TEXT_FIELD_CANDIDATES = [
    'short_description',
    'detailed_description',
    'about',
    'about_the_game',
    'about_the_game_text',
    'about_the_game_clean',
]

def _run_kmeans(df_emb: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Ejecuta el algoritmo KMeans de forma local o con Spark.
    """
    kmeans_cfg = cfg['kmeans']
    threshold = kmeans_cfg.get('threshold_spark', 100000)
    n_samples = len(df_emb)

    if n_samples >= threshold:
        print("[INFO] El nÃºmero de documentos supera el umbral. Usando Spark ML KMeans.")
        spark = get_spark_session("ClusteringSpark")
        
        list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
        spark_df = spark.createDataFrame(df_emb)
        spark_df = spark_df.withColumn("embedding_vec", list_to_vector_udf(col("embedding")))
        
        evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="embedding_vec", metricName="silhouette")
        best_k = -1
        best_score = -1
        
        k_range = kmeans_cfg['k_range']
        print(f"[INFO] Buscando k Ã³ptimo en el rango {k_range} con Spark...")
        for k in range(k_range[0], k_range[1] + 1):
            kmeans = SparkKMeans(k=k, seed=kmeans_cfg['seed'], featuresCol="embedding_vec")
            model = kmeans.fit(spark_df)
            predictions = model.transform(spark_df)
            score = evaluator.evaluate(predictions)
            if score > best_score:
                best_score = score
                best_k = k
        
        final_model = SparkKMeans(k=best_k, seed=kmeans_cfg['seed'], featuresCol="embedding_vec").fit(spark_df)
        predictions = final_model.transform(spark_df)
        df_clusters = predictions.select(col("appid"), col("prediction").alias("cluster_id")).toPandas()
        
        mlflow.log_param("k_final", best_k)
        mlflow.log_metric("best_silhouette_score", best_score)
        
        return df_clusters
    else:
        print("[INFO] El nÃºmero de documentos estÃ¡ por debajo del umbral. Usando scikit-learn (local).")
        X = np.vstack(df_emb['embedding'].apply(np.asarray).tolist()).astype(np.float32)
        k = kmeans_cfg.get('n_clusters', 50)
        kmeans = KMeans(n_clusters=k, random_state=kmeans_cfg['seed'], n_init=10)
        df_emb['cluster_id'] = kmeans.fit_predict(X)
        mlflow.log_param("k_final", k)
        mlflow.log_metric("inertia", kmeans.inertia_)
        return df_emb[['appid', 'cluster_id']]

def _run_leiden_local(df_emb: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Ejecuta el algoritmo de clustering Leiden, incluyendo opciones avanzadas como
    soft membership, detecciÃ³n de juegos borderline y clustering de consenso.
    """
    leiden_cfg = cfg['graph_leiden']
    
    X = np.vstack(df_emb['embedding'].apply(np.asarray).tolist()).astype(np.float32)
    faiss_cfg = cfg['faiss']
    faiss_index = build_faiss_index(X, faiss_cfg.get('index', 'FlatIP'), faiss_cfg.get('use_gpu', False))
    print("[INFO] Ãndice FAISS construido exitosamente.")

    print(f"[INFO] Buscando los {leiden_cfg['k_neighbors']} vecinos mÃ¡s cercanos...")
    D, I = search_faiss_index(faiss_index, X, leiden_cfg['k_neighbors'])
    
    print("[INFO] Creando grafo de similitud a partir de los vecinos...")
    mutual_knn = bool(leiden_cfg.get('mutual_knn', False))
    snn_cfg = (leiden_cfg.get('snn') or {})
    snn_enabled = bool(snn_cfg.get('enabled', False))
    snn_method = str(snn_cfg.get('method', 'count')).lower()
    min_shared = int(snn_cfg.get('min_shared_neighbors', 1))
    min_sim = float(leiden_cfg.get('sim_threshold', 0.25))

    neighbor_sets = []
    for row in I:
        neighbors = {int(idx) for idx in row[1:] if idx >= 0}
        neighbor_sets.append(neighbors)

    edges = []
    weights = []
    seen_edges = set()

    def add_edge(i: int, nbr: int, weight: float) -> None:
        key = (i, nbr) if i < nbr else (nbr, i)
        if key in seen_edges:
            return
        seen_edges.add(key)
        edges.append(key)
        weights.append(float(weight))

    for i in range(I.shape[0]):
        for j in range(1, I.shape[1]):
            nbr = int(I[i, j])
            if nbr < 0 or nbr == i:
                continue
            sim = float(D[i, j])
            if np.isnan(sim) or sim < min_sim:
                continue
            if mutual_knn and i not in neighbor_sets[nbr]:
                continue

            weight = sim
            if snn_enabled:
                shared = len(neighbor_sets[i] & neighbor_sets[nbr])
                if shared < min_shared:
                    continue
                if snn_method == 'jaccard':
                    union = len(neighbor_sets[i] | neighbor_sets[nbr]) or 1
                    weight = shared / union
                elif snn_method == 'overlap':
                    denom = min(len(neighbor_sets[i]), len(neighbor_sets[nbr])) or 1
                    weight = shared / denom
                else:  # count
                    weight = float(shared)
                if weight <= 0:
                    continue
            add_edge(i, nbr, weight)

    if not edges:
        print("[WARN] Ninguna arista superÃ³ los criterios configurados; relajando filtros (sin mutual ni SNN).")
        seen_edges.clear()
        for i in range(I.shape[0]):
            for j in range(1, I.shape[1]):
                nbr = int(I[i, j])
                if nbr < 0 or nbr == i:
                    continue
                sim = float(D[i, j])
                if np.isnan(sim) or sim < min_sim:
                    continue
                add_edge(i, nbr, sim)
        if not edges:
            print("[WARN] Grafo todavÃ­a vacÃ­o tras relajar filtros; usando vecinos mÃ¡s prÃ³ximos por defecto.")
            seen_edges.clear()
            for i in range(I.shape[0]):
                for j in range(1, min(I.shape[1], 3)):
                    nbr = int(I[i, j])
                    if nbr < 0 or nbr == i:
                        continue
                    sim = float(D[i, j])
                    if np.isnan(sim):
                        continue
                    add_edge(i, nbr, sim)

    print(f"[INFO] Grafo generado con {len(edges)} aristas (mutual={mutual_knn}, SNN={snn_enabled}).")



    # Asegurar que el grafo tenga todos los nodos, aunque haya aislados
    g = ig.Graph(n=I.shape[0], edges=edges, directed=False)
    g.es['weight'] = weights

    post_analysis_cfg = cfg.get('post_analysis', {})
    consensus_cfg = post_analysis_cfg.get('consensus', {})

    partition_key = str(leiden_cfg.get('partition', 'RB')).upper()
    partition_type = la.RBConfigurationVertexPartition if partition_key == 'RB' else la.ModularityVertexPartition

    if consensus_cfg.get('enabled', False):
        if not (hasattr(la, 'similarity_matrix') and hasattr(la, 'consensus_graph')):
            print(f"[INFO] Clustering de consenso no soportado en esta versiÃ³n de 'leidenalg'; usando Leiden simple con resolution={leiden_cfg['resolution']}.")
            part = la.find_partition(g, partition_type, resolution_parameter=leiden_cfg['resolution'])
            df_emb['cluster_id'] = part.membership
        else:
            print("[INFO] Ejecutando clustering de consenso de Leiden...")
            partitions = []
            try:
                for res in consensus_cfg['resolutions']:
                    partitions.append(la.find_partition(g, partition_type, resolution_parameter=res))
                coassoc_matrix = la.similarity_matrix(g, partitions)
                g_consensus = la.consensus_graph(g, coassoc_matrix, min_coassoc=consensus_cfg['min_coassoc'])
                final_partition = la.find_partition(g_consensus, partition_type)
                df_emb['cluster_id'] = final_partition.membership
            except Exception as ce:
                print(f"[INFO] Consenso no disponible ({ce}). Usando Leiden simple con resolution={leiden_cfg['resolution']}.")
                part = la.find_partition(g, partition_type, resolution_parameter=leiden_cfg['resolution'])
                df_emb['cluster_id'] = part.membership
    else:
        print("[INFO] Ejecutando algoritmo de Leiden simple...")
        partition = la.find_partition(g, partition_type, resolution_parameter=leiden_cfg['resolution'])
        df_emb['cluster_id'] = partition.membership

    if post_analysis_cfg.get('soft_membership', {}).get('enabled', False):
        print("[INFO] Calculando soft-membership y juegos borderline...")
        soft_t0 = time.perf_counter()
        n_items = len(df_emb)
        print(f"[INFO] Soft-membership: preparando centroides para {n_items} juegos; este paso puede tardar unos minutos...")
        # Se calcula la similitud con todos los clÃºsteres para obtener las probabilidades.
        cluster_centroids = np.array([np.mean(X[np.where(df_emb['cluster_id'] == c_id)], axis=0) for c_id in np.unique(df_emb['cluster_id'])]).astype(np.float32, copy=False)
        print(f"[INFO] Soft-membership: centroides listos ({cluster_centroids.shape[0]} clÃºsteres).")
        sims_to_centroids = (X @ cluster_centroids.T).astype(np.float32, copy=False)
        print('[INFO] Soft-membership: aplicando softmax por bloques sobre la matriz de similitudes...')

        tau = float(post_analysis_cfg['soft_membership'].get('temperature', 0.07) or 1.0)
        tau = max(tau, 1e-6)
        n_items, n_clusters = sims_to_centroids.shape
        target_entries = 64 * 1024 * 1024 // 4  # ~64MB por chunk en float32
        chunk_size = max(512, min(n_items, int(target_entries / max(1, n_clusters))))
        p_assigned = np.empty(n_items, dtype=np.float32)
        p_second = np.zeros(n_items, dtype=np.float32)

        for start in range(0, n_items, chunk_size):
            end = min(start + chunk_size, n_items)
            chunk = sims_to_centroids[start:end]
            chunk -= chunk.max(axis=1, keepdims=True)
            chunk /= tau
            np.exp(chunk, out=chunk)
            denom = chunk.sum(axis=1, keepdims=True)
            denom[denom == 0.0] = 1.0
            chunk /= denom
            p_assigned[start:end] = chunk.max(axis=1)
            if n_clusters > 1:
                p_second[start:end] = np.partition(chunk, -2, axis=1)[:, -2]
            if n_items > chunk_size:
                print(f"[INFO] Soft-membership: procesados {end}/{n_items} juegos.")

        df_emb['p_assigned'] = p_assigned

        df_emb['p_second'] = p_second
        
        df_emb['confidence_margin'] = df_emb['p_assigned'] - df_emb['p_second']
        df_emb['is_borderline'] = False
        
        borderline_cfg = post_analysis_cfg.get('borderline', {})
        if borderline_cfg.get('method') == 'percentile':
            min_margin_per_cluster = df_emb.groupby('cluster_id')['confidence_margin'].transform(
                lambda x: np.percentile(x, borderline_cfg.get('percentile', 10))
            )
            df_emb['is_borderline'] = df_emb['confidence_margin'] <= min_margin_per_cluster
        else:
            df_emb['is_borderline'] = df_emb['confidence_margin'] <= borderline_cfg.get('absolute_threshold', 0.05)

        soft_elapsed = time.perf_counter() - soft_t0
        print(f"[INFO] Soft-membership completado en {soft_elapsed:.1f}s.")
    
    print(f"[OK] Clustering de Leiden finalizado. Se encontraron {df_emb['cluster_id'].nunique()} clÃºsteres.")
    
    cols = ['appid', 'cluster_id']
    if 'is_borderline' in df_emb.columns:
        cols.extend(['is_borderline', 'confidence_margin', 'p_assigned', 'p_second'])
    return df_emb[cols]

def _compute_centroids(df_emb: pd.DataFrame, df_clusters: pd.DataFrame) -> Dict[int, list]:
    """Calcula centroides por clÃºster como media de embeddings."""
    joined = df_emb[['appid', 'embedding']].merge(df_clusters[['appid', 'cluster_id']], on='appid', how='inner')
    centroids: Dict[int, list] = {}
    for cid, grp in joined.groupby('cluster_id'):
        M = np.vstack(grp['embedding'].apply(np.asarray).to_list()).astype(np.float32)
        centroids[int(cid)] = np.mean(M, axis=0).astype(float).tolist()
    return centroids

def _compute_cluster_stats(df_emb: pd.DataFrame, df_clusters: pd.DataFrame) -> pd.DataFrame:
    """Devuelve estadÃ­sticas por clÃºster y calcula mÃ©tricas globales simples.

    Columnas: cluster_id, size, pct_borderline (si existe),
    mean_p_assigned, mean_confidence_margin (si existen).
    """
    base = df_clusters.groupby('cluster_id').size().rename('size').reset_index()
    # Enriquecer con mÃ©tricas de post-anÃ¡lisis si existen
    if 'is_borderline' in df_clusters.columns:
        tmp = df_clusters.groupby('cluster_id')['is_borderline'].mean().rename('pct_borderline')
        base = base.merge(tmp.reset_index(), on='cluster_id', how='left')
    if 'p_assigned' in df_clusters.columns:
        tmp = df_clusters.groupby('cluster_id')['p_assigned'].mean().rename('mean_p_assigned')
        base = base.merge(tmp.reset_index(), on='cluster_id', how='left')
    if 'confidence_margin' in df_clusters.columns:
        tmp = df_clusters.groupby('cluster_id')['confidence_margin'].mean().rename('mean_confidence_margin')
        base = base.merge(tmp.reset_index(), on='cluster_id', how='left')

    # Similitud promedio al centroide por clÃºster (producto interno)
    try:
        joined = df_emb[['appid', 'embedding']].merge(df_clusters[['appid', 'cluster_id']], on='appid', how='inner')
        sims = []
        for cid, grp in joined.groupby('cluster_id'):
            M = np.vstack(grp['embedding'].apply(np.asarray).to_list()).astype(np.float32)
            centroid = np.mean(M, axis=0)
            centroid = centroid / (np.linalg.norm(centroid) + 1e-12)
            M_norm = M / (np.linalg.norm(M, axis=1, keepdims=True) + 1e-12)
            cos = (M_norm @ centroid.reshape(-1, 1)).ravel()
            sims.append({'cluster_id': cid, 'mean_sim_to_centroid': float(np.mean(cos))})
        base = base.merge(pd.DataFrame(sims), on='cluster_id', how='left')
    except Exception as e:
        print(f"[WARN] No se pudo computar mean_sim_to_centroid: {e}")
    return base


def _normalize_metadata_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Select and normalize metadata columns required downstream."""
    if df.empty:
        return pd.DataFrame()
    if 'appid' not in df.columns or 'name' not in df.columns:
        return pd.DataFrame()
    meta = df.copy()
    try:
        meta['appid'] = meta['appid'].astype(str)
    except Exception:
        meta['appid'] = meta['appid'].apply(lambda x: str(x) if x is not None else None)
    for col in ('short_description', 'detailed_description', 'about'):
        if col not in meta.columns:
            meta[col] = None
    preferred = ['appid', 'name', 'short_description', 'detailed_description', 'about']
    other_cols = [col for col in meta.columns if col not in preferred]
    ordered_cols = preferred + other_cols
    normalized = (
        meta[ordered_cols]
        .dropna(subset=['appid', 'name'])
        .drop_duplicates('appid')
    )
    return normalized
def _build_name_lookup(df_emb: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """Return app metadata (name/about) based on embeddings or configured sources."""
    if 'name' in df_emb.columns:
        cols = ['appid', 'name']
        if 'about' in df_emb.columns:
            cols.append('about')
        return (
            df_emb[cols]
            .dropna(subset=['appid', 'name'])
            .drop_duplicates('appid')
        )

    input_paths = cfg.get('input_paths', {}) if isinstance(cfg, dict) else {}
    meta_path = input_paths.get('metadata_parquet') or cfg.get('metadata_parquet') or 'data/processed/game_metadata.parquet'
    meta_df = pd.DataFrame()

    if meta_path:
        if path_exists(meta_path):
            try:
                tmp_df = read_parquet_any(meta_path, engine='pyarrow')
            except Exception as exc:
                print(f"[WARN] Could not read metadata from {meta_path}: {exc}")
            else:
                if tmp_df.empty:
                    print(f"[WARN] Metadata at {meta_path} is empty; skipping metadata enrichment.")
                elif 'appid' not in tmp_df.columns or 'name' not in tmp_df.columns:
                    print(f"[WARN] Metadata at {meta_path} does not contain 'appid' and 'name'; skipping metadata enrichment.")
                else:
                    meta_candidate = _normalize_metadata_frame(tmp_df)
                    if meta_candidate.empty:
                        print(f"[WARN] Metadata at {meta_path} does not contain the expected columns; skipping metadata enrichment.")
                    else:
                        available = [col for col in ('short_description', 'detailed_description', 'about') if col in meta_candidate.columns and meta_candidate[col].notna().any()]
                        if available:
                            print(f"[INFO] Metadata columns available for enrichment: {', '.join(available)}")
                        missing = [col for col in ('short_description', 'detailed_description') if col in meta_candidate.columns and meta_candidate[col].isna().all()]
                        if missing:
                            print(f"[WARN] Metadata at {meta_path} is missing values for {', '.join(missing)}; topic profiling may have limited context.")
                        meta_df = meta_candidate
        else:
            print(f"[WARN] No game metadata found at {meta_path}; skipping metadata enrichment.")

    if not meta_df.empty:
        return meta_df

    mongo_cfg = cfg.get('metadata_mongo') if isinstance(cfg, dict) else None
    if not mongo_cfg:
        return pd.DataFrame()

    try:
        uri = mongo_cfg['uri']
        database = mongo_cfg['database']
        collection_name = mongo_cfg['collection']
    except KeyError as missing:
        print(f"[WARN] metadata_mongo config missing key: {missing}")
        return pd.DataFrame()

    projection_fields = mongo_cfg.get('projection') or ['appid', 'name']
    for field in ('appid', 'name'):
        if field not in projection_fields:
            projection_fields.append(field)
    for candidate in TEXT_FIELD_CANDIDATES:
        if candidate not in projection_fields:
            projection_fields.append(candidate)
    projection = {field: 1 for field in projection_fields}
    query = mongo_cfg.get('query') or {}

    client = None
    try:
        client = MongoClient(uri)
        cursor = client[database][collection_name].find(query, projection)
        def _first_non_empty(doc_obj, candidates):
            for field in candidates:
                value = doc_obj.get(field)
                if isinstance(value, str):
                    if value.strip():
                        return value
                elif value is not None:
                    return value
            return None

        rows = []
        for doc in cursor:
            appid = doc.get('appid')
            name = doc.get('name')
            if appid is None or name is None:
                continue
            try:
                appid = str(appid)
            except Exception:
                continue
            row = {'appid': appid, 'name': name}
            short_value = _first_non_empty(doc, ['short_description'])
            if short_value is not None:
                row['short_description'] = short_value
            detailed_value = _first_non_empty(doc, ['detailed_description', 'about_the_game', 'about_the_game_text', 'about_the_game_clean', 'about'])
            if detailed_value is not None:
                row['detailed_description'] = detailed_value
            about_value = _first_non_empty(doc, ['about', 'about_the_game', 'about_the_game_text', 'about_the_game_clean', 'detailed_description'])
            if about_value is not None:
                row['about'] = about_value
            rows.append(row)
        lookup_df = pd.DataFrame(rows)
        if lookup_df.empty:
            print(f"[WARN] metadata_mongo query returned no documents with 'appid' and 'name'.")
            return pd.DataFrame()
        print(f"[INFO] Loaded {len(lookup_df)} app metadata records from MongoDB ({database}.{collection_name}).")
        result = _normalize_metadata_frame(lookup_df)
        if result.empty:
            print(f"[WARN] Metadata obtained from MongoDB lacks the required text fields; skipping metadata enrichment.")
            return pd.DataFrame()
        if meta_path and not result.empty:
            try:
                makedirs_if_local(Path(meta_path).parent)
                write_parquet_any(result, meta_path)
                print(f"[INFO] Metadata guardada en -> {meta_path}")
            except Exception as exc:
                print(f"[WARN] No se pudo guardar metadata en {meta_path}: {exc}")
        return result
    except Exception as exc:
        print(f"[WARN] Could not load game metadata from MongoDB: {exc}")
        return pd.DataFrame()
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

def _attach_game_names(df_clusters: pd.DataFrame, name_lookup: pd.DataFrame) -> pd.DataFrame:
    """Merge game metadata (name/about) into the cluster dataframe when possible."""
    if name_lookup.empty or 'appid' not in name_lookup.columns:
        return df_clusters

    columns = ['appid']
    for field in ('name', 'about'):
        if field in name_lookup.columns:
            columns.append(field)

    lookup = name_lookup[columns].drop_duplicates('appid').copy()
    original_dtype = df_clusters['appid'].dtype
    try:
        lookup['appid'] = lookup['appid'].astype(original_dtype)
        merged = df_clusters.merge(lookup, on='appid', how='left')
        return merged
    except (TypeError, ValueError):
        pass
    except Exception as exc:
        print(f"[WARN] Could not attach game metadata to clusters: {exc}")
        return df_clusters
    try:
        lookup['appid'] = lookup['appid'].astype(str)
        merged = (
            df_clusters.assign(appid=df_clusters['appid'].astype(str))
            .merge(lookup, on='appid', how='left')
        )
        try:
            merged['appid'] = merged['appid'].astype(original_dtype)
        except (TypeError, ValueError):
            pass
        return merged
    except Exception as exc:
        print(f"[WARN] Could not attach game metadata to clusters: {exc}")
        return df_clusters

def _save_to_mongo(df: pd.DataFrame, cfg: Dict[str, Any]):
    """
    Guarda el DataFrame de resultados de clustering en una colecciÃ³n de MongoDB.
    """
    mongo_cfg = cfg['mongo_connection']
    client = MongoClient(mongo_cfg['uri'])
    db = client[mongo_cfg['database']]
    collection = db[mongo_cfg['collection']]
    
    print(f"[INFO] Conectando a MongoDB para guardar los resultados en la colecciÃ³n: {mongo_cfg['collection']}")

    operations = []
    for _, row in df.iterrows():
        doc = row.to_dict()
        doc['_id'] = str(doc['appid'])
        doc['processed_date'] = datetime.now()
        operations.append(ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
        
    if operations:
        try:
            result = collection.bulk_write(operations, ordered=False)
            print(f"[OK] {result.upserted_count} documentos insertados/actualizados en MongoDB.")
        except Exception as e:
            print(f"[ERROR] Error al guardar en MongoDB: {e}")
            mlflow.log_text(f"Error al guardar en MongoDB: {e}", "mongo_error.log")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Ruta al archivo de configuraciÃ³n YAML.")
    args = ap.parse_args()
    
    with open(args.config, 'r') as f:
        cfg = expand_env_in_obj(yaml.safe_load(f))
    
    mlflow_enabled = bool(cfg.get('mlflow', {}).get('enabled', True))
    run_name = cfg['mlflow'].get('run_name_prefix', "") + datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    from contextlib import nullcontext
    ctx = nullcontext()
    if mlflow_enabled:
        mlflow.set_experiment(cfg['mlflow'].get('experiment', 'Steam Analytics'))
        ctx = mlflow.start_run(run_name=run_name)
    
    with ctx:
        if mlflow_enabled:
            print(f"[INFO] Iniciando ejecuciÃ³n MLflow '{run_name}' para clustering.")
            log_mlflow_params(cfg)
        
        input_paths = cfg.get('input_paths', {})
        emb_path = input_paths.get("embeddings_sharded_uri", "data/processed/embeddings/")
        
        if not path_exists(emb_path):
             raise FileNotFoundError(f"No se encontraron embeddings en {emb_path}. Ejecute la etapa de embeddings primero.")
             
        print(f"[INFO] Leyendo embeddings desde {emb_path}...")
        df_emb = read_parquet_any(emb_path, engine='pyarrow')
        
        if df_emb.empty:
            print("[WARN] No se encontraron embeddings para procesar. Abortando.")
            return

        name_lookup = _build_name_lookup(df_emb, cfg)

        # SelecciÃ³n de mÃ©todo con fallback automÃ¡tico a Spark KMeans por umbral
        method = cfg['method']
        n_samples = len(df_emb)
        kmeans_cfg = cfg.get('kmeans', {})
        spark_threshold = int(kmeans_cfg.get('threshold_spark', 1_000_000))

        if method != 'kmeans' and n_samples >= spark_threshold:
            print(f"[INFO] Dataset grande (n={n_samples} >= {spark_threshold}). Anulando mÃ©todo '{method}' y usando Spark KMeans.")
            if mlflow_enabled:
                mlflow.log_param('fallback_to_spark_kmeans', True)
                mlflow.log_param('fallback_trigger_n_samples', n_samples)
            # Forzar Spark: ajustar threshold_spark a 0 en una copia superficial
            cfg_forced = {**cfg, 'method': 'kmeans', 'kmeans': {**kmeans_cfg, 'threshold_spark': 0}}
            df_clusters = _run_kmeans(df_emb, cfg_forced)
        else:
            print(f"[INFO] Iniciando el clustering con el mÃ©todo: {method}")
            if method == "kmeans":
                df_clusters = _run_kmeans(df_emb, cfg)
            elif method == "graph_leiden":
                df_clusters = _run_leiden_local(df_emb, cfg)
            else:
                raise ValueError(f"MÃ©todo de clustering '{method}' no soportado.")
            
        out_paths = cfg.get('output_paths', {})
        
        # 1. Guardar en disco (aÃ±adiendo versionado de clÃºster)
        out_clusters_path = Path(out_paths['clusters_parquet'])
        makedirs_if_local(out_clusters_path.parent)
        # Versionado simple: YYYYMM, configurable vÃ­a cfg['cluster_version']
        try:
            ver = str(cfg.get('cluster_version') or datetime.now().strftime('%Y%m'))
        except Exception:
            ver = datetime.now().strftime('%Y%m')
        try:
            as_of = pd.Timestamp.now().normalize()
        except Exception:
            as_of = pd.to_datetime(datetime.now().date())
        df_clusters = df_clusters.copy()
        df_clusters = _attach_game_names(df_clusters, name_lookup)
        df_clusters['cluster_version'] = ver
        df_clusters['as_of_date'] = as_of
        write_parquet_any(df_clusters, out_clusters_path)
        print(f"[OK] Clusters guardados en -> {out_clusters_path}")
        
        if not df_clusters.empty:
            n_clusters_found = len(df_clusters['cluster_id'].unique())
            if mlflow_enabled:
                mlflow.log_metric("n_clusters_found", n_clusters_found)
                mlflow.log_artifact(str(out_clusters_path))
            print(f"[OK] Se encontraron {n_clusters_found} clÃºsteres. Registrado en MLflow.")

            if 'is_borderline' in df_clusters.columns:
                df_borderline = df_clusters[df_clusters['is_borderline']]
                if not df_borderline.empty:
                    # Guardar ademÃ¡s en la ruta plana esperada por DVC/metrics
                    out_borderline_dir = Path(out_paths['borderline_dir'])
                    out_borderline_path_nested = out_borderline_dir / 'borderline_games.csv'
                    makedirs_if_local(out_borderline_dir)
                    write_csv_any(df_borderline, out_borderline_path_nested, index=False)

                    out_borderline_path = Path('outputs') / 'clustering' / 'borderline_games.csv'
                    makedirs_if_local(out_borderline_path.parent)
                    write_csv_any(df_borderline, out_borderline_path, index=False)

                    if mlflow_enabled:
                        mlflow.log_artifact(str(out_borderline_path_nested))
                        mlflow.log_artifact(str(out_borderline_path))
                        mlflow.log_metric("total_borderline_games", len(df_borderline))
                    print(f"[OK] Juegos borderline guardados en -> {out_borderline_path} y {out_borderline_path_nested}")

            # Guardar centroides/medoides para asignaciÃ³n de nuevos juegos
            try:
                centroids = _compute_centroids(df_emb, df_clusters)
                # Guardar centroides (medoids) para asignaciÃ³n diaria de nuevos juegos
                medoids_path = Path('models') / 'cluster_medoids.json'
                makedirs_if_local(medoids_path.parent)
                medoids_path.write_text(json.dumps({str(k): v for k, v in centroids.items()}), encoding='utf-8')
                if mlflow_enabled:
                    mlflow.log_artifact(str(medoids_path))
                print(f"[OK] Centroides guardados en -> {medoids_path}")
            except Exception as e:
                print(f"[WARN] No se pudieron calcular/guardar centroides: {e}")

            # Guardar estadÃ­sticas de clÃºster
            try:
                stats_df = _compute_cluster_stats(df_emb, df_clusters)
                out_stats_dir = Path(out_paths.get('stats_dir', 'outputs/clustering/stats'))
                makedirs_if_local(out_stats_dir)
                out_stats_path_nested = out_stats_dir / 'cluster_stats.csv'
                write_csv_any(stats_df, out_stats_path_nested, index=False)

                out_stats_path = Path('outputs') / 'clustering' / 'cluster_stats.csv'
                makedirs_if_local(out_stats_path.parent)
                write_csv_any(stats_df, out_stats_path, index=False)

                if mlflow_enabled:
                    mlflow.log_artifact(str(out_stats_path_nested))
                    mlflow.log_artifact(str(out_stats_path))
                    # MÃ©tricas globales simples
                    mlflow.log_metric('avg_cluster_size', float(stats_df['size'].mean()))
                    mlflow.log_metric('max_cluster_size', int(stats_df['size'].max()))
                    if 'mean_sim_to_centroid' in stats_df.columns:
                        mlflow.log_metric('avg_mean_sim_to_centroid', float(stats_df['mean_sim_to_centroid'].mean()))
                print(f"[OK] EstadÃ­sticas de clÃºster guardadas en -> {out_stats_path} y {out_stats_path_nested}")
            except Exception as e:
                print(f"[WARN] No se pudieron calcular/guardar estadÃ­sticas de clÃºster: {e}")

        # 2. Guardar en MongoDB
        _save_to_mongo(df_clusters, cfg)

        print("[INFO] Proceso de clustering completado.")
    
if __name__ == "__main__":
    main()



