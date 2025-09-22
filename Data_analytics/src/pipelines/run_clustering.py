#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Orquesta los algoritmos de clustering (KMeans o Leiden) sobre embeddings de juegos.
La elección de la implementación (local vs. Spark) y las opciones de precisión
avanzada se controlan desde un fichero de configuración YAML.
"""
import argparse
import yaml
import json
from pathlib import Path
from datetime import datetime
import numpy as np
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

# Asegúrate de que los módulos de utils estén en el PATH
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

def _run_kmeans(df_emb: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Ejecuta el algoritmo KMeans de forma local o con Spark.
    """
    kmeans_cfg = cfg['kmeans']
    threshold = kmeans_cfg.get('threshold_spark', 100000)
    n_samples = len(df_emb)

    if n_samples >= threshold:
        print("[INFO] El número de documentos supera el umbral. Usando Spark ML KMeans.")
        spark = get_spark_session("ClusteringSpark")
        
        list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
        spark_df = spark.createDataFrame(df_emb)
        spark_df = spark_df.withColumn("embedding_vec", list_to_vector_udf(col("embedding")))
        
        evaluator = ClusteringEvaluator(predictionCol="prediction", featuresCol="embedding_vec", metricName="silhouette")
        best_k = -1
        best_score = -1
        
        k_range = kmeans_cfg['k_range']
        print(f"[INFO] Buscando k óptimo en el rango {k_range} con Spark...")
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
        print("[INFO] El número de documentos está por debajo del umbral. Usando scikit-learn (local).")
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
    soft membership, detección de juegos borderline y clustering de consenso.
    """
    leiden_cfg = cfg['graph_leiden']
    
    X = np.vstack(df_emb['embedding'].apply(np.asarray).tolist()).astype(np.float32)
    faiss_cfg = cfg['faiss']
    faiss_index = build_faiss_index(X, faiss_cfg.get('index', 'FlatIP'), faiss_cfg.get('use_gpu', False))
    print("[INFO] Índice FAISS construido exitosamente.")

    print(f"[INFO] Buscando los {leiden_cfg['k_neighbors']} vecinos más cercanos...")
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
        print("[WARN] Ninguna arista superó los criterios configurados; relajando filtros (sin mutual ni SNN).")
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
            print("[WARN] Grafo todavía vacío tras relajar filtros; usando vecinos más próximos por defecto.")
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

    if consensus_cfg.get('enabled', False):
        print("[INFO] Ejecutando clustering de consenso de Leiden...")
        partitions = []
        partition_key = str(leiden_cfg.get('partition', 'RB')).upper()
        partition_type = la.RBConfigurationVertexPartition if partition_key == 'RB' else la.ModularityVertexPartition
        try:
            for res in consensus_cfg['resolutions']:
                partitions.append(la.find_partition(g, partition_type, resolution_parameter=res))
            # Algunas versiones de leidenalg no exponen estas utilidades
            coassoc_matrix = la.similarity_matrix(g, partitions)
            g_consensus = la.consensus_graph(g, coassoc_matrix, min_coassoc=consensus_cfg['min_coassoc'])
            final_partition = la.find_partition(g_consensus, partition_type)
            df_emb['cluster_id'] = final_partition.membership
        except Exception as ce:
            print(f"[WARN] Consenso no disponible ({ce}). Usando Leiden simple con resolution={leiden_cfg['resolution']}.")
            part = la.find_partition(g, partition_type, resolution_parameter=leiden_cfg['resolution'])
            df_emb['cluster_id'] = part.membership
    else:
        print("[INFO] Ejecutando algoritmo de Leiden simple...")
        partition_key = str(leiden_cfg.get('partition', 'RB')).upper()
        partition_type = la.RBConfigurationVertexPartition if partition_key == 'RB' else la.ModularityVertexPartition
        partition = la.find_partition(g, partition_type, resolution_parameter=leiden_cfg['resolution'])
        df_emb['cluster_id'] = partition.membership

    if post_analysis_cfg.get('soft_membership', {}).get('enabled', False):
        print("[INFO] Calculando soft-membership y juegos borderline...")
        # Se calcula la similitud con todos los clústeres para obtener las probabilidades.
        cluster_centroids = np.array([np.mean(X[np.where(df_emb['cluster_id'] == c_id)], axis=0) for c_id in np.unique(df_emb['cluster_id'])])
        sims_to_centroids = X @ cluster_centroids.T
        
        def softmax(x, tau=1.0):
            e_x = np.exp((x - np.max(x)) / tau)
            return e_x / e_x.sum(axis=0)

        tau = post_analysis_cfg['soft_membership'].get('temperature', 0.07)
        probs = np.apply_along_axis(softmax, 1, sims_to_centroids, tau)
        
        df_emb['p_assigned'] = np.max(probs, axis=1)
        
        second_best_probs = np.partition(probs, -2, axis=1)[:, -2]
        df_emb['p_second'] = second_best_probs
        
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
    
    print(f"[OK] Clustering de Leiden finalizado. Se encontraron {df_emb['cluster_id'].nunique()} clústeres.")
    
    cols = ['appid', 'cluster_id']
    if 'is_borderline' in df_emb.columns:
        cols.extend(['is_borderline', 'confidence_margin', 'p_assigned', 'p_second'])
    return df_emb[cols]

def _compute_centroids(df_emb: pd.DataFrame, df_clusters: pd.DataFrame) -> Dict[int, list]:
    """Calcula centroides por clúster como media de embeddings."""
    joined = df_emb[['appid', 'embedding']].merge(df_clusters[['appid', 'cluster_id']], on='appid', how='inner')
    centroids: Dict[int, list] = {}
    for cid, grp in joined.groupby('cluster_id'):
        M = np.vstack(grp['embedding'].apply(np.asarray).to_list()).astype(np.float32)
        centroids[int(cid)] = np.mean(M, axis=0).astype(float).tolist()
    return centroids

def _compute_cluster_stats(df_emb: pd.DataFrame, df_clusters: pd.DataFrame) -> pd.DataFrame:
    """Devuelve estadísticas por clúster y calcula métricas globales simples.

    Columnas: cluster_id, size, pct_borderline (si existe),
    mean_p_assigned, mean_confidence_margin (si existen).
    """
    base = df_clusters.groupby('cluster_id').size().rename('size').reset_index()
    # Enriquecer con métricas de post-análisis si existen
    if 'is_borderline' in df_clusters.columns:
        tmp = df_clusters.groupby('cluster_id')['is_borderline'].mean().rename('pct_borderline')
        base = base.merge(tmp.reset_index(), on='cluster_id', how='left')
    if 'p_assigned' in df_clusters.columns:
        tmp = df_clusters.groupby('cluster_id')['p_assigned'].mean().rename('mean_p_assigned')
        base = base.merge(tmp.reset_index(), on='cluster_id', how='left')
    if 'confidence_margin' in df_clusters.columns:
        tmp = df_clusters.groupby('cluster_id')['confidence_margin'].mean().rename('mean_confidence_margin')
        base = base.merge(tmp.reset_index(), on='cluster_id', how='left')

    # Similitud promedio al centroide por clúster (producto interno)
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

def _build_name_lookup(df_emb: pd.DataFrame, cfg: Dict[str, Any]) -> pd.DataFrame:
    """Return appid/name pairs based on embeddings or configured metadata."""
    if 'name' in df_emb.columns:
        return df_emb[['appid', 'name']].dropna(subset=['name']).drop_duplicates('appid')
    input_paths = cfg.get('input_paths', {}) if isinstance(cfg, dict) else {}
    meta_path = input_paths.get('metadata_parquet') or cfg.get('metadata_parquet') or 'data/processed/game_metadata.parquet'
    if not meta_path:
        return pd.DataFrame()
    if not path_exists(meta_path):
        print(f"[WARN] No game metadata found at {meta_path}; skipping name enrichment.")
        return pd.DataFrame()
    try:
        meta_df = read_parquet_any(meta_path, engine='pyarrow')
    except Exception as exc:
        print(f"[WARN] Could not read metadata from {meta_path}: {exc}")
        return pd.DataFrame()
    if meta_df.empty:
        print(f"[WARN] Metadata at {meta_path} is empty; skipping name enrichment.")
        return pd.DataFrame()
    if 'appid' not in meta_df.columns or 'name' not in meta_df.columns:
        print(f"[WARN] Metadata at {meta_path} does not contain 'appid' and 'name'; skipping name enrichment.")
        return pd.DataFrame()
    print(f"[INFO] Game metadata loaded from {meta_path} to enrich clusters with names.")
    return meta_df[['appid', 'name']].dropna(subset=['appid', 'name']).drop_duplicates('appid')

def _attach_game_names(df_clusters: pd.DataFrame, name_lookup: pd.DataFrame) -> pd.DataFrame:
    """Merge game names into the cluster dataframe when possible."""
    if name_lookup.empty or 'appid' not in name_lookup.columns or 'name' not in name_lookup.columns:
        return df_clusters
    lookup = name_lookup[['appid', 'name']].drop_duplicates('appid').copy()
    original_dtype = df_clusters['appid'].dtype
    try:
        lookup['appid'] = lookup['appid'].astype(original_dtype)
        merged = df_clusters.merge(lookup, on='appid', how='left')
        return merged
    except (TypeError, ValueError):
        pass
    except Exception as exc:
        print(f"[WARN] Could not attach game names to clusters: {exc}")
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
        print(f"[WARN] Could not attach game names to clusters: {exc}")
        return df_clusters

def _save_to_mongo(df: pd.DataFrame, cfg: Dict[str, Any]):
    """
    Guarda el DataFrame de resultados de clustering en una colección de MongoDB.
    """
    mongo_cfg = cfg['mongo_connection']
    client = MongoClient(mongo_cfg['uri'])
    db = client[mongo_cfg['database']]
    collection = db[mongo_cfg['collection']]
    
    print(f"[INFO] Conectando a MongoDB para guardar los resultados en la colección: {mongo_cfg['collection']}")

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
    ap.add_argument("--config", required=True, help="Ruta al archivo de configuración YAML.")
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
            print(f"[INFO] Iniciando ejecución MLflow '{run_name}' para clustering.")
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

        # Selección de método con fallback automático a Spark KMeans por umbral
        method = cfg['method']
        n_samples = len(df_emb)
        kmeans_cfg = cfg.get('kmeans', {})
        spark_threshold = int(kmeans_cfg.get('threshold_spark', 1_000_000))

        if method != 'kmeans' and n_samples >= spark_threshold:
            print(f"[INFO] Dataset grande (n={n_samples} >= {spark_threshold}). Anulando método '{method}' y usando Spark KMeans.")
            if mlflow_enabled:
                mlflow.log_param('fallback_to_spark_kmeans', True)
                mlflow.log_param('fallback_trigger_n_samples', n_samples)
            # Forzar Spark: ajustar threshold_spark a 0 en una copia superficial
            cfg_forced = {**cfg, 'method': 'kmeans', 'kmeans': {**kmeans_cfg, 'threshold_spark': 0}}
            df_clusters = _run_kmeans(df_emb, cfg_forced)
        else:
            print(f"[INFO] Iniciando el clustering con el método: {method}")
            if method == "kmeans":
                df_clusters = _run_kmeans(df_emb, cfg)
            elif method == "graph_leiden":
                df_clusters = _run_leiden_local(df_emb, cfg)
            else:
                raise ValueError(f"Método de clustering '{method}' no soportado.")
            
        out_paths = cfg.get('output_paths', {})
        
        # 1. Guardar en disco (añadiendo versionado de clúster)
        out_clusters_path = Path(out_paths['clusters_parquet'])
        makedirs_if_local(out_clusters_path.parent)
        # Versionado simple: YYYYMM, configurable vía cfg['cluster_version']
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
            print(f"[OK] Se encontraron {n_clusters_found} clústeres. Registrado en MLflow.")

            if 'is_borderline' in df_clusters.columns:
                df_borderline = df_clusters[df_clusters['is_borderline']]
                if not df_borderline.empty:
                    # Guardar además en la ruta plana esperada por DVC/metrics
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

            # Guardar centroides/medoides para asignación de nuevos juegos
            try:
                centroids = _compute_centroids(df_emb, df_clusters)
                # Guardar centroides (medoids) para asignación diaria de nuevos juegos
                medoids_path = Path('models') / 'cluster_medoids.json'
                makedirs_if_local(medoids_path.parent)
                medoids_path.write_text(json.dumps({str(k): v for k, v in centroids.items()}), encoding='utf-8')
                if mlflow_enabled:
                    mlflow.log_artifact(str(medoids_path))
                print(f"[OK] Centroides guardados en -> {medoids_path}")
            except Exception as e:
                print(f"[WARN] No se pudieron calcular/guardar centroides: {e}")

            # Guardar estadísticas de clúster
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
                    # Métricas globales simples
                    mlflow.log_metric('avg_cluster_size', float(stats_df['size'].mean()))
                    mlflow.log_metric('max_cluster_size', int(stats_df['size'].max()))
                    if 'mean_sim_to_centroid' in stats_df.columns:
                        mlflow.log_metric('avg_mean_sim_to_centroid', float(stats_df['mean_sim_to_centroid'].mean()))
                print(f"[OK] Estadísticas de clúster guardadas en -> {out_stats_path} y {out_stats_path_nested}")
            except Exception as e:
                print(f"[WARN] No se pudieron calcular/guardar estadísticas de clúster: {e}")

        # 2. Guardar en MongoDB
        _save_to_mongo(df_clusters, cfg)

        print("[INFO] Proceso de clustering completado.")
    
if __name__ == "__main__":
    main()
