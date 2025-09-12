# Clustering de Juegos

Esta etapa agrupa juegos a partir de sus embeddings.

- Script: `src/pipelines/run_clustering.py`
- Config: `configs/clustering.yaml`
- Entradas: `data/processed/embeddings/`
- Salidas:
  - `data/processed/clusters.parquet` (appid -> cluster_id [+ columnas soft])
  - `models/cluster_medoids.json` (centroides por clúster)
  - `outputs/clustering/cluster_stats.csv` (tamaño, similitud media al centroide, etc.)
  - `outputs/clustering/borderline_games.csv` (si aplica)

Métodos disponibles
- KMeans: local (scikit-learn) o distribuido (Spark ML) según `kmeans.threshold_spark`.
- Graph Leiden: construye grafo kNN con FAISS y particiona con Leiden.

Parámetros clave (configs/clustering.yaml)
- `method`: `kmeans` | `graph_leiden`
- `kmeans.threshold_spark`, `kmeans.k_range`, `kmeans.n_clusters`, `kmeans.seed`
- `faiss.index` (`FlatIP` por defecto), `faiss.use_gpu`
- `graph_leiden.k_neighbors`, `graph_leiden.sim_threshold`, `graph_leiden.resolution`
- Post-análisis: `post_analysis.soft_membership`, `post_analysis.borderline`, `post_analysis.consensus`

Lógica de Post-análisis (Leiden)
- Soft-membership: calcula probabilidades hacia centroides de clúster con softmax térmico (`temperature`).
- Borderline: marca juegos con margen de confianza bajo (percentil o umbral absoluto).
- Cluster Stats: tamaño, % borderline, medias de `p_assigned` y `confidence_margin`, y similitud promedio al centroide (coseno) por clúster.

Detalles avanzados
- Probabilidad (soft-membership): se calcula la similitud de cada juego a los centroides por clúster y se aplica `softmax(sim / tau)` donde `tau=temperature`. Devuelve `p_assigned` (máxima) y `p_second` (segunda mejor) para obtener `confidence_margin`.
- Juegos borderline: dos opciones:
  - `percentile`: marca como borderline el percentil `p` inferior del `confidence_margin` dentro de cada clúster.
  - `absolute`: marca los juegos con `confidence_margin <= threshold`.
- Consenso Leiden: ejecuta Leiden con múltiples `resolution` y usa un grafo de coasociación para obtener una partición más estable (si la versión de `leidenalg` expone utilidades de consenso).
- Alternativas de índice FAISS: `FlatIP` (exacto, rápido para MVP). Para volúmenes grandes se podrían evaluar `IVF*` o `HNSW` (no implementados en este repo por simplicidad).

Fallback automático a Spark KMeans
- Independientemente del `method` configurado, si el número de juegos `n_samples` ≥ `kmeans.threshold_spark`, el pipeline fuerza Spark KMeans y registra la decisión en MLflow (`fallback_to_spark_kmeans=true`). Ajusta el umbral en `configs/clustering.yaml`.

Ejecución
- Makefile: `make clustering` o `make clustering-check`
- DVC: `dvc repro clustering` (tras `embeddings`)

Evidencias gráficas sugeridas (colócalas en `docs/img/` y enlázalas aquí)
- Histograma de tamaños de clúster (`cluster_stats.size`).
- Distribución de `mean_sim_to_centroid` por clúster.
- Top clústeres con mayor % de borderline.
