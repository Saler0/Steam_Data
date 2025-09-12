# Opciones y Alternativas del Pipeline

Este documento resume las alternativas configurables por etapa y cómo elegirlas.

Embeddings (`configs/embeddings.yaml`)
- Modelo: `embedding_model` (MiniLM por defecto); trade-off entre precisión y coste.
- Normalización: `normalize_embeddings` (recomendable para similitud por producto interno).
- Paralelización: `parallelization.method` (`multiprocessing` | `ray`) y `n_shards`.
- FAISS persistente: `faiss_index.enabled`, `index` (FlatIP), `use_gpu`.

Clustering (`configs/clustering.yaml`)
- Método: `method` = `kmeans` | `graph_leiden`.
  - `kmeans`: local (scikit) o Spark (`threshold_spark`, `k_range` para selección por silueta, o `n_clusters`).
  - `graph_leiden`: construcción kNN con FAISS (`k_neighbors`, `sim_threshold`) + `resolution` (número de clústeres indirecto).
- Post-análisis:
  - `soft_membership.enabled` + `temperature` (probabilidades y margin/confianza).
  - `borderline.method` (`percentile`/`absolute`).
  - `consensus.enabled` + `resolutions` + `min_coassoc` para mayor estabilidad (si la lib lo permite).

Eventos (`configs/events.yaml`)
- Paralelización: `parallelization.mode` (`ray` | `multiprocessing`). Fallback automático si no hay Ray.
- Preagregados: `preaggregated.reviews_monthly`, `preaggregated.players_monthly` (opcional para escala).
- Señales externas: `signals.twitch|youtube` (`file` CSV o `api`) y `dlc.enabled` (Mongo).

Tópicos (BERTopic)
- Ventana: `bertopic.window_months`, tamaño mínimo: `min_topic_size`, min_df: `min_df`.
- Ponderación: `weighting.enabled|method|cap|min_len`.
- Coherencia C_v (opcional): requiere `gensim`; registra promedio en MLflow.

Noticias (Clasificador LLM)
- Config LLM: `llm.model_id`, `server_url`, `max_new_tokens`, `temperature`, `news_labels`, `mlflow_experiment`.
- Mongo noticias: `mongodb.uri|db_name|collection_name`.
- Modo: por `--appid` o batch (todos los appids distintos).

CCF/Granger (`configs/ccf_analysis.yaml`)
- Transformaciones hacia estacionariedad: orden y parámetros en `stationarity.*` (ADF `alpha`, `seasonal.period`).
- Pares a analizar: `ccf_pairs[*]` y `ccf_lags`.
- Granger: `maxlag`, `alpha` + corrección FDR.

Reportes
- Preferencia por FAISS persistente para vecinos (`models/embeddings.faiss`) con fallback a producto interno.
- Filtro opcional por clúster para generación masiva (`cluster_filter`).

