Steam Game Analytics — Pipeline y Uso

Resumen del pipeline de analytics para juegos de Steam: embeddings → clustering → eventos/tópicos/CCF/noticias → enriquecimiento → reportes y vistas. Incluye orquestación con DVC y soporte Docker.

## Modos de Ejecución: MVP vs Big Data

Este repositorio soporta dos formas de ejecutar el pipeline, pensadas para necesidades distintas:

- MVP (local/rápido): orientado a iterar, depurar y correr en una sola máquina o dentro del contenedor `app` del `docker-compose`.
- Big Data (producción/distribuido): orientado a datasets grandes y ejecución distribuida con Spark y Ray.

### MVP (local/rápido)
- Componentes principales:
  - Embeddings y features con Python (pandas, sentence-transformers).
  - Clustering local con `scikit-learn.KMeans` o por grafo con `Leiden` (FAISS para kNN).
  - Detección de eventos en CPU: `src/pipelines/event_detection/detect_events.py`.
  - Tópicos con BERTopic: `src/insights/topic_motives.py`.
  - CCF/Granger con `statsmodels`: `src/pipelines/ccf_analysis/analyze_competitors_ccf.py`.
  - Enriquecimiento (Twitch/YouTube/DLC) y clasificación de noticias: `src/pipelines/event_detection/enrich_events.py`, `src/insights/news_classifier.py`.
- Almacenamiento por defecto:
  - Parquet local bajo `data/` y `outputs/`.
  - Artefactos en `models/` (p. ej., `cluster_medoids.json`, índices FAISS opcionales).
  - MLflow local (file store) vía el servicio `mlflow` del `docker-compose.yml`.
- Cómo ejecutar (ejemplos):
  - Ejecutar todo lo principal: `dvc repro embeddings clustering events topics news_classifier enrich ccf report editor_view`
  - Con Makefile (dentro de Docker): `make run-all`
  - Solo clustering: `make clustering`
  - Reporte por juego: `APPID=12345 make report`
- Configuración relevante:
  - `configs/clustering.yaml`: `method: kmeans` o `graph_leiden`; sección `faiss` (índice y uso de GPU), y tracking en MLflow.
  - `configs/events.yaml`: rutas de entradas/salidas, opciones de BERTopic y LLM, `preaggregated`.
  - `configs/ccf_analysis.yaml`: pares, filtros y paths para preagregados.
- Cuándo usarlo:
  - Exploración, prototipado, conjuntos medianos/pequeños, CI local y desarrollo.

### Big Data (producción/distribuido)
- Componentes principales:
  - Clustering con Spark ML (`pyspark.ml.KMeans`) cuando el dataset supera un umbral configurable o usando una config dedicada (`configs/clustering_spark.yaml`).
  - Detección de eventos con Spark SQL/DataFrames: `src/pipelines/event_detection/events_spark.py` (stage DVC `events_spark`).
  - Pipeline de tópicos híbrido: preparación con Spark (`src/insights/topics_prep_spark.py`) y modelado paralelo con Ray (`src/insights/topics_from_prep_ray.py`) — stages `topics_spark_prep` y `topics_ray`.
  - CCF distribuido: `src/pipelines/ccf_analysis/ccf_spark.py` (stage `ccf_spark`).
  - Preagregados de reseñas con intento Spark + fallback: `src/pipelines/preaggregations/reviews_monthly.py`.
- Almacenamiento recomendado:
  - Parquet particionado en data lake (`s3://`, `gs://`, ADLS). El repo ya soporta `gs://` vía `fsspec/gcsfs` (ver utilidades en `src/utils/io.py`).
  - Parametriza rutas en `configs/*.yaml` (p. ej., `preaggregated`, `output_paths`, `input_paths`).
- Orquestación y tracking:
  - DVC es la fuente de verdad del DAG (`dvc.yaml`).
  - Ejemplos de orquestación: Airflow (`airflow/dags/steam_analytics_dag.py`) y Prefect (`prefect/flow_steam_analytics.py`) invocando `dvc repro` por etapa.
  - MLflow como tracking; en producción se recomienda backend SQL + artifact store en object storage (ajusta `MLFLOW_TRACKING_URI`).
- Cómo ejecutar (local con Docker o en cluster):
  - Eventos con Spark: `make events-spark`
  - Tópicos Spark + Ray: `make topics-prep && make topics-ray`
  - CCF con Spark: `make ccf-spark`
  - Clustering con Spark ML: usar `configs/clustering_spark.yaml` o ajustar `kmeans.threshold_spark` y ejecutar `make clustering`.
  - En producción real: usar `spark-submit` en tu cluster (K8s/YARN/Dataproc/EMR) y apuntar las rutas a `s3://`/`gs://`.
- Configuración relevante:
  - `configs/clustering_spark.yaml`: rutas de embeddings, parámetros de KMeans en Spark, MLflow y Mongo (para persistencia opcional de clusters).
  - `configs/events.yaml` y `configs/ccf_analysis.yaml`: secciones `preaggregated` y rutas de lectura/escritura; funcionan con `gs://`/`s3://` si tus credenciales están configuradas.
- Consideraciones de escala:
  - Ajusta `spark.sql.shuffle.partitions`, `maxRecordsPerFile` y `partitionBy` en escrituras (ver ejemplos en `events_spark.py` y `reviews_monthly.py`).
  - Evita UDFs en lo posible; usa funciones nativas (por ejemplo, `array_to_vector` en lugar de UDFs para embeddings en Spark ML).
- Outputs esperados:
  - Los mismos artefactos funcionales que en el modo MVP, pero generados a escala: `data/processed/clusters.parquet`, `outputs/events/*.parquet`, `outputs/ccf_analysis/summary.parquet`, reportes JSON, etc.

### Cómo elegir el modo
- Usa MVP para desarrollo, validación rápida y datasets manejables en una máquina.
- Usa Big Data si:
  - El número de juegos/documentos supera el umbral `kmeans.threshold_spark` o el procesamiento local es lento/insuficiente.
  - Requieres preagregados/reporte sobre millones de filas de reseñas o señales externas.
  - Necesitas paralelismo real y reintentos/orquestación a nivel de cluster.

Novedades recientes
- Embeddings generan shards en `data/processed/embeddings/` y Parquet consolidado en `data/processed/embeddings.parquet`.
- Añadido `MongoLoader` y corrección de MLflow en embeddings.
- Enriquecimiento de eventos implementado (Twitch/YouTube/DLC + noticias clasificadas + tópicos etiquetados) en `src/pipelines/event_detection/enrich_events.py`.
- BERTopic con ponderación opcional por votos de utilidad de reseñas (`configs/events.yaml` → `bertopic.weighting`).
- Índice FAISS persistente opcional (`configs/embeddings.yaml: faiss_index`) y uso transparente para vecinos en reportes.
- Preagregados mensuales opcionales para escala (`configs/events.yaml` y `configs/ccf_analysis.yaml: preaggregated`).
- DVC alineado a outputs reales; Makefile y `.bat` actualizados al servicio `app/steam-analytics`.
- Plantilla de contrato de datos en `docs/DATA_CONTRACT.yaml`.

Requisitos
- Docker y Docker Compose (para entorno recomendado).
- Python 3.11 si ejecutas local. Dependencias en `requirements.txt` (incluye `ray`, `bertopic`, `gensim`, `faiss-cpu`, `pyspark`).
- Servicios opcionales: MongoDB y MLflow (levantados por `docker-compose.yml`).

Configuración
- Variables `.env` (ver `.env.example`).
- Configs clave:
- `configs/embeddings.yaml`: conexión a Mongo (juegos) y opciones de paralelización.
  - Sección `faiss_index` para generar `models/embeddings.faiss` y `models/emb_ids.json` (ANN persistente).
- `configs/clustering.yaml`: método `kmeans` o `graph_leiden`, FAISS, MLflow.
  - Fallback automático: si `method` no es `kmeans` y el tamaño del dataset ≥ `kmeans.threshold_spark`, el pipeline fuerza Spark KMeans (se registra en MLflow).
- `configs/events.yaml`: salidas, `embeddings_parquet`, `clusters_parquet`, `metadata_parquet`, `bertopic.weighting`, `topics_input_path`.
  - Sección `preaggregated` (paths Parquet `reviews_monthly` y `players_monthly`).
  - Sección `mongodb` (uri/db/collection) para clasificar noticias desde `exploitation_zone.news_games`.
  - Sección `llm` (modelo/servidor/temperatura/labels/`mlflow_experiment`).
- `configs/ccf_analysis.yaml`: pares CCF y Granger, players CSVs, filtro opcional por clúster.
  - Sección `preaggregated` (paths Parquet `reviews_monthly` y `players_monthly`).
  - Contrato de datos: `docs/DATA_CONTRACT.yaml` (rellénalo para adaptar lectores/validaciones).

Arranque rápido (Docker)
- `docker compose up -d --build`
- `dvc repro embeddings clustering` o `make run-all` para ejecutar todo el flujo base y eventos (incluye clasificación de noticias).
- Reporte por juego: `APPID=12345 make report` o dentro del contenedor: `python src/insights/build_game_report.py --config configs/events.yaml --appid 12345 --top_k 15`.

Makefile (servicio `steam-analytics`)
- `make up` / `make down` / `make sh`
- `make run-all`: embeddings → clustering → ccf → detect_events → topic_motives → news_classifier → enrich_events.
- `make report`: requiere `APPID`.
- `make clustering` / `make clustering-check`: ejecuta solo clustering y valida artefactos (`data/processed/clusters.parquet`, `outputs/clustering/cluster_stats.csv`, `outputs/clustering/borderline_games.csv`, `models/cluster_medoids.json`).
- `make topics` / `make topics-check`: ejecuta BERTopic y verifica `outputs/events/topics.parquet`; si está instalado `gensim`, también comprueba presencia de `coherence_cv` por tópico.
- `make news` / `make news-check`: ejecuta el clasificador de noticias (batch) y verifica `outputs/events/news_classified.parquet` y presencia de etiquetas.
- `make ccf` / `make ccf-check`: ejecuta análisis CCF/Granger y verifica `outputs/ccf_analysis/summary.parquet` y columnas corregidas por FDR (`granger_*_p_fdr`, `granger_*_sig_fdr`).
- `make validate-all`: encadena `clustering-check`, `topics-check` y `ccf-check`.

Documentación ampliada
- Índice: `docs/index.md`
- Métodos: `docs/methods/clustering.md`, `docs/methods/topics.md`, `docs/methods/ccf.md`
- Guía de uso: `docs/usage.md`
- Modo Big Data: ver `docs/methods/pipeline_options.md` y diagramas en `docs/architecture.mmd` / `docs/diagrams/*`
 - Backends distribuidos (no-MVP):
   - Eventos Spark: `src/pipelines/event_detection/events_spark.py` (DVC: `events_spark`)
   - Tópicos prep Spark + Ray: `src/insights/topics_prep_spark.py` → `src/insights/topics_from_prep_ray.py` (DVC: `topics_spark_prep`, `topics_ray`)
   - CCF Spark (Pandas UDF): `src/pipelines/ccf_analysis/ccf_spark.py` (DVC: `ccf_spark`)

Notas de datos
- Embeddings: se leen tanto de `data/processed/embeddings/` como de `data/processed/embeddings.parquet`.
- Eventos: requieren `outputs/events/events.parquet` (producido por `detect_events.py`).
- Enriquecimiento: consume `outputs/events/news_classified.parquet` (conteos por categoría/mes) y `outputs/events/topics_labeled.parquet` (etiquetas LLM por evento/mes) si existen.
- BERTopic: usa reseñas de Mongo; ponderación por votos se configura en `configs/events.yaml`.
- Reportes: si existe `models/embeddings.faiss` y `models/emb_ids.json`, usa ANN para vecinos; si no, producto escalar en memoria.
- Escala/MVP: para pruebas locales (≈10k juegos, 2M reseñas), usa `FlatIP` + `multiprocessing`; para producción, activa `preaggregated` y considera ANN.

Notas sobre nuevas validaciones
- Clustering: se generan métricas y artefactos en `outputs/clustering/*`; los checks del Makefile validan su existencia.
- Tópicos: la coherencia C_v requiere `gensim`; si no está disponible, el cálculo se omite y el check avisará con WARN.
- CCF/Granger: los p-values se corrigen por FDR (Benjamini–Hochberg) y se exponen columnas `*_p_fdr` y flags `*_sig_fdr`.
- Paralelización: si Ray no está disponible, eventos/enriquecimiento hacen fallback a multiprocessing/secuencial.

Despliegue con Docker
- Prerrequisitos: Docker Desktop (o Docker Engine) y Docker Compose v2.
- Preparación:
  1) Copia `.env.example` a `.env` y ajusta `MONGO_URI`, `MLFLOW_TRACKING_URI`, claves de APIs si las usas.
  2) Si usas GCS, coloca tus credenciales en `secrets/gcp.json` (ya está mapeado como solo-lectura).
  3) Asegura que `data/`, `outputs/`, `configs/` existen (el compose los monta en el contenedor `app`).
- Levantar servicios:
  - `docker compose up -d --build`
  - Servicios expuestos:
    - MongoDB: `localhost:27017` (volumen persistente `mongo_data`)
    - MLflow UI: `http://localhost:5000` (volumen persistente `mlruns`)
    - App (contenedor `steam-analytics`): monta `./data`, `./outputs`, `./configs`, `./secrets`
- Verificar/operar:
  - Estado: `docker compose ps`
  - Logs: `docker compose logs -f app` (o `mlflow`/`mongo`)
  - Shell dentro del contenedor: `docker exec -it steam-analytics bash`
  - Ejecutar pipeline dentro del contenedor:
    - Completo: `make run-all`
    - DVC por etapas: `dvc repro embeddings clustering` (y luego `events topics enrich ccf report` según necesites)
  - MLflow: `xdg-open http://localhost:5000` (o abre en navegador)
- Parar y limpiar:
  - Parar: `docker compose down`
  - Reconstruir limpio: `docker compose build --no-cache && docker compose up -d`
  - Borrar volúmenes (perderás datos): `docker compose down -v`
- Windows: usa `docker compose` (no `docker-compose`) en PowerShell/CMD modernos.

Ejemplo de salida (JSON)

Ejemplo sintético del JSON final por juego (`outputs/reports/{appid}.json`). Campos reales pueden variar según datos disponibles.

```json
{
  "appid": "12345",
  "generated_at": "2025-09-02T12:34:56Z",
  "metadata": {
    "name": "Sample Game",
    "genres": ["Action", "RPG"],
    "categories": ["Single-player"],
    "release_date": "2023-11-10",
    "price": 29.99
  },
  "cluster": { "cluster_id": 42 },
  "neighbors": [
    { "appid": "67890", "name": "Neighbor A", "cluster_id": 42, "similarity": 0.8123 },
    { "appid": "13579", "name": "Neighbor B", "cluster_id": 42, "similarity": 0.7954 }
  ],
  "ccf_granger": [
    {
      "pair_name": "players_vs_positive_reviews",
      "best_lag": 1,
      "best_ccf": 0.56,
      "best_pval": 0.012,
      "best_significant_fdr": true,
      "lead_or_lag": "predictor_leads",
      "granger_xy_pmin": 0.018,
      "granger_yx_pmin": 0.21,
      "granger_xy_sig": true,
      "granger_yx_sig": false
    }
  ],
  "events": [
    { "year_month": "2024-06-01", "variable": "players", "direction": "peak", "zscore": 2.7, "value": 12345, "growth_rate": 0.35 }
  ],
  "topics": [
    { "event_year_month": "2024-06-01", "topics": [ { "Topic": 12, "Count": 134, "Name": "performance, stutter, patch" } ] }
  ],
  "explanations": [
    { "appid": "12345", "year_month": "2024-06-01", "twitch_spike": true, "yt_mentions": 28, "steam_update": "2024-06-03" }
  ],
  "rules_analysis": {
    "regla_precio": "Precio alineado al estándar del segmento"
  },
  "provenance": {
    "metadata_parquet": "data/processed/game_metadata.parquet",
    "embeddings_parquet": "data/processed/embeddings.parquet",
    "clusters_parquet": "data/processed/clusters.parquet",
    "ccf_summary_parquet": "outputs/ccf_analysis/summary.parquet",
    "events_parquet": "outputs/events/events.parquet",
    "topics_parquet": "outputs/events/topics.parquet",
    "explanations_parquet": "outputs/events/explanations.parquet",
    "rules_parquet": "data/with_rules/with_rules.parquet"
  }
}
```

Ejemplo sintético del JSON de cliente (`outputs/reports/client_{id}.json`), pensado para pasar a IA/redacción. Incluye competidores enriquecidos.

```json
{
  "appid": "client-001",
  "metadata": {
    "name": "Cliente Game",
    "description": "Juego del cliente con combate táctico y exploración.",
    "price": 19.99,
    "tags": ["strategy", "tactical", "indie"],
    "release_date": "2025-09-01"
  },
  "cluster": { "cluster_id": 42 },
  "neighbors": [
    { "appid": "67890", "name": "Neighbor A", "cluster_id": 42, "similarity": 0.81 }
  ],
  "competitors": [
    {
      "appid": "67890",
      "name": "Neighbor A",
      "cluster_id": 42,
      "similarity": 0.81,
      "ccf_granger": [ { "pair_name": "players_vs_positive_reviews", "best_lag": 1, "best_ccf": 0.52 } ],
      "events": [ { "year_month": "2024-05-01", "variable": "players", "direction": "peak", "zscore": 2.3 } ],
      "topics": [ { "event_year_month": "2024-05-01", "topics": [ { "Topic": 7, "Name": "content, expansion" } ] } ],
      "explanations": [ { "year_month": "2024-05-01", "twitch_spike": true } ]
    }
  ],
  "rules_analysis": { "regla_precio": "Juego económico frente al segmento" },
  "provenance": {
    "embeddings_parquet": "data/processed/embeddings.parquet",
    "clusters_parquet": "data/processed/clusters.parquet",
    "metadata_parquet": "data/processed/game_metadata.parquet",
    "ccf_summary_parquet": "outputs/ccf_analysis/summary.parquet",
    "events_parquet": "outputs/events/events.parquet",
    "topics_parquet": "outputs/events/topics.parquet",
    "explanations_parquet": "outputs/events/explanations.parquet",
    "medoids_json": "models/cluster_medoids.json"
  }
}
```

Notas sobre validación de esquemas
- Reporte por appid: validación mínima y opcional contra `schemas/app_report.schema.json` si `jsonschema` está instalado.
- Reporte de cliente: validación mínima y opcional contra `schemas/client_report.schema.json` si `jsonschema` está instalado.


How-To (paso a paso)
- Docker
  1) Copia `.env.example` a `.env` y ajusta credenciales/URIs.
  2) `docker compose up -d --build`
  3) `dvc repro embeddings clustering` (o `make run-all` para ejecutar todo)
  4) Genera un reporte: `APPID=12345 make report`

- Local
  1) `python -m venv .venv && source .venv/bin/activate` (o `.\.venv\Scripts\activate` en Windows)
  2) `pip install -r requirements.txt`
  3) Exporta variables del `.env` (o usa un gestor tipo `dotenv`)
  4) Ejecuta por etapas:
     - `python src/pipelines/generate_embeddings.py --config configs/embeddings.yaml`
     - `python src/pipelines/run_clustering.py --config configs/clustering.yaml`
     - `python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_analysis.yaml`
     - `python src/pipelines/event_detection/detect_events.py --config configs/events.yaml`
     - `python src/insights/topic_motives.py --config configs/events.yaml`
     - `python src/insights/news_classifier.py --config configs/events.yaml`
     - `python src/pipelines/event_detection/enrich_events.py --config configs/events.yaml`
     - `python src/insights/build_game_report.py --config configs/events.yaml --appid 12345 --top_k 15`

Modo Big Data (opcional)
- Preagregados (Spark con fallback):
  - `make preagg` para generar `data/warehouse/reviews_monthly.parquet` y `data/warehouse/players_monthly.parquet`.
  - Se escriben particionados por `year_month` y con orden interno por `appid` para mejorar lecturas por ventana temporal y subconjuntos de juegos.
- Clustering con Spark KMeans:
  - Usar `configs/clustering_spark.yaml` (fuerza Spark desde 10k juegos) y ejecutar:
    - `docker exec -it steam-analytics bash -lc 'python src/pipelines/run_clustering.py --config configs/clustering_spark.yaml'`
- ANN escalable:
  - En `configs/embeddings.yaml: faiss_index.index` puedes establecer `IVF4096,Flat` (siempre que tengas suficiente memoria para entrenar el índice). El default sigue siendo `FlatIP`.
- Eventos/CCF consumen por defecto los preagregados si existen (ver `configs/events.yaml` y `configs/ccf_analysis.yaml`).
- Fallback de clustering: aunque elijas `graph_leiden`, si `n_samples ≥ kmeans.threshold_spark` se ejecutará Spark KMeans automáticamente.
 - Backends alternativos:
   - Eventos Spark: `make events-spark` (o `dvc repro events_spark`).
   - Tópicos: `make topics-prep` y luego `make topics-ray`.
   - CCF Spark: `make ccf-spark` (o `dvc repro ccf_spark`).

Estructura del Repo
```
.
├─ configs/
│  ├─ embeddings.yaml
│  ├─ clustering.yaml
│  ├─ events.yaml
│  ├─ ccf_analysis.yaml
│  └─ params.yaml
├─ src/
│  ├─ pipelines/
│  │  ├─ generate_embeddings.py
│  │  ├─ run_clustering.py
│  │  ├─ event_detection/{detect_events.py,enrich_events.py}
│  │  └─ ccf_analysis/analyze_competitors_ccf.py
│  ├─ insights/{build_game_report.py,topic_motives.py,make_client_report.py,compose_editor_payload.py}
│  ├─ ingestion/{twitch.py,youtube.py,dlcs.py}
│  └─ utils/{io.py,mlflow_utils.py,faiss_utils.py,spark_utils.py,config_utils.py,mongo_utils.py}
├─ data/
│  ├─ processed/ (embeddings/, clusters.parquet, ...)
│  └─ external/ (players/, twitch/, youtube/, changelogs/)
├─ outputs/
│  ├─ events/ (events.parquet, topics.parquet, topics_labeled.parquet, news_classified.parquet, explanations.parquet)
│  └─ ccf_analysis/ (summary.parquet)
├─ models/ (cluster_medoids.json)
├─ docs/ (pipeline.mmd, DATA_CONTRACT.yaml)
├─ dvc.yaml
├─ Dockerfile / docker-compose.yml
├─ Makefile
└─ README.md
```

Variables de Entorno (ver `.env.example`)
- `MONGO_URI`, `MONGO_DB_GAMES`, `MONGO_COLL_GAMES` (juegos/metadata)
- `MONGO_DB_REVIEWS`, `MONGO_COLL_REVIEWS` (reseñas)
- `MONGO_DB_EXPLOITATION`, `MONGO_COLL_NEWS` (noticias: explotación `exploitation_zone.news_games`)
- `MLFLOW_TRACKING_URI`
- `GOOGLE_APPLICATION_CREDENTIALS` (si usas GCS)
- `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET`, `YT_API_KEY` (si tiras de APIs)

Requisitos de Datos
- Players: CSV por appid en `data/external/players/{appid}.csv` con columnas `date,players`.
- Mongo (reseñas): colección con `appid`, `timestamp_created` (epoch sec), `voted_up`, `review`, `language`, y opcionales `votes_up|votes_helpful|helpful` para ponderación BERTopic.
- Mongo (juegos): `name`, descripciones, `genres[]`, `categories[]`, `dlc[]` (con `appid,name,release_date`).

Pipelines DVC
- Ver definición completa en `dvc.yaml` y diagrama en `docs/pipeline.mmd`:
  - embeddings → clustering → {events → topics → news → enrich} + {ccf} → report/editor → client_report

Troubleshooting
- Ray no instalado: cambia `parallel_mode` a `multiprocessing` en configs o `pip install ray`.
- FAISS GPU: ajusta `faiss.use_gpu: false` en `clustering.yaml` si no hay CUDA.
 - Dependencias nuevas (p. ej., `gensim`): si corres en Docker, reconstruye la imagen para incluirlas: `docker compose build --no-cache && docker compose up -d`. En local, ejecuta `pip install -r requirements.txt` tras hacer pull.
 - Make en Windows: usa PowerShell/CMD con `docker compose` (no `docker-compose`) o Git Bash/WSL si tienes problemas con los targets del Makefile.
 - MLflow no responde: verifica servicios con `docker compose ps` y logs con `docker compose logs -f mlflow`.

Scripts y Funciones
- `src/pipelines/generate_embeddings.py`: genera embeddings de juegos desde Mongo y metadata; guarda en `data/processed/embeddings/` y `embeddings.parquet`; opcional índice FAISS persistente (`models/embeddings.faiss`).
- `src/pipelines/run_clustering.py`: agrupa juegos con KMeans (local/Spark) o Leiden; calcula soft-membership y borderline; guarda `data/processed/clusters.parquet`, `models/cluster_medoids.json`, y métricas/artefactos en `outputs/clustering/*`; persistencia en Mongo y logging en MLflow.
- `src/pipelines/event_detection/detect_events.py`: detecta picos/caídas (z-score) en series y produce `outputs/events/events.parquet`.
- `src/insights/topic_motives.py`: BERTopic por evento; pondera reseñas por votos; calcula coherencia C_v si `gensim` está disponible; salida `outputs/events/topics.parquet` y métricas en MLflow.
- `src/insights/news_classifier.py`: clasifica noticias y tópicos con un LLM (configurable); salidas `outputs/events/news_classified.parquet` y `topics_labeled.parquet`.
- `src/pipelines/event_detection/enrich_events.py`: enriquece eventos con señales externas (Twitch/YouTube/DLC) y genera explicaciones (`outputs/events/explanations.parquet`).
- `src/pipelines/ccf_analysis/analyze_competitors_ccf.py`: calcula CCF y tests de Granger por juego/pares; aplica corrección FDR (Benjamini–Hochberg); salida `outputs/ccf_analysis/summary.{parquet,csv}` y métricas MLflow.
- `src/insights/build_game_report.py`: compone el reporte final por `appid` (`outputs/reports/{appid}.json`).
- `src/insights/compose_editor_payload.py`: transforma el reporte en payload para editor (`{appid}_editor.json`).
- `src/insights/make_client_report.py`: genera reporte por cliente a partir de archivo de entrada.
- `src/ingestion/{twitch,youtube,dlcs}.py`: conectores para señales y contenidos externos.
- `src/utils/{io,mlflow_utils,faiss_utils,spark_utils,config_utils,mongo_utils}.py`: utilidades de IO (local/GCS), MLflow, FAISS, Spark, expansión de env en configs y Mongo.
- Spark: si no lo usas, los scripts caen a local; evita stages Spark en reglas si no tienes Java.
