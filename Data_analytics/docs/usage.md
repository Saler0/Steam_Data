# Guía de Uso

Ejecución con Docker
- Levantar servicios: `docker compose up -d --build`
- Flujo base + eventos: `make run-all`
- MLflow UI: `make mlflow`

Modo Big Data (opcional)
- Generar preagregados mensuales (Spark con fallback): `make preagg`
- Ejecutar clustering con Spark (si supera el umbral o quieres forzarlo):
  - Usando fallback automático: ajusta `kmeans.threshold_spark` en `configs/clustering.yaml`.
  - O con config alternativa: `python src/pipelines/run_clustering.py --config configs/clustering_spark.yaml`
- Ajustar embeddings para rendimiento: `configs/embeddings.yaml: embedding_batch_size` (p. ej., 256/512) y `parallelization.n_shards`.

Ejecución por etapas (Makefile)
- Embeddings + Clustering mínimo: `dvc repro embeddings clustering` o `make clustering`
- Validaciones rápidas:
  - `make clustering-check`
  - `make topics-check`
  - `make ccf-check`
  - `make validate-all`

Paralelización
- `events.yaml`: `parallelization.mode`: `ray` o `multiprocessing`
- `ccf_analysis.yaml`: `parallel_mode`: `multiprocessing` (por defecto), `ray` o `sequential`

Datos y Contrato
- Revisa `docs/DATA_CONTRACT.yaml` y completa campos para adaptar lectores/validaciones.

Clustering: cadencia y asignación diaria
- Refit mensual del clustering (o trimestral) para estabilidad; las salidas incluyen `cluster_version` (YYYYMM) y `as_of_date`.
- Asignación diaria de nuevos juegos con medoids: `python -m src.pipelines.cluster_assignment.assign_new_games --embeddings data/processed/embeddings_new.parquet --medoids models/cluster_medoids.json --out data/processed/clusters_assigned.parquet --cluster_version 202501`
 - También disponible como etapa DVC: `dvc repro cluster_assign`

Evidencias gráficas
- Genera gráficos a partir de `outputs/` y colócalos en `docs/img/`.
- Enlázalos desde las páginas de método (`docs/methods/*.md`).
Renderizar diagramas (opcional)
- Requiere Mermaid CLI: `npm i -g @mermaid-js/mermaid-cli`
- Render: `make docs-svg` (o `mmdc -i docs/architecture.mmd -o docs/architecture.svg`, etc.)
