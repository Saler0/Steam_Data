# Documentación — Steam Game Analytics

Bienvenido a la documentación del pipeline. Aquí encontrarás detalles de métodos, configuración, artefactos y guías de ejecución y validación.

- Visión general del flujo: consulta el diagrama en `docs/pipeline.svg` o `docs/pipeline.mmd`.
- Arquitectura (fuentes → orquestación → pipelines → validación/MLflow → artefactos → consumidores): `docs/architecture.mmd`
- Opciones de Clustering (decisiones y post-análisis): `docs/diagrams/clustering_options.mmd`
- Opciones de Eventos/Tópicos: `docs/diagrams/events_topics_options.mmd`
- Opciones de CCF/Granger: `docs/diagrams/ccf_granger_options.mmd`
- Orquestación (DVC + MLflow y ejemplos DAG): `docs/orchestration.md`
 - Topics Spark+Ray pipeline: `docs/diagrams/topics_spark_ray.mmd`
 - CCF Spark backend: `docs/diagrams/ccf_spark_backend.mmd`
- Métodos:
  - Clustering: `docs/methods/clustering.md`
  - Embeddings: `docs/methods/embeddings.md`
  - Tópicos (BERTopic): `docs/methods/topics.md`
  - CCF/Granger: `docs/methods/ccf.md`
- Opciones del pipeline: `docs/methods/pipeline_options.md`
- Guías: `docs/usage.md`

Sigue las guías para añadir evidencias gráficas (plots) en `docs/img/` provenientes de salidas en `outputs/`.
