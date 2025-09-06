# Tópicos de Reseñas (BERTopic)

Modela tópicos en ventanas alrededor de eventos temporalmente relevantes.

- Script: `src/insights/topic_motives.py`
- Config: `configs/events.yaml` (sección `bertopic` y `parallelization`)
- Entradas: `outputs/events/events.parquet` y reseñas en Mongo.
- Salidas: `outputs/events/topics.parquet`

Características
- Ventana configurable alrededor del mes del evento (`window_months`).
- Ponderación opcional por votos de utilidad (replicación de textos por peso), con tope.
- Paralelización vía multiprocessing o Ray.
- Coherencia C_v opcional (requiere `gensim`): se calcula para los tópicos principales por evento y se registra en MLflow el promedio.

Detalles de coherencia C_v
- C_v mide la consistencia semántica de las palabras top-k de cada tópico con respecto al corpus de documentos tokenizado.
- Implementación: se obtiene la lista de términos por tópico desde BERTopic, se tokeniza el corpus con el analyzer del vectorizador, y se usa `gensim.CoherenceModel(coherence='c_v')`.
- Interpretación: valores más altos ~ mayor coherencia. En pruebas, C_v suele estar en [0.2, 0.7] según dominio y tamaño de muestra.

Parámetros relevantes (`configs/events.yaml`)
- `bertopic.embedding_model`, `bertopic.min_topic_size`, `bertopic.min_df`
- `bertopic.max_docs_per_event` (muestreo por evento)
- `bertopic.weighting.enabled|cap|method|min_len`
- `parallelization.mode`: `ray` | `multiprocessing`

Ejecución
- Makefile: `make topics` o `make topics-check`
- DVC: etapa `topics`

Evidencias gráficas sugeridas
- Nube de palabras por tópico (top-k términos) por evento.
- Barras de coherencia C_v por tópico principal.
