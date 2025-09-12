# Embeddings y Features

Genera embeddings de juegos a partir de metadatos textuales y etiquetas (géneros/categorías), con paralelización configurable.

- Script: `src/pipelines/generate_embeddings.py`
- Config: `configs/embeddings.yaml`
- Entradas: Mongo (juegos) con campos de texto y tags.
- Salidas:
  - Shards: `data/processed/embeddings/part-*.parquet`
  - Consolidado: `data/processed/embeddings.parquet`
  - Índice FAISS opcional: `models/embeddings.faiss` + `models/emb_ids.json`

Construcción del documento
- `document_fields.text_fields`: concatena `name`, `short_description`, `detailed_description` (configurable) con separador de salto de línea.
- `document_fields.tag_fields`: agrega `genres` y `categories` normalizados (`espacio`→`_`).

Modelo y normalización
- `embedding_model`: por defecto `sentence-transformers/all-MiniLM-L6-v2`.
- `normalize_embeddings`: si es `true`, normaliza los vectores (útil para similitud por producto interno).
 - `embedding_batch_size`: controla el tamaño de lote en `model.encode` (ajústalo según memoria GPU/CPU; típicamente 64–512).

Paralelización
- `parallelization.method`: `multiprocessing` o `ray`.
- `n_shards`: número de shards/procesos/workers.

Índice FAISS (opcional)
- `faiss_index.enabled`: si es true, construye y guarda un índice (`FlatIP` por defecto).
- `use_gpu`: intenta mover a GPU si es posible; si falla, cae a CPU.
- Persistencia: guarda el índice y el orden de appids (`emb_ids.json`) para consultas posteriores.

Buenas prácticas
- Verifica que `appid` esté presente y sea único por fila.
- Para corpus muy grandes, evalúa modelos más compactos o preagregaciones.

Performance tuning
- Aumenta `embedding_batch_size` si tienes memoria suficiente para acelerar el throughput.
- Incrementa `n_shards` para paralelizar mejor en CPU o usa Ray para distribuir entre máquinas.
