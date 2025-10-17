# Flujo Cliente + Mongo (PoC → JSON final)

Objetivo: construir el JSON final del cliente en MongoDB, integrando vecinos (PoC), reglas de decisión (si existen) y analítica offline (tópicos por juego).

Pasos:

- 0) Offline analytics (tópicos por juego) → guardar en Mongo
  - Generar tópicos: `dvc repro topics` o `make topics`
  - Persistir en Mongo:
    - `python Data_analytics/scripts/offline_topics_to_mongo.py \
         --parquet outputs/events/topics.parquet \
         --mongo-uri mongodb://localhost:27017 \
         --mongo-db analytics \
         --mongo-coll analytics_topics \
         --aggregate-by-app`

- 1–4) Orquestar cliente (input → vecinos → reglas → anexar tópicos vecinos)
  - Preparar JSON de cliente (ejemplo): `Data_analytics/configs/clients/poc-stellar.json`
  - Ejecutar orquestador:
    - `python Data_analytics/scripts/build_client_final_json.py \
         --client-id poc-stellar \
         --client-file Data_analytics/configs/clients/poc-stellar.json \
         --mongo-uri mongodb://localhost:27017 \
         --mongo-db exploitation_zone \
         --mongo-coll client_profiles`

Estructura en Mongo (`exploitation_zone.client_profiles`):
- `_id`: id de cliente (p. ej., `poc-stellar`)
- `client_input`: datos ingresados por usuario
- `neighbors` + `neighbors_appids`: competidores seleccionados por PoC
- `decision_rules_neighbors`: reglas para vecinos (si existe `data/with_rules/with_rules.parquet`)
- `analytics_neighbors.topics`: tópicos de vecinos traídos desde `analytics.analytics_topics`
- `status`: etapa/note y `updated_at`

Notas:
- El orquestador usa embeddings/clusters locales (`data/processed/*`) y `models/cluster_medoids.json` si existe.
- Si no hay parquet de reglas, el documento se completa sin la sección `decision_rules_neighbors`.
- Las escrituras en Mongo son upserts idempotentes; se puede re-ejecutar para actualizar.

Alternativa centrada en Backend (guardar docs por competidor y unir en backend)
- Persistir reportes por juego (uno por appid) a Mongo eliminando `provenance`:
  - `python Data_analytics/scripts/persist_reports_to_mongo.py \
       --reports-dir outputs/reports \
       --mongo-uri mongodb://localhost:27017 \
       --mongo-db analytics \
       --mongo-coll app_reports \
       --drop-fields provenance`
- Solo competidores del cliente (usando el archivo generado por el PoC):
  - `python Data_analytics/scripts/persist_reports_to_mongo.py \
       --reports-dir outputs/reports \
       --appids-file outputs/clients/client_poc-stellar_appids.txt \
       --mongo-uri mongodb://localhost:27017 \
       --mongo-db analytics \
       --mongo-coll app_reports \
       --drop-fields provenance`

Con este enfoque, el backend consulta `analytics.app_reports` por appids vecinos y compone el JSON final del cliente sin que `data_analytics` una nada.
