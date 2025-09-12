# Orquestación con DVC y MLflow (y ejemplos DAG)

Este proyecto usa DVC para definir etapas del pipeline y MLflow para tracking. A continuación se muestra cómo integrarlo en orquestadores típicos (opcional) sin sustituir DVC/MLflow.

- Airflow: `airflow/dags/steam_analytics_dag.py` encadena `dvc repro` de las etapas principales (preagg → embeddings → clustering → events → topics → news → enrich → ccf → report).
- Prefect: `prefect/flow_steam_analytics.py` define un `flow` que invoca `dvc repro` con dependencias explícitas.

Notas:
- Estos DAG/flows son ejemplos. DVC sigue siendo la fuente de verdad del DAG de datos.
- MLflow se mantiene para registrar runs/artefactos; no depende del orquestador.

## Comandos DVC útiles
- Reproducir todo por lotes: `dvc repro preagg_reviews preagg_players embeddings clustering events topics news_classifier enrich ccf report editor_view`
- Reproducir una rama: `dvc repro topics` (requiere `events` previos), `dvc repro ccf`.

## Integración con Docker
- Ejecuta los scripts dentro del contenedor app (`steam-analytics`) para asegurar entornos (dependencias, paths montados).

```bash
docker exec -it steam-analytics bash -lc 'python src/pipelines/generate_embeddings.py --config configs/embeddings.yaml'
```

