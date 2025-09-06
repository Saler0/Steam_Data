.PHONY: up down sh mlflow run-all assign report clustering clustering-check topics topics-check ccf ccf-check validate-all docs-open preagg preagg-reviews preagg-players events-spark topics-prep topics-ray ccf-spark

up:
	docker compose up -d --build

down:
	docker compose down

sh:
	docker exec -it steam-analytics /bin/bash

mlflow:
	xdg-open http://localhost:5000 || open http://localhost:5000 || true

run-all:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/generate_embeddings.py --config configs/embeddings.yaml && python src/pipelines/run_clustering.py --config configs/clustering.yaml && python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_analysis.yaml && python src/pipelines/event_detection/detect_events.py --config configs/events.yaml && python src/insights/topic_motives.py --config configs/events.yaml && python src/insights/news_classifier.py --config configs/events.yaml && python src/pipelines/event_detection/enrich_events.py --config configs/events.yaml'

assign:
	@echo "Target 'assign' desactualizado: script no disponible."

report:
	docker exec -it steam-analytics bash -lc 'python src/insights/build_game_report.py --config configs/events.yaml --appid $$APPID --top_k 15'

# Ejecuta solo clustering con la config actual
clustering:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/run_clustering.py --config configs/clustering.yaml'

# Ejecuta clustering y valida artefactos/metricas esperadas
clustering-check:
	docker exec -it steam-analytics bash -lc \
		"python src/pipelines/run_clustering.py --config configs/clustering.yaml && \
		test -f data/processed/clusters.parquet && \
		test -f outputs/clustering/cluster_stats.csv && \
		test -f outputs/clustering/borderline_games.csv && \
		test -f models/cluster_medoids.json && \
		echo 'OK: clustering outputs present' || (echo 'ERROR: faltan artefactos de clustering' && exit 1)"

# Ejecuta modelado de tópicos (BERTopic)
topics:
	docker exec -it steam-analytics bash -lc 'python src/insights/topic_motives.py --config configs/events.yaml'

# Ejecuta tópicos y valida archivo y, si es posible, coherencia C_v
topics-check:
	docker exec -it steam-analytics bash -lc \
		"python src/insights/topic_motives.py --config configs/events.yaml && \
		test -f outputs/events/topics.parquet || (echo 'ERROR: no existe outputs/events/topics.parquet' && exit 1) && \
		python - <<'PY'
import pandas as pd
from pathlib import Path
p=Path('outputs/events/topics.parquet')
df=pd.read_parquet(p) if p.exists() else None
if df is None or df.empty:
    print('WARN: topics.parquet está vacío')
else:
    # Buscar alguna coherencia C_v presente en la lista de tópicos
    found=False
    for rec in df.get('topics', []):
        if not isinstance(rec, list):
            continue
        for t in rec:
            if isinstance(t, dict) and 'coherence_cv' in t and t['coherence_cv'] is not None:
                found=True; break
        if found: break
    if found:
        print('OK: coherencia C_v presente (al menos un tópico)')
    else:
        print('WARN: coherencia C_v no presente (puede faltar gensim)')
PY
		 && echo 'OK: topics outputs present'"

# Ejecuta análisis CCF/Granger
ccf:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_analysis.yaml'

# Ejecuta CCF y valida summary y columnas FDR
ccf-check:
	docker exec -it steam-analytics bash -lc \
		"python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_analysis.yaml && \
		test -f outputs/ccf_analysis/summary.parquet || (echo 'ERROR: no existe outputs/ccf_analysis/summary.parquet' && exit 1) && \
		python - <<'PY'
import pandas as pd
from pathlib import Path
p=Path('outputs/ccf_analysis/summary.parquet')
df=pd.read_parquet(p) if p.exists() else None
if df is None or df.empty:
    print('WARN: summary.parquet está vacío')
else:
    required=['granger_xy_p_fdr','granger_xy_sig_fdr','granger_yx_p_fdr','granger_yx_sig_fdr']
    missing=[c for c in required if c not in df.columns]
    if missing:
        print('ERROR: faltan columnas FDR:', ','.join(missing)); exit(1)
    print('OK: columnas FDR presentes')
PY
		 && echo 'OK: ccf outputs present'"

# Ejecuta validación completa de artefactos principales
validate-all: clustering-check topics-check ccf-check

# Abre la documentación local en tu visor por defecto
docs-open:
	@# Intenta Linux/macOS; en Windows usa 'start' si está disponible
	@if command -v xdg-open >/dev/null 2>&1; then xdg-open docs/index.md; \
	elif command -v open >/dev/null 2>&1; then open docs/index.md; \
	else echo "Abre docs/index.md manualmente o con tu editor"; fi

# --- Docs rendering (Mermaid -> SVG) ---
.PHONY: docs-svg docs-pipeline-svg docs-arch-svg docs-clustering-svg docs-events-svg docs-ccf-svg docs-topics-spark-svg docs-ccf-backend-svg

docs-svg: docs-pipeline-svg docs-arch-svg docs-clustering-svg docs-events-svg docs-ccf-svg docs-topics-spark-svg docs-ccf-backend-svg ## Render all Mermaid diagrams to SVG (requires mermaid-cli `mmdc`)

docs-pipeline-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mmdc -i docs/pipeline.mmd -o docs/pipeline.svg; \
		echo "OK: docs/pipeline.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

docs-arch-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mmdc -i docs/architecture.mmd -o docs/architecture.svg; \
		echo "OK: docs/architecture.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

docs-clustering-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mkdir -p docs/diagrams; \
		mmdc -i docs/diagrams/clustering_options.mmd -o docs/diagrams/clustering_options.svg; \
		echo "OK: docs/diagrams/clustering_options.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

docs-events-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mkdir -p docs/diagrams; \
		mmdc -i docs/diagrams/events_topics_options.mmd -o docs/diagrams/events_topics_options.svg; \
		echo "OK: docs/diagrams/events_topics_options.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

docs-ccf-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mkdir -p docs/diagrams; \
		mmdc -i docs/diagrams/ccf_granger_options.mmd -o docs/diagrams/ccf_granger_options.svg; \
		echo "OK: docs/diagrams/ccf_granger_options.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

docs-topics-spark-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mkdir -p docs/diagrams; \
		mmdc -i docs/diagrams/topics_spark_ray.mmd -o docs/diagrams/topics_spark_ray.svg; \
		echo "OK: docs/diagrams/topics_spark_ray.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

docs-ccf-backend-svg:
	@if command -v mmdc >/dev/null 2>&1; then \
		mkdir -p docs/diagrams; \
		mmdc -i docs/diagrams/ccf_spark_backend.mmd -o docs/diagrams/ccf_spark_backend.svg; \
		echo "OK: docs/diagrams/ccf_spark_backend.svg actualizado"; \
	else \
		echo "WARN: 'mmdc' no encontrado. Instala 'npm i -g @mermaid-js/mermaid-cli' para renderizar."; \
	fi

# --- Preaggregations (Spark with fallback) ---
preagg: preagg-reviews preagg-players

preagg-reviews:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/preaggregations/reviews_monthly.py --config configs/ccf_analysis.yaml'

preagg-players:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/preaggregations/players_monthly.py --players_dir data/external/players --out data/warehouse/players_monthly.parquet'

events-spark:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/event_detection/events_spark.py --config configs/events.yaml'

topics-prep:
	docker exec -it steam-analytics bash -lc 'python src/insights/topics_prep_spark.py --config configs/events.yaml'

topics-ray:
	docker exec -it steam-analytics bash -lc 'python src/insights/topics_from_prep_ray.py --config configs/events.yaml'

ccf-spark:
	docker exec -it steam-analytics bash -lc 'python src/pipelines/ccf_analysis/ccf_spark.py --config configs/ccf_analysis.yaml'

# --- News classifier helpers ---
.PHONY: news news-check

news:
	docker exec -it steam-analytics bash -lc 'python src/insights/news_classifier.py --config configs/events.yaml'

news-check:
	docker exec -it steam-analytics bash -lc \
		"python - <<'PY'\nfrom pathlib import Path\nimport pandas as pd\np_news = Path('outputs/events/news_classified.parquet')\np_topics = Path('outputs/events/topics_labeled.parquet')\nstatus=[]\nif p_news.exists():\n    df=pd.read_parquet(p_news)\n    print('OK: news_classified.parquet existe con', len(df), 'registros')\n    if 'label' in df.columns:\n        print('OK: columna label presente (valores únicos:', df['label'].dropna().unique()[:10], ')')\n    else:\n        print('WARN: columna label no presente')\nelse:\n    print('ERROR: outputs/events/news_classified.parquet no existe')\nif p_topics.exists():\n    dt=pd.read_parquet(p_topics)\n    print('OK: topics_labeled.parquet existe con', len(dt), 'filas')\nelse:\n    print('WARN: outputs/events/topics_labeled.parquet no existe (puede omitirse si no hay topics)')\nPY"
