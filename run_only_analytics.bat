@echo off
chcp 65001 >nul
setlocal EnableExtensions EnableDelayedExpansion

rem Ir a la carpeta del script (donde está docker-compose.yml)
cd /d %~dp0

rem Asegura nombre de proyecto consistente con tu compose (name: proyecto_steam)
set COMPOSE_PROJECT_NAME=proyecto_steam

echo.
echo ============================
echo Limpiando contenedores previos con nombre estático...
echo ============================
docker rm -f mongo 2>nul
docker rm -f steam_analytics 2>nul
docker rm -f data_management_pipeline 2>nul

echo.
echo ============================
echo Levantando solo Mongo + Postgres + MLflow y Analytics...
echo ============================
docker compose up -d mongo postgres mlflow analytics
if errorlevel 1 (
    echo.
    echo ERROR: No se pudieron levantar los servicios requeridos.
    pause
    exit /b 1
)

rem ===== Control de etapas DVC =====
set "DVC_TARGETS=embeddings clustering cluster_topics_profile cluster_topics_map review_segments"
set "DVC_FLAGS="
set "SKIP_EMB=0"
set "RUN_TOPICS_ONLY=0"
set "RUN_POC=0"
set "RUN_PREAGG_ONLY=0"
set "RUN_PREAGG_BEFORE=0"
set "CUSTOM_MODE=0"
set "CUSTOM_STAGES="
set "APPID_LIST="
set "RUN_REVIEWS_MONGO=0"
set "POC_ARGS="
set "RUN_CCF_ONE=0"
set "RUN_NEIGHBORS=0"
set "RUN_PLAYERS_PG=0"
set "RUN_PG_RECREATE=0"
set "PG_TABLE="
set "RUN_POC_CLIENT=0"
set "CLIENT_ID="
set "CLIENT_FILE="
set "RUN_OFFLINE_ALL=0"
set "RUN_NEWS_TRAIN=0"
set "RUN_TOPICS_SUMMARY=0"
set "TOPICS_SUMMARY_PROVIDER=heuristic"
set "RUN_SPARK_BACKEND=0"
set "FEATURIZER="
set "EMB_MODEL="
set "TRAIN_MODELS="
set "TRAIN_SCORING="
set "NS_MIN_SCORE="
set "CV_K="
set "RUN_FROM_NEWS=0"

set "APPID_BUILDING=0"
for %%A in (%*) do (
    set "ARG=%%~A"
    if /I "!ARG!"=="skip-embeddings" set "SKIP_EMB=1"
    if /I "!ARG!"=="topics-only" set "RUN_TOPICS_ONLY=1"
    if /I "!ARG!"=="single-game-poc" set "RUN_POC=1"
    if /I "!ARG!"=="reviews-mongo" set "RUN_REVIEWS_MONGO=1"
    if /I "!ARG!"=="ccf-one" set "RUN_CCF_ONE=1"
    if /I "!ARG!"=="neighbors" set "RUN_NEIGHBORS=1"
    if /I "!ARG!"=="players-pg" set "RUN_PLAYERS_PG=1"
    if /I "!ARG!"=="pg-recreate" set "RUN_PG_RECREATE=1"
    if /I "!ARG:~0,9!"=="pg-table=" set "PG_TABLE=!ARG:~9!"
    if /I "!ARG!"=="poc-client" set "RUN_POC_CLIENT=1"
    if /I "!ARG!"=="offline-all" set "RUN_OFFLINE_ALL=1"
    if /I "!ARG!"=="news-train" set "RUN_NEWS_TRAIN=1"
    if /I "!ARG!"=="svm-train" set "RUN_NEWS_TRAIN=1"
    if /I "!ARG!"=="from-news" set "RUN_FROM_NEWS=1"
    if /I "!ARG!"=="topics-summary" set "RUN_TOPICS_SUMMARY=1"
    if /I "!ARG:~0,17!"=="summary-provider=" set "TOPICS_SUMMARY_PROVIDER=!ARG:~17!"
    if /I "!ARG!"=="spark-backend" set "RUN_SPARK_BACKEND=1"
    if /I "!ARG:~0,11!"=="featurizer=" set "FEATURIZER=!ARG:~11!"
    if /I "!ARG:~0,10!"=="emb-model=" set "EMB_MODEL=!ARG:~10!"
    if /I "!ARG:~0,7!"=="models=" set "TRAIN_MODELS=!ARG:~7!"
    if /I "!ARG:~0,8!"=="scoring=" set "TRAIN_SCORING=!ARG:~8!"
    if /I "!ARG:~0,6!"=="score=" set "TRAIN_SCORING=!ARG:~6!"
    if /I "!ARG:~0,10!"=="min-score=" set "NS_MIN_SCORE=!ARG:~10!"
    if /I "!ARG:~0,3!"=="cv=" set "CV_K=!ARG:~3!"
    if /I "!ARG:~0,10!"=="client_id=" set "CLIENT_ID=!ARG:~10!"
    if /I "!ARG:~0,12!"=="client_file=" set "CLIENT_FILE=!ARG:~12!"
    if /I "!ARG!"=="preagg-only" (
        set "RUN_PREAGG_ONLY=1"
        set "CUSTOM_MODE=1"
        set "CUSTOM_STAGES=!CUSTOM_STAGES! preagg_reviews preagg_players"
    )
    if /I "!ARG!"=="with-preagg" set "RUN_PREAGG_BEFORE=1"
    if /I "!ARG:~0,6!"=="stage=" (
        set "CUSTOM_MODE=1"
        set "CUSTOM_STAGES=!CUSTOM_STAGES! !ARG:~6!"
    )
    if /I "!ARG!"=="appids" (
        set "APPID_BUILDING=1"
        set "APPID_LIST="
    ) else if "!APPID_BUILDING!"=="1" (
        rem Cortar modo appids si llega cualquier flag conocido
        set "IS_STOPPER=0"
        if /I "!ARG!"=="from-news"         set "IS_STOPPER=1"
        if /I "!ARG!"=="with-preagg"       set "IS_STOPPER=1"
        if /I "!ARG!"=="players-pg"        set "IS_STOPPER=1"
        if /I "!ARG:~0,8!"=="pg-table"     set "IS_STOPPER=1"
        if /I "!ARG:~0,10!"=="min-score="  set "IS_STOPPER=1"
        if /I "!ARG:~0,3!"=="cv="          set "IS_STOPPER=1"
        if /I "!ARG:~0,7!"=="models="      set "IS_STOPPER=1"
        if /I "!ARG:~0,11!"=="featurizer=" set "IS_STOPPER=1"
        if /I "!ARG:~0,10!"=="emb-model="  set "IS_STOPPER=1"
        if /I "!ARG!"=="exploitation_zone" set "IS_STOPPER=1"
        if "!IS_STOPPER!"=="1" (
            set "APPID_BUILDING=0"
        ) else (
            rem Solo aceptar tokens numéricos como appids
            set "NONNUM="
            for /f "delims=0123456789" %%Z in ("!ARG!") do set "NONNUM=%%Z"
            if not defined NONNUM (
                if defined APPID_LIST (
                    set "APPID_LIST=!APPID_LIST!,!ARG!"
                ) else (
                    set "APPID_LIST=!ARG!"
                )
            ) else (
                set "APPID_BUILDING=0"
            )
        )
    )
)

rem --- ramas de subcomandos ---
if "!RUN_CCF_ONE!"=="1" goto :run_ccf_one
if "!RUN_NEIGHBORS!"=="1" goto :run_neighbors
if "!RUN_POC!"=="1" goto :run_poc
if "!RUN_REVIEWS_MONGO!"=="1" goto :run_reviews_mongo
if "!RUN_POC_CLIENT!"=="1" goto :run_poc_client
if "!RUN_OFFLINE_ALL!"=="1" goto :run_offline_all
if "!RUN_NEWS_TRAIN!"=="1" goto :run_news_train

if defined APPID_LIST (
    set "APPID_LIST=!APPID_LIST:,= !"
)

if "!CUSTOM_MODE!"=="0" (
    if "!RUN_TOPICS_ONLY!"=="1" (
        set "DVC_TARGETS=cluster_topics_profile cluster_topics_map"
        set "DVC_FLAGS=--single-item"
        echo [INFO] Ejecutando solo los stages de tópicos; se reutilizarán artefactos previos de clustering.
    ) else (
        if "!SKIP_EMB!"=="1" (
            set "DVC_TARGETS=clustering cluster_topics_profile cluster_topics_map"
            set "DVC_FLAGS=--single-item"
            echo [INFO] Saltando regeneración de embeddings; se usarán artefactos existentes.
        )
    )
)

echo.
echo ============================
echo Ejecutando DVC (!DVC_TARGETS!) dentro de steam_analytics...
echo ============================

rem Marca /app como safe en Git (por si hay "dubious ownership")
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec analytics git config --global --add safe.directory /app

rem Inicializa DVC en el subdirectorio si aún no existe .dvc en la raíz
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "test -d /app/.dvc || dvc init -f --subdir"

rem Ejecuta el pipeline según la configuración seleccionada
if "!RUN_PREAGG_BEFORE!"=="1" (
    echo [INFO] Ejecutando preagg_reviews antes del pipeline principal...
    call :RunSingleStage preagg_reviews
    if errorlevel 1 goto :dvc_failed
    echo [INFO] Ejecutando preagg_players antes del pipeline principal...
    call :RunSingleStage preagg_players
    if errorlevel 1 goto :dvc_failed
)

if "!CUSTOM_MODE!"=="1" (
    for %%S in (!CUSTOM_STAGES!) do (
        set "CURRENT_STAGE=%%~S"
        if not "!CURRENT_STAGE!"=="" (
            if /I "!CURRENT_STAGE!"=="report" (
                if defined APPID_LIST (
                    for %%I in (!APPID_LIST!) do (
                        call :RunStageWithAppid report %%I
                        if errorlevel 1 goto :dvc_failed
                    )
                ) else (
                    echo [INFO] Ejecutando stage report dentro de steam_analytics...
                    call :RunSingleStage report
                    if errorlevel 1 goto :dvc_failed
                )
            ) else if /I "!CURRENT_STAGE!"=="editor_view" (
                if defined APPID_LIST (
                    for %%I in (!APPID_LIST!) do (
                        call :RunStageWithAppid editor_view %%I
                        if errorlevel 1 goto :dvc_failed
                    )
                ) else (
                    echo [INFO] Ejecutando stage editor_view dentro de steam_analytics...
                    call :RunSingleStage editor_view
                    if errorlevel 1 goto :dvc_failed
                )
            ) else (
                echo [INFO] Ejecutando stage !CURRENT_STAGE! dentro de steam_analytics...
                call :RunSingleStage !CURRENT_STAGE!
                if errorlevel 1 goto :dvc_failed
            )
        )
    )
    goto :dvc_success
)

if "!RUN_TOPICS_ONLY!"=="1" (
    call :RunSingleStage cluster_topics_profile
    if errorlevel 1 goto :dvc_failed
    call :RunSingleStage cluster_topics_map
    if errorlevel 1 goto :dvc_failed
) else (
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics dvc repro !DVC_FLAGS! !DVC_TARGETS!
    if errorlevel 1 goto :dvc_failed
)

goto :dvc_success

:run_ccf_one
echo.
echo ============================
echo Ejecutando SOLO CCF/Granger para 1 appid...
echo Uso: %~nx0 ccf-one APPID
echo ============================
if "%~2"=="" (
    echo [ERROR] Debes indicar un APPID. Ej.: %~nx0 ccf-one 1938090
    pause
    exit /b 1
)
set "APPID=%~2"

echo [INFO] Preparando artefactos temporales dentro del contenedor...
rem Crea parquet con un único appid
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python -c \"import pandas as pd, pathlib; pathlib.Path('data/processed').mkdir(parents=True, exist_ok=True); pd.DataFrame({'appid':[str(%APPID%)],'cluster_id':[0]}).to_parquet('data/processed/_tmp_single_app_clusters.parquet')\""
if errorlevel 1 (
    echo [ERROR] No se pudo crear el parquet temporal.
    pause
    exit /b 1
)

rem Genera configs/ccf_single.yaml derivada
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python -c \"import yaml,io; cfg=yaml.safe_load(open('configs/ccf_analysis.yaml','r',encoding='utf-8')); cfg['input_path']['clusters_parquet']='data/processed/_tmp_single_app_clusters.parquet'; cfg['output_dir']=f'outputs/ccf_analysis/single_%APPID%'; open('configs/ccf_single.yaml','w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))\""
if errorlevel 1 (
    echo [ERROR] No se pudo generar configs/ccf_single.yaml.
    pause
    exit /b 1
)

echo [INFO] Lanzando analyze_competitors_ccf.py solo para APPID=%APPID%...
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_single.yaml
if errorlevel 1 goto :ccf_failed

echo.
echo ============================
echo Listo. Revisa:
echo   outputs/ccf_analysis/single_%APPID%/
echo ============================
pause
endlocal
exit /b 0

:run_poc
echo.
echo ============================
echo Ejecutando PoC de asignación de juego...
echo ============================
set "POC_ARGS=%*"
set "POC_ARGS=!POC_ARGS:single-game-poc=!"
set "POC_ARGS=!POC_ARGS:skip-embeddings=!"
set "POC_ARGS=!POC_ARGS:topics-only=!"
set "POC_ARGS=!POC_ARGS:preagg-only=!"
set "POC_ARGS=!POC_ARGS:with-preagg=!"
set "POC_ARGS=!POC_ARGS:  = !"
for /f "tokens=* delims= " %%P in ("!POC_ARGS!") do set "POC_ARGS=%%P"
echo [INFO] Lanzando scripts/poc_assign_single_game.py !POC_ARGS!
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/poc_assign_single_game.py !POC_ARGS!
if errorlevel 1 goto :poc_failed
goto :poc_success

:dvc_failed
echo.
echo ERROR: Falló al ejecutar el pipeline de DVC.
echo Revisa los logs del contenedor para ver el error.
pause
endlocal
exit /b 1

:dvc_success
echo.
echo =======================================================
echo Pipeline de analytics finalizado con éxito.
echo =======================================================
echo Resultados del clustering:
echo   - data/processed/clusters.parquet
echo   - data/processed/game_metadata.parquet
echo   - models/cluster_medoids.json
echo   - outputs/clustering/cluster_stats.csv
echo   - outputs/clustering/cluster_topics.json
echo   - outputs/clustering/cluster_topics_umap.html
echo.
echo Puedes apagar los contenedores con: docker compose down
echo.
pause
endlocal
exit /b 0

:poc_failed
echo.
echo ERROR: La PoC de asignación de juego falló.
echo Revisa la salida del comando python dentro del contenedor analytics.
pause
endlocal
exit /b 1

:poc_success
echo.
echo ===============================================
echo PoC de asignación ejecutada correctamente.
echo Puedes pasar argumentos extra tras 'single-game-poc' (ej. --scenario farm).
echo ===============================================
pause
endlocal
exit /b 0

:RunStageWithAppid
set "TARGET_STAGE=%~1"
set "TARGET_APPID=%~2"
if "%~1"=="" goto :RunStageWithAppid_Error
if "%~2"=="" goto :RunStageWithAppid_Error
echo [INFO] Ejecutando %~1 (report.appid=%~2) dentro de steam_analytics...
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics dvc repro --single-item --set-param report.appid=%~2 %~1
exit /b %errorlevel%

:RunStageWithAppid_Error
echo [ERROR] Faltan argumentos para RunStageWithAppid.
exit /b 1

:RunSingleStage
rem Ejecuta un stage. Para reglas de decisión, llama directamente al script con --stage
rem Las reglas de decisión han sido movidas al backend; no se ejecutan aquí
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -c "set -a; source <(sed 's/\r$//' .env); set +a; dvc repro --single-item %1"
exit /b %errorlevel%

:run_neighbors
echo.
echo ============================
echo Ejecutando pipeline limitado a vecinos (subset de APPIDs)...
echo ============================
if not defined APPID_LIST (
    echo [ERROR] Debes indicar appids con appids=111,222,333 junto a 'neighbors'
    pause
    endlocal
    exit /b 1
)

rem Normaliza separadores
set "APPID_LIST=!APPID_LIST:,= !"

rem Preparar archivo temporal de clusters con solo estos appids
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -e APPIDS="!APPID_LIST!" -w /app/Data_analytics analytics bash -lc "python -c \"import os, pandas as pd, pathlib; pathlib.Path('data/processed').mkdir(parents=True, exist_ok=True); apps=[a for a in os.getenv('APPIDS','').split() if a]; df=pd.DataFrame({'appid':[str(a) for a in apps],'cluster_id':[0]*len(apps)}); df.to_parquet('data/processed/_tmp_neighbors_clusters.parquet')\""
if errorlevel 1 (
    echo [ERROR] No se pudo crear el parquet temporal de vecinos.
    pause
    endlocal
    exit /b 1
)

rem Generar configs subset sin DVC (llamada directa)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/prepare_subset_config.py --events configs/events.yaml --ccf configs/ccf_analysis.yaml --clusters data/processed/_tmp_neighbors_clusters.parquet --out-events configs/events_subset.yaml --out-ccf configs/ccf_subset.yaml
if errorlevel 1 goto :neighbors_failed

rem Seleccionar modelo local para noticias si existe (override) sin DVC
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/select_news_model.py --out configs/llm_override.yaml
if errorlevel 1 goto :neighbors_failed

rem Si se solicita, generar players_monthly desde Postgres (sobrescribe parquet)
if "!RUN_PLAYERS_PG!"=="1" (
    echo [INFO] Generando players_monthly desde Postgres...
    set "PG_ARGS=--postgres-host $POSTGRES_HOST --postgres-port $POSTGRES_PORT --postgres-user $POSTGRES_USER --postgres-password $POSTGRES_PASSWORD --postgres-db $POSTGRES_DB"
    if defined PG_TABLE (
        set "PG_ARGS=!PG_ARGS! --postgres-table $PG_TABLE"
    )
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); set +a; python src/pipelines/preaggregations/players_monthly.py !PG_ARGS! --out data/warehouse/players_monthly.parquet"
    if errorlevel 1 goto :neighbors_failed
)

rem Opcional: ejecutar preagregados antes si se pidió
if "!RUN_PREAGG_BEFORE!"=="1" (
    echo [INFO] Ejecutando preagg_reviews y preagg_players antes del subset...
    call :RunSingleStage preagg_reviews
    if errorlevel 1 goto :neighbors_failed
    call :RunSingleStage preagg_players
    if errorlevel 1 goto :neighbors_failed
)

rem Ejecutar eventos -> tópicos para el subset (a menos que se pida continuar desde noticias)
if not "!RUN_FROM_NEWS!"=="1" (
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python src/pipelines/event_detection/detect_events.py --config configs/events_subset.yaml
    if errorlevel 1 goto :neighbors_failed
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python src/insights/topic_motives.py --config configs/events_subset.yaml
    if errorlevel 1 goto :neighbors_failed
    rem Anotar tópicos con CCF (topics_relevance)
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python src/insights/score_topics_with_ccf.py --config configs/events_subset.yaml
    if errorlevel 1 goto :neighbors_failed
)

rem Clasificar noticias SOLO para los appids del subset (si LLM habilitado)
for %%I in (!APPID_LIST!) do (
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); set +a; python src/insights/news_classifier.py --config configs/events_subset.yaml --appid %%I"
    if errorlevel 1 goto :neighbors_failed
)

rem Generar eventos (subset) con Spark si se pide; fallback a local
if "!RUN_SPARK_BACKEND!"=="1" (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics python src/pipelines/event_detection/events_spark.py --config configs/events_subset.yaml
  if errorlevel 1 (
    echo [WARN] events_spark fallo; usando backend local
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python src/pipelines/event_detection/detect_events.py --config configs/events_subset.yaml
    if errorlevel 1 goto :neighbors_failed
  )
) else (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics python src/pipelines/event_detection/detect_events.py --config configs/events_subset.yaml
  if errorlevel 1 goto :neighbors_failed
)

rem Enriquecer (solo si existen eventos)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "if [ -f outputs/events/events.parquet ]; then python src/pipelines/event_detection/enrich_events.py --config configs/events_subset.yaml; else echo '[INFO] No existe outputs/events/events.parquet; omitiendo enrich para subset.'; fi"
if errorlevel 1 goto :neighbors_failed

rem (Opcional) Generar columna topics_summary legible a partir de 'topics'
if "!RUN_TOPICS_SUMMARY!"=="1" (
  echo [INFO] Resumiendo columna 'topics' -> 'topics_summary' (provider=!TOPICS_SUMMARY_PROVIDER!)
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "if [ -f outputs/events/enriched_events.parquet ]; then set -a; source <(sed 's/\r$//' .env); set +a; python scripts/summarize_topics_column.py --in outputs/events/enriched_events.parquet --out outputs/events/enriched_events_with_topics_summary.parquet --topics-col topics --summary-col topics_summary --provider !TOPICS_SUMMARY_PROVIDER!; else echo '[INFO] No existe enriched_events.parquet; omitiendo resumen de topics.'; fi"
  if errorlevel 1 goto :neighbors_failed
  rem Exportar topics_summary a Postgres si existe y hay conexión
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); set +a; python scripts/export_topics_summary_to_postgres.py"
  if errorlevel 1 goto :neighbors_failed
)

rem CCF limitado al subset (mantener backend local por compatibilidad de consistency)
if "!RUN_SPARK_BACKEND!"=="1" (
  rem Opcional: intentar Spark y caer a local si falla (local genera consistency requerido por topics_relevance)
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics python src/pipelines/ccf_analysis/ccf_spark.py --config configs/ccf_subset.yaml
  if errorlevel 1 (
    echo [WARN] ccf_spark fallo; usando backend local con consistency
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_subset.yaml
    if errorlevel 1 goto :neighbors_failed
  )
) else (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_subset.yaml
  if errorlevel 1 goto :neighbors_failed
)
if errorlevel 1 goto :neighbors_failed

rem Generar segmentos de reseñas (revisiones por experiencia) y aplicar al reporte
echo [INFO] Generando reviews_with_segments y review_segments...
call :RunSingleStage reviews_with_segments
if errorlevel 1 goto :neighbors_failed
call :RunSingleStage review_segments
if errorlevel 1 goto :neighbors_failed

rem Generar dashboards Altair (original vs estacionaria + métricas) por APPID del subset
echo [INFO] Generando dashboards Altair por APPID del subset...
for %%I in (!APPID_LIST!) do (
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python scripts/plot_ccf_altair.py --config configs/ccf_subset.yaml --appid %%I
    if errorlevel 1 goto :neighbors_failed
)

rem Generar PNGs (series original vs estacionaria) por APPID del subset
echo [INFO] Generando PNGs de series por APPID del subset...
for %%I in (!APPID_LIST!) do (
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python scripts/plot_ccf_series.py --config configs/ccf_subset.yaml --appid %%I
    if errorlevel 1 goto :neighbors_failed
)

rem Filtrar datasets de reseñas y tópicos SOLO a los APPIDs del subset
echo [INFO] Filtrando reviews/topics al subset de APPIDs: !APPID_LIST!
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -e APPIDS="!APPID_LIST!" -w /app/Data_analytics analytics python scripts/filter_subset_appids.py --reviews-in data/warehouse/reviews_with_segments.parquet --reviews-out data/warehouse/reviews_with_segments_subset.parquet --topics-in outputs/events/reviews_topics.parquet --topics-out outputs/events/reviews_topics_subset.parquet
if errorlevel 1 goto :neighbors_failed

rem Exportar ratios de abandono por experiencia (CSV) y opcional a Postgres (USANDO SUBSET)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/export_abandon_rates_by_experience.py --reviews data/warehouse/reviews_with_segments_subset.parquet --out outputs/events/abandon_rates_by_experience.csv --freq M --window 1 --min-samples 5 --abandon-column abandon_general
if "!RUN_PG_RECREATE!"=="1" (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); export POSTGRES_RECREATE=1; set +a; python scripts/export_abandon_rates_to_postgres.py"
) else (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); set +a; python scripts/export_abandon_rates_to_postgres.py"
)

rem Exportar tópicos por experiencia (CSV con appid) y opcional a Postgres (USANDO SUBSET)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/export_topics_by_experience.py --reviews data/warehouse/reviews_with_segments_subset.parquet --topics outputs/events/reviews_topics_subset.parquet --out outputs/events/topics_by_experience.csv --top-n 5
if "!RUN_PG_RECREATE!"=="1" (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); export POSTGRES_RECREATE=1; set +a; python scripts/export_topics_by_experience_to_postgres.py"
) else (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); set +a; python scripts/export_topics_by_experience_to_postgres.py"
)

rem Generar reportes por juego (uno por APPID del subset) usando la config del subset
for %%I in (!APPID_LIST!) do (
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python src/insights/build_game_report.py --config configs/events_subset.yaml --appid %%I --top_k 15
    if errorlevel 1 goto :neighbors_failed
)

rem Persistir reportes de los vecinos en Mongo, eliminando 'provenance' y 'rules_analysis'
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -e APPIDS="!APPID_LIST!" -w /app/Data_analytics analytics bash -lc "for id in $APPIDS; do echo $id; done > outputs/tmp_neighbors_appids.txt && python scripts/persist_reports_to_mongo.py --reports-dir outputs/reports --appids-file outputs/tmp_neighbors_appids.txt --mongo-coll app_reports --drop-fields provenance,rules_analysis"
if errorlevel 1 goto :neighbors_failed

echo.
echo ===============================================
echo Subset (vecinos) ejecutado correctamente y reportes por juego almacenados en Mongo.
echo - Eventos/Tópicos/Enrich: outputs/events/*
echo - CCF: outputs/ccf_analysis/subset_neighbors/*
echo - Reportes por appid: outputs/reports/*.json
echo - Mongo: exploitation_zone.app_reports (sin provenance ni rules_analysis)
echo ===============================================
pause
endlocal
exit /b 0

:neighbors_failed
echo.
echo ERROR: Falló en la ejecución del subset de vecinos.
echo Revisa los logs en el contenedor 'analytics'.
pause
endlocal
exit /b 1

:run_offline_all
echo.
echo ============================
echo Ejecutando analytics OFFLINE para TODOS los juegos (sin PoC)...
echo ============================

rem Si el usuario pasa APPIDs, reutilizamos el flujo de subset (neighbors)
if defined APPID_LIST (
    echo [INFO] Modo offline con subset de APPIDs: !APPID_LIST!
    set "RUN_NEIGHBORS=1"
    goto :run_neighbors
)

rem (Opcional) correr preagregados antes
call :RunSingleStage preagg_reviews
if errorlevel 1 goto :offline_failed
call :RunSingleStage preagg_players
if errorlevel 1 goto :offline_failed

rem Ejecutar stages base (SIN embeddings/clustering, se asume artefactos ya existen)
if not "!RUN_FROM_NEWS!"=="1" (
    if "!RUN_SPARK_BACKEND!"=="1" (
        call :RunSingleStage events_spark
        if errorlevel 1 (
            echo [WARN] events_spark fallo; retrocediendo a events local
            call :RunSingleStage events
            if errorlevel 1 goto :offline_failed
        )
    ) else (
        call :RunSingleStage events
        if errorlevel 1 goto :offline_failed
    )
    call :RunSingleStage topics
    if errorlevel 1 goto :offline_failed
    call :RunSingleStage topics_relevance
    if errorlevel 1 goto :offline_failed
)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics dvc repro -q --single-item news_model_select
if errorlevel 1 goto :offline_failed
call :RunSingleStage news_classifier
if errorlevel 1 goto :offline_failed
call :RunSingleStage enrich
if errorlevel 1 goto :offline_failed

rem (Opcional) Generar y exportar topics_summary en offline-all
if "!RUN_TOPICS_SUMMARY!"=="1" (
  echo [INFO] Resumiendo columna 'topics' -> 'topics_summary' (provider=!TOPICS_SUMMARY_PROVIDER!)
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "if [ -f outputs/events/enriched_events.parquet ]; then set -a; source <(sed 's/\r$//' .env); set +a; python scripts/summarize_topics_column.py --in outputs/events/enriched_events.parquet --out outputs/events/enriched_events_with_topics_summary.parquet --topics-col topics --summary-col topics_summary --provider !TOPICS_SUMMARY_PROVIDER!; else echo '[INFO] No existe enriched_events.parquet; omitiendo resumen de topics.'; fi"
  if errorlevel 1 goto :offline_failed
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics bash -lc "set -a; source <(sed 's/\r$//' .env); set +a; python scripts/export_topics_summary_to_postgres.py"
  if errorlevel 1 goto :offline_failed
)
call :RunSingleStage reviews_with_segments
if errorlevel 1 goto :offline_failed
call :RunSingleStage review_segments
if errorlevel 1 goto :offline_failed

rem Exportar ratios de abandono por experiencia (CSV) y opcional a Postgres (offline-all)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/export_abandon_rates_by_experience.py --reviews data/warehouse/reviews_with_segments.parquet --out outputs/events/abandon_rates_by_experience.csv --freq M --window 1 --min-samples 5 --abandon-column abandon_general
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python - <<'PY'
import os, pandas as pd
try:
  from sqlalchemy import create_engine
  SQLA=True
except Exception:
  SQLA=False
uri=os.getenv('POSTGRES_URI')
if not uri:
  host=os.getenv('POSTGRES_HOST'); user=os.getenv('POSTGRES_USER'); pwd=os.getenv('POSTGRES_PASSWORD'); db=os.getenv('POSTGRES_DB'); port=os.getenv('POSTGRES_PORT','5432')
  if host and user and pwd and db:
    uri=f'postgresql://{user}:{pwd}@{host}:{port}/{db}'
if not uri or not SQLA:
  print('[INFO] Postgres no configurado; omitiendo export de abandon rates')
  raise SystemExit(0)
path_csv='outputs/events/abandon_rates_by_experience.csv'
if not os.path.exists(path_csv):
  print('[INFO] No existe CSV de abandon rates; omitiendo export')
  raise SystemExit(0)
df=pd.read_csv(path_csv)
if df.empty:
  print('[INFO] CSV vacío; omitiendo export')
  raise SystemExit(0)
schema=os.getenv('POSTGRES_SCHEMA','public')
engine=create_engine(uri)
df.to_sql('abandon_rates_by_experience', engine, schema=schema, if_exists='append', index=False)
print('[OK] Exportado abandon_rates_by_experience a Postgres')
PY"
if "!RUN_SPARK_BACKEND!"=="1" (
    call :RunSingleStage ccf
    if errorlevel 1 goto :offline_failed
) else (
    call :RunSingleStage ccf
    if errorlevel 1 goto :offline_failed
)
call :RunSingleStage event_leadlag
if errorlevel 1 goto :offline_failed

rem Construir reportes por juego para TODOS los appids del clusters.parquet
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python - <<'PY'
import pandas as pd
from pathlib import Path
import sys
df = pd.read_parquet('data/processed/clusters.parquet')
apps = sorted(set(df['appid'].astype(str)))
Path('outputs').mkdir(exist_ok=True)
Path('outputs/tmp_all_appids.txt').write_text('\n'.join(apps), encoding='utf-8')
print(f'[OK] AppIDs totales: {len(apps)}')
PY"
if errorlevel 1 goto :offline_failed

docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "set -e; while IFS= read -r A; do [ -z \"$A\" ] && continue; python src/insights/build_game_report.py --config configs/events.yaml --appid \"$A\" --top_k 15 || exit 1; done < outputs/tmp_all_appids.txt"
if errorlevel 1 goto :offline_failed

rem Persistir TODOS los reportes a Mongo (sin provenance ni rules_analysis)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python scripts/persist_reports_to_mongo.py --reports-dir outputs/reports --mongo-coll app_reports --drop-fields provenance,rules_analysis
if errorlevel 1 goto :offline_failed

rem Generar dashboards Altair para TODOS los appids
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "set -e; while IFS= read -r A; do [ -z \"$A\" ] && continue; python scripts/plot_ccf_altair.py --config configs/ccf_analysis.yaml --appid \"$A\" || exit 1; done < outputs/tmp_all_appids.txt"
if errorlevel 1 goto :offline_failed

rem Generar PNGs de series para TODOS los appids
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "set -e; while IFS= read -r A; do [ -z \"$A\" ] && continue; python scripts/plot_ccf_series.py --config configs/ccf_analysis.yaml --appid \"$A\" || exit 1; done < outputs/tmp_all_appids.txt"
if errorlevel 1 goto :offline_failed

echo.
echo ===============================================
echo Offline (todos los juegos) ejecutado y almacenado en Mongo.
echo - Reportes: outputs/reports/*.json
echo - Mongo: analytics.app_reports (sin provenance ni rules_analysis)
echo ===============================================
pause
endlocal
exit /b 0

:offline_failed
echo.
echo ERROR: Falló en la ejecución offline para todos los juegos.
pause
endlocal
exit /b 1

:run_poc_client
echo.
echo ============================
echo Preparando cliente (vecinos y appids) y ejecutando subset...
echo ============================

if not defined CLIENT_ID (
    set "CLIENT_ID=client-001"
)

rem Ejecuta pipeline de cliente para derivar vecinos
if defined CLIENT_FILE (
    rem Opcional: actualizar neighbor_strategy.min_score si se pasó min-score=
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -e NS_MIN_SCORE="!NS_MIN_SCORE!" -w /app/Data_analytics analytics bash -lc "python - <<'PY'
import os, yaml
p='configs/params.yaml'
ms=os.getenv('NS_MIN_SCORE')
if ms:
  with open(p,'r',encoding='utf-8') as f:
    cfg=yaml.safe_load(f) or {}
  ns = cfg.get('neighbor_strategy') or {}
  try:
    ns['min_score'] = float(ms)
  except Exception:
    ns['min_score'] = ms
  cfg['neighbor_strategy']=ns
  open(p,'w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))
  print(f'[OK] params.yaml actualizado: neighbor_strategy.min_score={ns["min_score"]}')
else:
  print('[INFO] NS_MIN_SCORE no definido; se mantiene configuración actual')
PY"
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python scripts/poc_client_pipeline.py --client-file !CLIENT_FILE! --client-id !CLIENT_ID!
) else (
    echo [WARN] CLIENT_FILE no especificado; usando configs/clients/!CLIENT_ID!.json si existe.
    rem Opcional: actualizar neighbor_strategy.min_score si se pasó min-score=
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -e NS_MIN_SCORE="!NS_MIN_SCORE!" -w /app/Data_analytics analytics bash -lc "python - <<'PY'
import os, yaml
p='configs/params.yaml'
ms=os.getenv('NS_MIN_SCORE')
if ms:
  with open(p,'r',encoding='utf-8') as f:
    cfg=yaml.safe_load(f) or {}
  ns = cfg.get('neighbor_strategy') or {}
  try:
    ns['min_score'] = float(ms)
  except Exception:
    ns['min_score'] = ms
  cfg['neighbor_strategy']=ns
  open(p,'w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))
  print(f'[OK] params.yaml actualizado: neighbor_strategy.min_score={ns["min_score"]}')
else:
  print('[INFO] NS_MIN_SCORE no definido; se mantiene configuración actual')
PY"
    docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
      exec -w /app/Data_analytics analytics python scripts/poc_client_pipeline.py --client-id !CLIENT_ID!
)
if errorlevel 1 (
    echo [ERROR] No se pudieron calcular vecinos del cliente.
    pause
    endlocal
    exit /b 1
)

rem Actualiza params.yaml con client_id y client_file
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python - <<'PY'
import yaml,sys,os
p='configs/params.yaml'
with open(p,'r',encoding='utf-8') as f:
  cfg=yaml.safe_load(f) or {}
cr=cfg.get('client_report') or {}
cr['client_id']=os.environ.get('CID','client-001')
cf=os.environ.get('CFILE') or f'configs/clients/{cr["client_id"]}.json'
cr['client_file']=cf
cfg['client_report']=cr
open(p,'w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))
print('[OK] params.yaml actualizado')
PY"
if errorlevel 1 (
    echo [ERROR] No se pudo actualizar configs/params.yaml.
    pause
    endlocal
    exit /b 1
)

rem Leer appids desde outputs/clients/client_{id}_appids.txt
for /f "usebackq tokens=*" %%L in ("Data_analytics\outputs\clients\client_!CLIENT_ID!_appids.txt") do set "APPID_LIST=%%L"
if not defined APPID_LIST (
    echo [ERROR] No se encontraron appids de vecinos para el cliente !CLIENT_ID!.
    pause
    endlocal
    exit /b 1
)

rem Encadena al modo neighbors con los appids obtenidos
set "RUN_NEIGHBORS=1"
goto :run_neighbors

:run_reviews_mongo
echo.
echo ============================
echo Generando reviews y tópicos desde MongoDB...
echo ============================
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python src/pipelines/review_segments/prepare_reviews_with_segments.py --config configs/review_segments.yaml --run-bertopic"
if errorlevel 1 goto :reviews_mongo_failed
goto :reviews_mongo_success

:reviews_mongo_failed
echo.
echo ERROR: No se pudieron generar las reviews desde MongoDB.
pause
endlocal
exit /b 1

:run_news_train
echo.
echo ============================
echo Entrenando clasificadores de noticias (multi-modelo, selección automática)...
echo ============================
rem Asegurar dataset de entrenamiento con etiquetas (si no existe, clasificar en batch con LLM actual)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python - <<'PY'
from pathlib import Path
from subprocess import run
import sys
if not Path('outputs/events/news_classified.parquet').exists():
    print('[INFO] news_classified.parquet no existe. Ejecutando clasificación batch con LLM para generarlo...')
    rc = run(['python','src/insights/news_classifier.py','--config','configs/events.yaml']).returncode
    sys.exit(rc)
print('[OK] Dataset de entrenamiento existente: outputs/events/news_classified.parquet')
PY"
if errorlevel 1 (
    if not defined CV_K set "CV_K=5"
    echo [ERROR] No se pudo generar outputs/events/news_classified.parquet. Revisa credenciales del LLM o la config.
    pause
    endlocal
    exit /b 1
)

set "TRAIN_ARGS=--input outputs/events/news_classified.parquet --text-cols title,contents"
if defined TRAIN_MODELS set "TRAIN_ARGS=!TRAIN_ARGS! --models !TRAIN_MODELS!"
if not defined TRAIN_MODELS set "TRAIN_ARGS=!TRAIN_ARGS! --models all"
if defined TRAIN_SCORING set "TRAIN_ARGS=!TRAIN_ARGS! --scoring !TRAIN_SCORING!"
if not defined TRAIN_SCORING set "TRAIN_ARGS=!TRAIN_ARGS! --scoring f1_macro"
if defined FEATURIZER set "TRAIN_ARGS=!TRAIN_ARGS! --featurizer !FEATURIZER!"
if defined EMB_MODEL set "TRAIN_ARGS=!TRAIN_ARGS! --embedding-model !EMB_MODEL!"
if defined CV_K set "TRAIN_ARGS=!TRAIN_ARGS! --cv !CV_K!"

docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/insights/train_news_classifier_auto.py !TRAIN_ARGS!
if errorlevel 1 (
    echo [ERROR] Falló al entrenar el SVM de noticias. Asegúrate de haber generado outputs/events/news_classified.parquet primero (news_classifier).
    pause
    endlocal
    exit /b 1
)
echo [OK] Modelos entrenados. Mejor guardado en models/news_best.joblib
if "!USE_SVM!"=="1" (
    echo [INFO] Flag use-svm activo: el subset usará provider=svm con models/news_best.joblib
)
pause
endlocal
exit /b 0

:reviews_mongo_success
echo.
echo ===============================================
echo Reviews segmentadas y tópicos generados desde MongoDB.
echo Revisar: data/warehouse/reviews_with_segments.parquet
echo          outputs/events/reviews_topics.parquet
echo ===============================================
pause
endlocal
exit /b 0

:ccf_failed
echo.
echo ERROR: La ejecución de CCF/Granger falló.
echo Revisa la salida del contenedor analytics.
pause
endlocal
exit /b 1
