@echo off
setlocal enabledelayedexpansion

rem Ir a la carpeta del script (donde esta docker-compose.yml)
cd /d %~dp0

rem Asegura nombre de proyecto consistente con tu compose (name: proyecto_steam)
set COMPOSE_PROJECT_NAME=proyecto_steam

echo.
echo ============================
echo Limpiando contenedores previos con nombre estatico...
echo ============================
rem Borra si existen; ignora errores
docker rm -f mongo 2>nul
rem docker rm -f steam_mlflow 2>nul
docker rm -f steam_analytics 2>nul
docker rm -f data_management_pipeline 2>nul
rem docker rm -f postgres_db 2>nul

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
set "PG_TABLE="
set "RUN_POC_CLIENT=0"
set "CLIENT_ID="
set "CLIENT_FILE="

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
    if /I "!ARG:~0,9!"=="pg-table=" set "PG_TABLE=!ARG:~9!"
    if /I "!ARG!"=="poc-client" set "RUN_POC_CLIENT=1"
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
        if not "!ARG!"=="with-preagg" if not "!ARG!"=="players-pg" if not "!ARG!"=="pg-table=" if not "!ARG!"=="exploitation_zone" (
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

rem --- ramas de subcomandos ---
if "!RUN_CCF_ONE!"=="1" goto :run_ccf_one
if "!RUN_NEIGHBORS!"=="1" goto :run_neighbors
if "!RUN_POC!"=="1" goto :run_poc
if "!RUN_REVIEWS_MONGO!"=="1" goto :run_reviews_mongo
if "!RUN_POC_CLIENT!"=="1" goto :run_poc_client

if defined APPID_LIST (
    set "APPID_LIST=!APPID_LIST:,= !"
)

if "!CUSTOM_MODE!"=="0" (
    if "!RUN_TOPICS_ONLY!"=="1" (
        set "DVC_TARGETS=cluster_topics_profile cluster_topics_map"
        set "DVC_FLAGS=--single-item"
        echo [INFO] Ejecutando solo los stages de topicos; se reutilizaran artefactos previos de clustering.
    ) else (
        if "!SKIP_EMB!"=="1" (
            set "DVC_TARGETS=clustering cluster_topics_profile cluster_topics_map"
            set "DVC_FLAGS=--single-item"
            echo [INFO] Saltando regeneracion de embeddings; se usaran artefactos existentes.
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

rem Inicializa DVC en el subdirectorio si aun no existe .dvc en la raiz
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "test -d /app/.dvc || dvc init -f --subdir"

rem Ejecuta el pipeline segun la configuracion seleccionada
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
rem Crea parquet con un unico appid
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
echo Ejecutando PoC de asignacion de juego...
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
echo ERROR: Fallo al ejecutar el pipeline de DVC.
echo Revisa los logs del contenedor para ver el error.
pause
endlocal
exit /b 1

:dvc_success
echo.
echo =======================================================
echo Pipeline de analytics finalizado con exito.
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
echo ERROR: La PoC de asignacion de juego fallo.
echo Revisa la salida del comando python dentro del contenedor analytics.
pause
endlocal
exit /b 1

:poc_success
echo.
echo ===============================================
echo PoC de asignacion ejecutada correctamente.
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

rem Generar configs/events_subset.yaml apuntando al parquet temporal
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python -c \"import yaml,io; cfg=yaml.safe_load(open('configs/events.yaml','r',encoding='utf-8')); ip=cfg.get('input_paths') or {}; ip['clusters_parquet']='data/processed/_tmp_neighbors_clusters.parquet'; cfg['input_paths']=ip; cfg['clusters_parquet']='data/processed/_tmp_neighbors_clusters.parquet'; open('configs/events_subset.yaml','w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))\""
if errorlevel 1 (
    echo [ERROR] No se pudo generar configs/events_subset.yaml.
    pause
    endlocal
    exit /b 1
)

rem Generar configs/ccf_subset.yaml apuntando al parquet temporal
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "python -c \"import yaml,io; cfg=yaml.safe_load(open('configs/ccf_analysis.yaml','r',encoding='utf-8')); cfg['input_path']['clusters_parquet']='data/processed/_tmp_neighbors_clusters.parquet'; cfg['output_dir']='outputs/ccf_analysis/subset_neighbors'; open('configs/ccf_subset.yaml','w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))\""
if errorlevel 1 (
    echo [ERROR] No se pudo generar configs/ccf_subset.yaml.
    pause
    endlocal
    exit /b 1
)

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

rem Opcional: ejecutar preagregados antes si se pidio
if "!RUN_PREAGG_BEFORE!"=="1" (
    echo [INFO] Ejecutando preagg_reviews y preagg_players antes del subset...
    call :RunSingleStage preagg_reviews
    if errorlevel 1 goto :neighbors_failed
    call :RunSingleStage preagg_players
    if errorlevel 1 goto :neighbors_failed
)

rem Ejecutar eventos -> topicos para el subset
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/pipelines/event_detection/detect_events.py --config configs/events_subset.yaml
if errorlevel 1 goto :neighbors_failed

docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/insights/topic_motives.py --config configs/events_subset.yaml
if errorlevel 1 goto :neighbors_failed

rem Anotar topicos con CCF (topics_relevance)
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/insights/score_topics_with_ccf.py --config configs/events_subset.yaml
if errorlevel 1 goto :neighbors_failed

rem Clasificar noticias SOLO para los appids del subset (si LLM habilitado)
for %%I in (!APPID_LIST!) do (
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics python src/insights/news_classifier.py --config configs/events_subset.yaml --appid %%I
  if errorlevel 1 goto :neighbors_failed
)

rem Enriquecer
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/pipelines/event_detection/enrich_events.py --config configs/events_subset.yaml
if errorlevel 1 goto :neighbors_failed

rem CCF limitado al subset
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics python src/pipelines/ccf_analysis/analyze_competitors_ccf.py --config configs/ccf_subset.yaml
if errorlevel 1 goto :neighbors_failed

rem Generar segmentos de reseñas (revisiones por experiencia) y aplicar al reporte
echo [INFO] Generando reviews_with_segments y review_segments...
call :RunSingleStage reviews_with_segments
if errorlevel 1 goto :neighbors_failed
call :RunSingleStage review_segments
if errorlevel 1 goto :neighbors_failed

rem Reglas de decision (prepare/apply/evaluate)
echo [INFO] Ejecutando reglas de decision...
call :RunSingleStage prepare
if errorlevel 1 goto :neighbors_failed
call :RunSingleStage apply_rules
if errorlevel 1 goto :neighbors_failed
call :RunSingleStage evaluate
if errorlevel 1 goto :neighbors_failed

rem Generar reporte de cliente (usa configs/params.yaml:client_report)
echo [INFO] Generando client_report a partir de params...
call :RunSingleStage client_report
if errorlevel 1 goto :neighbors_failed

echo.
echo ===============================================
echo Subset (vecinos) ejecutado correctamente y reporte de cliente generado.
echo - Eventos/Topicos/Enrich: outputs/events/*
echo - CCF: outputs/ccf_analysis/subset_neighbors/*
echo - Reporte cliente: outputs/reports/client_*.json
echo ===============================================
pause
endlocal
exit /b 0

:neighbors_failed
echo.
echo ERROR: Fallo en la ejecucion del subset de vecinos.
echo Revisa los logs en el contenedor 'analytics'.
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
  docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
    exec -w /app/Data_analytics analytics python scripts/poc_client_pipeline.py --client-file !CLIENT_FILE! --client-id !CLIENT_ID!
) else (
  echo [WARN] CLIENT_FILE no especificado; usando configs/clients/!CLIENT_ID!.json si existe.
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
  exec -w /app/Data_analytics analytics bash -lc "python - <<'PY'\nimport yaml,sys,os\np='configs/params.yaml'\nwith open(p,'r',encoding='utf-8') as f:\n  cfg=yaml.safe_load(f) or {}\ncr=cfg.get('client_report') or {}\ncr['client_id']=os.environ.get('CID','client-001')\ncf=os.environ.get('CFILE') or f'configs/clients/{cr["client_id"]}.json'\ncr['client_file']=cf\ncfg['client_report']=cr\nopen(p,'w',encoding='utf-8').write(yaml.safe_dump(cfg,sort_keys=False,allow_unicode=True))\nprint('[OK] params.yaml actualizado')\nPY" 
  
if errorlevel 1 (
  echo [ERROR] No se pudo actualizar configs/params.yaml.
  pause
  endlocal
  exit /b 1
)

rem Leer appids desde outputs/clients/client_{id}_appids.txt
for /f "usebackq tokens=*" %%L in ("Data_analytics\\outputs\\clients\\client_!CLIENT_ID!_appids.txt") do set "APPID_LIST=%%L"
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
echo Generando reviews y topicos desde MongoDB...
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

:reviews_mongo_success
echo.
echo ===============================================
echo Reviews segmentadas y topicos generados desde MongoDB.
echo Revisar: data/warehouse/reviews_with_segments.parquet
echo          outputs/events/reviews_topics.parquet
echo ===============================================
pause
endlocal
exit /b 0

:ccf_failed
echo.
echo ERROR: La ejecucion de CCF/Granger fallo.
echo Revisa la salida del contenedor analytics.
pause
endlocal
exit /b 1
