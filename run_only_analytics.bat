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
set "POC_ARGS="


for %%A in (%*) do (
    if /I "%%A"=="skip-embeddings" set "SKIP_EMB=1"
    if /I "%%A"=="topics-only" set "RUN_TOPICS_ONLY=1"
    if /I "%%A"=="single-game-poc" set "RUN_POC=1"
)

if "!RUN_POC!"=="1" goto :run_poc

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

:run_poc
echo.
echo ============================
echo Ejecutando PoC de asignacion de juego...
echo ============================
set "POC_ARGS=%*"
set "POC_ARGS=!POC_ARGS:single-game-poc=!"
set "POC_ARGS=!POC_ARGS:skip-embeddings=!"
set "POC_ARGS=!POC_ARGS:topics-only=!"
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

:RunSingleStage
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics dvc repro --single-item %1
exit /b %errorlevel%
