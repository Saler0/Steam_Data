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
docker rm -f steam_mlflow 2>nul
docker rm -f steam_analytics 2>nul
docker rm -f data_management_pipeline 2>nul
docker rm -f postgres_db 2>nul

echo.
echo ============================
echo Levantando solo Mongo + MLflow y Analytics...
echo ============================
docker compose up -d mongo postgres mlflow analytics
if errorlevel 1 (
    echo.
    echo ERROR: No se pudieron levantar los servicios requeridos.
    pause
    exit /b 1
)

set "DVC_STAGES=embeddings clustering"
IF /I "%~1"=="skip-embeddings" (
  set "DVC_STAGES=clustering"
  echo [INFO] Saltando regeneracion de embeddings; se usaran artefactos existentes.
)

echo.
echo ============================
echo Ejecutando DVC (%DVC_STAGES%) dentro de steam_analytics...
echo ============================
rem Marca /app como safe en Git (por si hay "dubious ownership")
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec analytics git config --global --add safe.directory /app

rem Inicializa DVC en el subdirectorio si aún no existe .dvc en la raíz
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics bash -lc "test -d /app/.dvc || dvc init -f --subdir"

rem Ahora sí, ejecutar el pipeline en la carpeta del repo DVC:
docker compose -f "docker-compose.yml" --project-directory . --profile analytics --profile mlflow ^
  exec -w /app/Data_analytics analytics dvc repro %DVC_STAGES%

if errorlevel 1 (
    echo.
    echo ERROR: Fallo al ejecutar el pipeline de DVC.
    echo Revisa los logs del contenedor para ver el error.
    pause
    exit /b 1
)

echo.
echo =======================================================
echo Pipeline de analytics finalizado con exito.
echo =======================================================

echo Resultados del clustering:

echo - data/processed/clusters.parquet

echo - models/cluster_medoids.json

echo - outputs/clustering/cluster_stats.csv

echo.
echo Puedes apagar los contenedores con: docker compose down

echo.
pause

endlocal
