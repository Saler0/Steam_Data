@echo off
setlocal enabledelayedexpansion

rem Ir a la carpeta del script (donde está docker-compose.yml)
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



REM Ejecuta el comando DVC DENTRO del contenedor 'analytics'
docker compose exec analytics dvc repro clustering
if errorlevel 1 (
    echo.
    echo ERROR: Fallo al ejecutar el pipeline de DVC.
    echo Revisa los logs del contenedor para ver el error.
    pause
    exit /b 1
)

echo.
echo =======================================================
echo [PASO 4 de 4] Pipeline finalizado con exito!
echo =======================================================
echo.
echo Los resultados del clustering se encuentran en:
echo - El fichero: data/processed/clusters.parquet
echo - La base de datos MongoDB (coleccion: game_clusters)
echo.
echo Puedes apagar los contenedores ejecutando: docker-compose down
echo.
pause

endlocal
pause
