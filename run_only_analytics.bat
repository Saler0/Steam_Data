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

echo.
echo ============================
echo Levantando solo Mongo + MLflow y Analytics...
echo ============================
docker compose up -d mongo mlflow analytics

echo.
echo ============================
echo Ver logs de analytics (Ctrl+C para salir)...
echo ============================
docker compose logs -f analytics

endlocal
pause
