@echo off
setlocal enabledelayedexpansion

cd /d %~dp0
set COMPOSE_PROJECT_NAME=proyecto_steam

echo.
echo ============================
echo 0) Rebuild de imagenes (app, mlflow, analytics)...
echo ============================
docker compose build %BUILD_ARGS% app mlflow analytics
if errorlevel 1 (
  echo.
  echo *** ERROR: fallo el rebuild de imagenes. Abortando. ***
  pause
  exit /b 1
)

echo.
echo ============================
echo 1) Asegurando Mongo levantado...
echo ============================
docker compose up -d mongo

echo.
echo ============================
echo 2) Ejecutando job de siembra (app) y esperando a que termine...
echo ============================
rem Opción A: correr app como servicio del proyecto y esperar su salida
rem Esto mantiene el contenedor como parte del proyecto y captura exit code
docker compose --profile seed up --abort-on-container-exit --exit-code-from app app
if errorlevel 1 (
  echo.
  echo *** ERROR: el job 'app' ha fallado. Abortando. ***
  pause
  exit /b 1
)

echo.
echo ============================
echo 3) Levantando MLflow y Analytics...
echo ============================

rem Con MLflow:
docker compose up -d mlflow analytics


echo.
echo ============================
echo Ver logs de app + analytics (Ctrl+C para salir)...
echo ============================
docker compose logs -f app analytics

endlocal
pause
