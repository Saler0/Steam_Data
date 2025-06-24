@echo off
REM -------------------------------------------------
REM  Script sencillo: build + up en un solo comando
REM -------------------------------------------------

cd /d %~dp0

echo.
echo ============================
echo Reconstruyendo y levantando...
echo ============================
docker compose up --build -d
if errorlevel 1 (
    echo.
    echo ERROR al construir o levantar. Revisa los logs.
    pause
    exit /b 1
)

echo.
echo Contenedores en ejecución:
docker ps

echo logs:
docker compose logs -f app

pause