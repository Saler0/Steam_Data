@echo off
REM -------------------------------------------------
REM  Script sencillo: build + up en un solo comando
REM -------------------------------------------------

cd /d %~dp0



echo.
echo ============================
echo Parando cualquier contenedor existente...
echo ============================
docker stop mongo steam_data-app 2>nul

echo.
echo ============================
echo Eliminando contenedores detenidos...
echo ============================
docker rm mongo steam_data-app 2>nul


echo.
echo ============================
echo Deteniendo y eliminando contenedores del compose…
echo ============================
docker compose down


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

echo.
echo Siguiendo logs de “app” (pantalla + app.log)...
powershell -NoLogo -Command ^
  "docker compose logs -f app | Tee-Object -FilePath 'app.log'"
pause