@echo off
REM ---------------------------------------------------------------------
REM Script para levantar el entorno Docker y ejecutar el pipeline
REM de análisis de Steam HASTA la fase de clustering.
REM ---------------------------------------------------------------------

REM Asegura que el script se ejecuta desde su propio directorio
cd /d %~dp0

echo.
echo =======================================================
echo [PASO 1 de 4] Limpiando entorno Docker anterior...
echo =======================================================
docker compose down

echo.
echo =======================================================
echo [PASO 2 de 4] Reconstruyendo y levantando servicios...
echo =======================================================
docker compose up --build -d
if errorlevel 1 (
    echo.
    echo ERROR: Fallo al construir o levantar los contenedores.
    echo Revisa los logs de Docker y el fichero docker-compose.yml.
    pause
    exit /b 1
)

echo.
echo =======================================================
echo [PASO 3 de 4] Entorno listo. Ejecutando pipeline...
echo (Esto puede tardar varios minutos la primera vez)
echo =======================================================

REM Ejecuta el comando DVC DENTRO del contenedor 'app'
docker compose exec app dvc repro clustering
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
