#!/bin/sh

# Salir inmediatamente si un comando falla
set -e

# 1. Inicializar la base de datos
# Este comando crea el archivo .db y la tabla si no existen.
echo "--- Inicializando la base de datos SQLite... ---"
python db/sqllite.py

# 2. Ejecutar el proceso principal de scraping
# "$@" pasa todos los argumentos del comando CMD (o los proporcionados en 'docker run') al script.
echo "--- Lanzando el scraper mensual... ---"
exec python main_mensual.py "$@"
