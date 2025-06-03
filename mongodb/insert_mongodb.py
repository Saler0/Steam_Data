import ndjson
from pymongo import MongoClient
import os
import csv

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["steam_data"]
collection = db["steam_data"]
log_collection = db["import_log"]  # Para rastrear archivos insertados

# Ruta de la carpeta con los archivos .ndjson
carpeta = r"C:\njson_al_cluster_upc"

# Insertar documentos desde archivos NDJSON
for nombre_archivo in os.listdir(carpeta):
    if nombre_archivo.endswith(".ndjson"):
        # Verificar si ya fue insertado
        if log_collection.find_one({"archivo": nombre_archivo}):
            print(f"{nombre_archivo} ya fue importado. Saltando...")
            continue

        with open(os.path.join(carpeta, nombre_archivo), "r", encoding="utf-8") as f:
            datos = ndjson.load(f)
            if datos:
                collection.insert_many(datos)
                log_collection.insert_one({"archivo": nombre_archivo})
                print(f"{nombre_archivo} importado con éxito.")
            else:
                print(f"{nombre_archivo} está vacío o malformado.")

# 1. Exportar documentos con errores (error_details o error_reviews)
errores_query = {
    "$or": [
        {"error_details": True},
        {"error_reviews": True}
    ]
}
errores_cursor = collection.find(errores_query, {"appid": 1, "name": 1, "_id": 0})

with open("errores.csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["appid", "name"])
    writer.writeheader()
    for doc in errores_cursor:
        writer.writerow(doc)

print("Exportado errores.csv")

# 2. Exportar juegos (details.type == "game")
juegos_query = {"details.type": "game"}
juegos_cursor = collection.find(juegos_query, {"appid": 1, "name": 1, "_id": 0})

with open("juegos.csv", "w", newline='', encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["appid", "name"])
    writer.writeheader()
    for doc in juegos_cursor:
        writer.writerow(doc)

print("Exportado juegos.csv")
