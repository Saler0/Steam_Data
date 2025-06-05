import ndjson
from pymongo import MongoClient
import os
import csv
import json


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
# juegos_cursor = collection.find(
#     {"details.type": "game"},
#     {
#         "_id": 0,
#         "error_details": 0,
#         "reviews": 0,
#         "error_reviews": 0,
#         "details.screenshots": 0,
#         "details.movies": 0,
#         "details.achievements": 0,
#         "details.background": 0,
#         "details.background_raw": 0,
#         "details.steam_appid": 0,
#         "details.header_image": 0,
#         "details.capsule_image": 0,
#         "details.capsule_imagev5": 0,
#         "details.support_info": 0
#     }
# )

# # Convertir cursor a lista de diccionarios
# juegos_lista = list(juegos_cursor)

# # Guardar en archivo JSON
# with open("juegos.json", "w", encoding="utf-8") as f:
#     json.dump(juegos_lista, f, ensure_ascii=False, indent=4)

# print("Exportado juegos.json sin screenshots, movies ni achievements")



# 3. ¿Cuál es el juego con mayor número de géneros?
pipeline = [
    {
        "$project": {
            "appid": 1,
            "name": 1,
            # Si details.genres no existe, lo sustituimos por [] para que $size siempre reciba un arreglo
            "num_genres": { "$size": { "$ifNull": ["$details.genres", []] } }
        }
    },
    { "$sort": { "num_genres": -1 } },
    { "$limit": 1 }
]

resultado = list(collection.aggregate(pipeline))

if resultado:
    juego = resultado[0]
    print("\nJuego con más géneros:")
    print(f"  appid: {juego['appid']}")
    print(f"  name: {juego['name']}")
    print(f"  número de géneros: {juego['num_genres']}")
else:
    print("No se encontró ningún juego con géneros.")


# 4. Cantidad total de generos de steam
generos_unicos = collection.distinct("details.genres.description")

# Si prefieres basarte en el ID de género, usa:
# generos_unicos = collection.distinct("details.genres.id")

total_generos = len(generos_unicos)
print(f"\nNúmero total de géneros distintos: {total_generos}")