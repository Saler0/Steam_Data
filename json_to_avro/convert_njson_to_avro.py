from pyspark.sql import SparkSession
import os

# Crear sesión Spark local
spark = SparkSession.builder \
    .appName("NDJSONtoAvro") \
    .master("local[*]") \
    .getOrCreate()

# Carpeta donde están los JSON
json_dir = "/home/bdma08/"
avro_output_dir = "/home/bdma08/steam_avro_local"

# Crear carpeta de salida si no existe
os.makedirs(avro_output_dir, exist_ok=True)

# Listar los archivos JSON que nos interesan
json_files = [f for f in os.listdir(json_dir)
              if f.startswith("steam_games_data") and f.endswith(".ndjson")]

for filename in json_files:
    input_path = os.path.join(json_dir, filename)
    output_path = os.path.join(avro_output_dir, filename.replace(".ndjson", ".avro"))
    print(f"Convirtiendo {filename} → {output_path}")

    try:
        df = spark.read.json(input_path)
        df.write.format("avro").save(output_path)
    except Exception as e:
        print(f"⚠️ Error procesando {filename}: {e}")

spark.stop()