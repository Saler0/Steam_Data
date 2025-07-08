import os
import datetime
import numpy as np
import pandas as pd
import joblib
import faiss
import yaml

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans as SparkKMeans
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import ClusteringEvaluator
from modelos_steam_data.mongodb import MongoDBClient
from modelos_steam_data.features import GameFeatureExtractor
import mlflow
mlflow.set_experiment("clustering_kmeans_faiss_spark")

# === Spark Session ===
spark = SparkSession.builder.appName("ClusteringFAISS").getOrCreate()

# === Cargar parámetros ===
with open("params.yaml") as f:
    params = yaml.safe_load(f)
    min_k = params["clustering"]["min_k"]
    max_k = params["clustering"]["max_k"]
    random_state = params["clustering"]["random_state"]

# === Trackear con MLflow ===
with mlflow.start_run():
    mlflow.log_param("min_k", min_k)
    mlflow.log_param("max_k", max_k)
    mlflow.log_param("random_state", random_state)
    mlflow.log_param("n_init", n_init)


# === Paths ===
MODEL_DIR = "models/clustering"
ENCODER_DIR = "models/encoders"
DATA_DIR = "data/processed"
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(ENCODER_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# === 1. Cargar datos desde MongoDB ===
mongo = MongoDBClient()
cursor = mongo.juegos.find({}, {
    "appid": 1,
    "name": 1,
    "genres": 1,
    "categories": 1,
    "required_age": 1,
    "detailed_description": 1,
    "about_the_game": 1,
    "short_description": 1
})
df = pd.DataFrame(list(cursor))
if df.empty:
    raise ValueError("No se encontraron juegos en MongoDB.")

# === 2. Generar embeddings ===
extractor = GameFeatureExtractor(mode="clustering")
X_np, df_enriquecido = extractor.transform(df)

# === 3. Convertir embeddings a Spark DataFrame ===
df_embeddings = pd.DataFrame([Vectors.dense(x) for x in X_np], columns=["features"])
spark_df = spark.createDataFrame(df_embeddings)

# === 4. Buscar mejor K con Spark MLlib ===
print("Buscando el mejor k con Spark MLlib...")
best_k = None
best_score = -1
best_model = None
scores = []

evaluator = ClusteringEvaluator()

for k in range(min_k, max_k + 1):
    kmeans = SparkKMeans(k=k, seed=random_state, featuresCol="features")
    model = kmeans.fit(spark_df)
    predictions = model.transform(spark_df)
    silhouette = evaluator.evaluate(predictions)
    print(f"  → k={k}, silhouette={silhouette:.4f}")
    scores.append((k, silhouette))
    if silhouette > best_score:
        best_k = k
        best_score = silhouette
        best_model = model

print(f"Mejor k: {best_k} con silhouette={best_score:.4f}")

# === Trackear con MLflow ===
mlflow.log_metric("best_k", best_k)
mlflow.log_metric("best_silhouette", best_score)

# === 5. Predecir clusters y guardar ===
final_predictions = best_model.transform(spark_df).toPandas()
df_enriquecido["cluster"] = final_predictions["prediction"].values

timestamp = datetime.datetime.now().strftime("%Y%m%d")

# Guardar modelo FAISS
index = faiss.IndexFlatIP(X_np.shape[1])
faiss.normalize_L2(X_np)
index.add(X_np)
faiss.write_index(index, f"{MODEL_DIR}/faiss_index_{timestamp}.index")
faiss.write_index(index, f"{MODEL_DIR}/faiss_index_latest.index")

# Guardar binarizadores
joblib.dump(extractor.mlb_genres, f"{ENCODER_DIR}/mlb_genres.pkl")
joblib.dump(extractor.mlb_categories, f"{ENCODER_DIR}/mlb_categories.pkl")

# Guardar embeddings
np.save(f"{DATA_DIR}/embeddings_{timestamp}.npy", X_np)
np.save(f"{DATA_DIR}/embeddings_latest.npy", X_np)

# Guardar dataframe con clusters
df_out = df[["appid", "name"]].copy()
df_out["cluster"] = df_enriquecido["cluster"]
df_out.to_csv(f"{DATA_DIR}/juegos_cluster_{timestamp}.csv", index=False)
df_out.to_csv(f"{DATA_DIR}/juegos_cluster_latest.csv", index=False)

# Guardar resultados de silhouette
pd.DataFrame(scores, columns=["k", "silhouette_score"]).to_csv(f"{DATA_DIR}/kmeans_silhouette_scores_latest.csv", index=False)

mlflow.log_artifact(f"{DATA_DIR}/juegos_cluster_latest.csv")
mlflow.log_artifact(f"{DATA_DIR}/kmeans_silhouette_scores_latest.csv")
mlflow.log_artifact(f"{MODEL_DIR}/faiss_index_latest.index")

print(f"Resultados guardados con timestamp {timestamp}.")
