import os
import joblib
import numpy as np
import pandas as pd
import datetime
import yaml
import faiss
import mlflow

from features import GameFeatureExtractor

# === Cargar parámetros ===
with open("params.yaml") as f:
    params = yaml.safe_load(f)
    umbral = params["similitud"]["umbral"]

# === MLflow Tracking ===
mlflow.set_experiment("similitud_cliente_faiss")

with mlflow.start_run():
    mlflow.log_param("umbral_similitud", umbral)

    # === Paths ===
    MODEL_KMEANS = "models/clustering/kmeans_latest.pkl"
    GENRES_ENCODER = "models/encoders/mlb_genres.pkl"
    CATEGORIES_ENCODER = "models/encoders/mlb_categories.pkl"
    EMBEDDINGS_PATH = "data/processed/embeddings_latest.npy"
    METADATA_PATH = "data/processed/juegos_cluster_latest.csv"
    INDEX_FAISS = "models/clustering/faiss_index_latest.index"

    # === 1. Cargar modelos y datos ===
    print("Cargando modelos y datos...")
    kmeans = joblib.load(MODEL_KMEANS)
    mlb_genres = joblib.load(GENRES_ENCODER)
    mlb_categories = joblib.load(CATEGORIES_ENCODER)
    X_all = np.load(EMBEDDINGS_PATH)
    df_metadata = pd.read_csv(METADATA_PATH)
    index = faiss.read_index(INDEX_FAISS)

    # === 2. Crear extractor con binarizadores existentes ===
    extractor = GameFeatureExtractor(
        mode="clustering",
        genres_vocab=mlb_genres.classes_,
        categories_vocab=mlb_categories.classes_
    )
    extractor.mlb_genres = mlb_genres
    extractor.mlb_categories = mlb_categories

    # === 3. Input del cliente ===

    # Esto es un ejemplo generado por IA, cuando se haya montado la plataforma web se desarrollará este fragmento.
    nuevo_juego = {
        "genres": ["Action", "Adventure"],
        "categories": ["Singleplayer", "Open World"],
        "required_age": 16,
        "detailed_description": "Eres un caballero que debe enfrentarse a un dragón legendario en un mundo abierto lleno de secretos.",
        "about_the_game": "Un RPG épico con exploración, combate táctico y decisiones morales.",
        "short_description": "Aventura épica de mundo abierto con dragones."
    }
    input_name = "Juego del Cliente"
    input_genres = nuevo_juego["genres"]
    input_categories = nuevo_juego["categories"]
    input_description = (
        nuevo_juego.get("detailed_description", "") + " " +
        nuevo_juego.get("about_the_game", "") + " " +
        nuevo_juego.get("short_description", "")
    ).strip()

    # === 4. Vectorizar el juego nuevo y normalizar ===
    print("Generando embedding del juego nuevo...")
    X_nuevo = extractor.transform_new_game(nuevo_juego)
    faiss.normalize_L2(X_nuevo)

    # === 5. Buscar vecinos con FAISS ===
    print("Buscando vecinos similares con FAISS...")
    distances, indices = index.search(X_nuevo, len(X_all))
    indices_validos = np.where(1 - distances[0] >= umbral)[0]

    # === 6. Recoger resultados
    rows = []
    for i in indices_validos:
        idx_global = indices[0][i]
        sim = 1 - distances[0][i]
        juego = df_metadata.iloc[idx_global]
        rows.append({
            "input_appid": None,
            "input_name": input_name,
            "input_description": input_description,
            "input_genres": "; ".join(input_genres),
            "input_categories": "; ".join(input_categories),
            "similar_appid": juego["appid"],
            "similar_name": juego["name"],
            "cluster": juego["cluster"],
            "similarity": round(sim, 4)
        })

    # === 7. Guardar resultados ===
    if rows:
        df_similares = pd.DataFrame(rows)
        fecha = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        output_dir = "reports/similarity"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/similar_games_{fecha}.csv"
        df_similares.to_csv(output_path, index=False)
        mlflow.log_metric("num_similares", len(df_similares))
        mlflow.log_artifact(output_path)
        print(f"Resultados guardados en: {output_path}")
    else:
        print("No se encontraron juegos suficientemente similares.")
        mlflow.log_metric("num_similares", 0)
