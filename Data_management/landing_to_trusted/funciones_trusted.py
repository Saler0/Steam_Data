import re
import os
import json
import logging
from html import unescape
from functools import reduce
from pyspark.sql.functions import col, from_unixtime, udf, struct, collect_list, concat_ws, lit
from db.mongodb import MongoDBClient
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, ArrayType, MapType
)
import asyncio
import glob
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import requests
from langdetect import detect, DetectorFactory
import time
from datetime import date


# ========== FUNCIONES GLOBALES COMPATIBLES CON SPARK ==========

def clean_text(text):
    if not text:
        return ""
    text_no_urls = re.sub(r'https?://\S+', '', text)
    text_no_html = re.sub(r'<.*?>', '', text_no_urls)
    text_clean = unescape(text_no_html)
    return text_clean.strip()

def call_google_translate(text: str) -> str:
    """Llama al endpoint público y devuelve la traducción."""
    time.sleep(1.5) # para evitar saturar la API
    url = "https://translate.googleapis.com/translate_a/single"
    params = {
        "client": "gtx",
        "sl": "auto",
        "tl": "en",
        "dt": "t",
        "q": text
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return "".join(seg[0] for seg in data[0])

def translate_to_english(text: str, appid: int) -> str:
    """
    - Si el texto es corto (< MIN_LEN_TO_DETECT), traduce siempre.
    - Si es largo, detecta idioma; sólo traduce si no es inglés.
    """
    cleaned = clean_text(text)
    if not cleaned:
        return ""

    # 1) Textos muy cortos: traducir directamente
    if len(cleaned) < 50:
        return call_google_translate(cleaned)

    # 2) Para el resto, detectar idioma
    try:
        lang = detect(cleaned)
    except Exception:
        lang = "auto"

    # 3) Si ya es inglés (o detect falló), devolvemos el texto limpio
    if lang == "en":
        return cleaned

    # 4) Si no es inglés, traducimos
    try:
        return call_google_translate(cleaned)
    except Exception as e:
        logging.error(f"Error traduciendo appid={appid}")
        
        return cleaned

def clean_and_translate(text,appid):
    cleaned = clean_text(text)
    if not cleaned or not isinstance(cleaned, str) or cleaned.strip() == "":
        return ""
    return translate_to_english(cleaned,appid)

def standardize_age_rating(ratings_dict):
    rating_map = {
        "esrb": {"e": "3+", "e10+": "10+", "t": "13+", "m": "17+", "ao": "18+", "rp": None},
        "pegi": {"3": "3+", "7": "7+", "12": "12+", "16": "16+", "18": "18+"},
        "usk": {"0": "0+", "6": "6+", "12": "12+", "16": "16+", "18": "18+"},
        "cero": {"a": "3+", "b": "12+", "c": "15+", "d": "17+", "z": "18+"},
    }
    for system, values in rating_map.items():
        rating = (ratings_dict.get(system) or {}).get("rating", "").lower()
        if rating in values:
            return values[rating]
    return None

def validate_constraints(game):
    try:
        age = int(game.get("required_age", 0))
        game["required_age"] = str(age) if age >= 0 else "0"
    except:
        game["required_age"] = "0"

    price_info = game.get("price_overview", {})
    if isinstance(price_info, dict):
        final_price = price_info.get("final")
        try:
            final_price = int(final_price)
            if final_price < 0:
                price_info["final"] = None
        except:
            price_info["final"] = None

    return game

def clean_game_json(game_json):
    details = game_json.get("details") or {}

    if not isinstance(details, dict):
        try:
            details = details.asDict()
        except:
            details = {}

    age_systems = details.get("age_ratings", {})  
    age_rating = standardize_age_rating(age_systems)

    raw_date = (details.get("release_date") or {}).get("date", "")
    try:
        cleaned_release_date = date.strptime(raw_date, "%b %d, %Y").strftime("%Y-%m-%d")
    except:
        cleaned_release_date = raw_date

    def parse_requirements(key):
        raw = details.get(key)
        if isinstance(raw, dict):
            return (
                clean_text(raw.get("minimum", "")),
                clean_text(raw.get("recommended", ""))
            )
        else:
            s = clean_text(str(raw or ""))
            return s, ""

    pc_min, pc_rec = parse_requirements("pc_requirements")
    mac_min, mac_rec = parse_requirements("mac_requirements")
    li_min, li_rec = parse_requirements("linux_requirements")
    mc = details.get("metacritic") or {}

    # normalizar dlc -> lista[int], y campos derivados
    dlc_raw = details.get("dlc") or []
    if isinstance(dlc_raw, list):
        dlc_ids = []
        for x in dlc_raw:
            # aceptar int o string numérica
            try:
                dlc_ids.append(int(str(x).strip()))
            except Exception:
                continue
    else:
        dlc_ids = []

    # juego base para los type: "dlc"
    fullgame_obj = details.get("fullgame") or {}
    fullgame_parsed = None
    if isinstance(fullgame_obj, dict):
        fg_appid = fullgame_obj.get("appid")
        try:
            fg_appid = int(str(fg_appid)) if fg_appid is not None else None
        except Exception:
            fg_appid = None
        fullgame_parsed = {
            "appid": fg_appid,
            "name": clean_text(fullgame_obj.get("name", "")),
        } if (fg_appid is not None or fullgame_obj.get("name")) else None

    game = {
        "type": clean_text(details.get("type", "")),
        "name": clean_text(details.get("name", "")),
        "appid": game_json.get("appid"),
        "required_age": details.get("required_age", ""),
        "is_free": details.get("is_free", False),
        "controller_support": clean_text(details.get("controller_support", "")),
        "detailed_description": clean_and_translate(details.get("detailed_description", ""),game_json.get("appid")),
        "about_the_game": clean_and_translate(details.get("about_the_game", ""), game_json.get("appid")),
        "short_description": clean_and_translate(details.get("short_description", ""), game_json.get("appid")),
        "supported_languages": clean_text(details.get("supported_languages", "")),
        "legal_notice": clean_text(details.get("legal_notice", "")),
        "pc_requirements": {"minimum": pc_min, "recommended": pc_rec},
        "mac_requirements": {"minimum": mac_min, "recommended": mac_rec},
        "linux_requirements": {"minimum": li_min, "recommended": li_rec},
        "developers": details.get("developers", []),
        "publishers": details.get("publishers", []),
        "price_overview": details.get("price_overview", {}),
        "platforms": details.get("platforms", {}),
        "categories": [c.get("description", "") for c in (details.get("categories") or [])],
        "genres": [g.get("description", "") for g in (details.get("genres") or [])],
        "release_date": cleaned_release_date,
        "recommendations_total": (details.get("recommendations") or {}).get("total", 0),
        "metacritic_score": mc.get("score"),
        "age_rating": age_rating,
        "dlc": dlc_ids,
        "fullgame": fullgame_parsed,
    }

    return validate_constraints(game)

clean_and_translate_udf = udf(clean_and_translate, StringType())

class PipelineLandingToTrusted:
    def __init__(self, spark, mongo_client: MongoDBClient):
        self.spark = spark
        self.mongo     = mongo_client
        self.mongo_uri = mongo_client.uri
        self.mongo_db  = mongo_client.db_name
        self.client = self.mongo.client
        self.db = self.mongo.db

    def load_ndjson_files(self, folder_path, prefix):
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
                 if f.endswith(".ndjson") and f.startswith(prefix)]
        if not files:
            return None
        return self.spark.read.json(files)

    def run_steam_games(self):
        fecha_actual = date.today()
        fecha_formateada = fecha_actual.strftime("%Y_%m_%d")
        path = f"landing_zone/api_steam/steam_games_{fecha_formateada}.ndjson"
        if not os.path.exists(path):
            logging.error("No se encontró steam_games.ndjson")
            return

        df = self.spark.read.json(path)

        schema = StructType([
            StructField("type", StringType(), True),
            StructField("name", StringType(), True),
            StructField("appid", IntegerType(), True),
            StructField("required_age", StringType(), True),
            StructField("is_free", BooleanType(), True),
            StructField("controller_support", StringType(), True),
            StructField("detailed_description", StringType(), True),
            StructField("about_the_game", StringType(), True),
            StructField("short_description", StringType(), True),
            StructField("supported_languages", StringType(), True),
            StructField("legal_notice", StringType(), True),
            StructField("pc_requirements", StructType([
                StructField("minimum", StringType(), True),
                StructField("recommended", StringType(), True),
            ]), True),
            StructField("mac_requirements", StructType([
                StructField("minimum", StringType(), True),
                StructField("recommended", StringType(), True),
            ]), True),
            StructField("linux_requirements", StructType([
                StructField("minimum", StringType(), True),
                StructField("recommended", StringType(), True),
            ]), True),
            StructField("developers", ArrayType(StringType()), True),
            StructField("publishers", ArrayType(StringType()), True),
            StructField("price_overview", MapType(StringType(), StringType()), True),
            StructField("platforms", MapType(StringType(), BooleanType()), True),
            StructField("categories", ArrayType(StringType()), True),
            StructField("genres", ArrayType(StringType()), True),
            StructField("release_date", StringType(), True),
            StructField("recommendations_total", IntegerType(), True),
            StructField("metacritic_score", IntegerType(), True),
            StructField("age_rating", StringType(), True),
            StructField("dlc", ArrayType(IntegerType()), True),
            StructField("fullgame", StructType([
                StructField("appid", IntegerType(), True),
                StructField("name",  StringType(),  True),
            ]), True),
        ])

        clean_rdd = df.rdd.map(lambda row: clean_game_json(row.asDict(recursive=True)))
        df_clean = self.spark.createDataFrame(clean_rdd, schema)
        df_clean = df_clean.withColumn("updated_at", lit(fecha_formateada.replace("_", "-")))  # "YYYY-MM-DD"

        # Obtén la lista de appid ya en trusted_zone
        existing_appids = self.mongo.juegos.distinct("appid")
        if existing_appids:
            # Creamos un DataFrame con una única columna llamada "appid"
            existing_df = (
                self.spark
                    .createDataFrame(
                        [(aid,) for aid in existing_appids],  # datos
                        ["appid"]                              # nombres de las columnas
                    )
            )
            df_to_insert = df_clean.join(existing_df, on="appid", how="left_anti")
        else:
            df_to_insert = df_clean


        count_to_insert = df_to_insert.count()
        logging.info(f"🚀 Juegos realmente nuevos a insertar: {count_to_insert}")

        if count_to_insert:
            df_to_insert.write \
                .format("mongo") \
                .mode("append") \
                .option("uri", self.mongo_uri) \
                .option("database", self.mongo_db) \
                .option("collection", self.mongo.juegos.name) \
                .save()
            logging.info("✅ Inserción distribuida completada sin duplicados.")
        else:
            logging.info("ℹ️ No hay juegos nuevos para insertar.")

    def run_youtube_comments(self):
        df = self.load_ndjson_files("landing_zone/api_youtube/", "comments_")
        if df is None:
            logging.warning("No se encontraron comentarios.")
            return

        def extract_comment(data, clean_fn):
            snippet = data.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
            return {
                "videoId": snippet.get("videoId"),
                "author": snippet.get("authorDisplayName"),
                "text": snippet.get("textOriginal"),
                "text_clean": clean_fn(snippet.get("textOriginal", "")),
                "publishedAt": snippet.get("publishedAt", "")[:10],
                "likeCount": snippet.get("likeCount", 0)
            }

        records = df.toJSON().map(lambda r: extract_comment(json.loads(r), clean_and_translate)).collect()
        if records:
            self.mongo.comentarios_youtube.insert_many(records)
            logging.info(f"{len(records)} comentarios de YouTube insertados en MongoDB.")

    def run_reviews(self):
        logging.info("Iniciando carga de reseñas desde landing_zone/api_steam/")
        df = self.load_ndjson_files("landing_zone/api_steam/", "reviews_")
        if df is None:
            logging.warning("No se encontraron archivos de reseñas.")
            return

        # Contar, limpiar y traducir
        count_raw = df.count()
        logging.info(f"Reseñas leídas: {count_raw}")
        df = df.withColumn("review_clean", clean_and_translate_udf(col("review"))) \
               .withColumn("timestamp_created", from_unixtime(col("timestamp_created")).cast("date")) \
               .withColumn("timestamp_updated", from_unixtime(col("timestamp_updated")).cast("date")) \
               .cache()
        count_trans = df.count()
        logging.info(f"Reseñas tras limpiar y traducir: {count_trans}")

        # Quito duplicados en batch
        df_unique = df.dropDuplicates(["recommendationid"])
        count_unique = df_unique.count()
        logging.info(f"Reseñas únicas a insertar: {count_unique}")

        # ¿Ya hay algo en Mongo?
        coll = self.db[self.mongo.reviews.name]
        total_in_mongo = coll.estimated_document_count()

        if total_in_mongo > 0:
            # obtengo IDs existentes con PyMongo y los llevo a Spark
            existing_list = coll.distinct("recommendationid")
            existing_df = (
                self.spark
                    .createDataFrame([(rid,) for rid in existing_list], StringType())
                    .toDF("recommendationid")
            )
            # left_anti join para quedarme solo con los nuevos
            new_reviews = df_unique.join(existing_df,
                                        on="recommendationid",
                                        how="left_anti")
        else:
            # si está vacío, todas las filas son nuevas
            new_reviews = df_unique

        count_new = new_reviews.count()
        logging.info(f"Reseñas nuevas a insertar: {count_new}")

        if count_new:
            new_reviews.coalesce(10) \
                .write \
                .format("mongo") \
                .mode("append") \
                .option("uri",      self.mongo_uri) \
                .option("database", self.mongo_db) \
                .option("collection", self.mongo.reviews.name) \
                .save()
            logging.info("Inserción completada.")
        else:
            logging.info("No hay reseñas nuevas para insertar.")



    def run_youtube_transcripts(self):
        folder = "landing_zone/api_youtube/"
        files = glob.glob(os.path.join(folder, "transcript_*.ndjson"))

        if not files:
            logging.warning("No se encontraron transcripciones.")
            return

        dfs = []
        for f in files:
            try:
                df_file = self.spark.read.json(f)
                if df_file.rdd.isEmpty():
                    logging.warning(f"Archivo vacío: {f}")
                    continue

                video_id = os.path.basename(f).replace("transcript_", "").replace(".ndjson", "")
                df_file = df_file.withColumn("video_id", lit(video_id))
                dfs.append(df_file)
            except Exception as e:
                logging.warning(f"Error al procesar {f}: {e}")

        if not dfs:
            logging.warning("No se pudo cargar ningún transcript válido.")
            return

        df = dfs[0] if len(dfs) == 1 else reduce(lambda a, b: a.unionByName(b), dfs)

        df_grouped = df.groupBy("video_id") \
                    .agg(concat_ws(" ", collect_list("text")).alias("transcription"))

        updates = df_grouped.toJSON().map(lambda r: json.loads(r)).collect()

        for record in updates:
            video_id = record["video_id"]
            transcription = record["transcription"]
            self.mongo.videos_youtube.update_many(
                {"videoId": video_id},
                {"$set": {"transcription": transcription}},
                upsert=True
            )

        logging.info("Transcripciones procesadas e insertadas correctamente.")


    def run(self):
        logging.info("========== INICIO DE PIPELINE ==========")
        logging.info("===== INICIO DE PIPELINE DE LIMPIEZA Y TRANSFORMACIÓN =====")
        self.run_steam_games()
        # self.run_reviews()
        # self.run_youtube_comments()
        # self.run_youtube_transcripts()

    def stop(self):
        self.spark.stop()
