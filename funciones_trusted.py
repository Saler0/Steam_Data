import re
import os
import json
import logging
from html import unescape
from datetime import datetime
from googletrans import Translator
from pyspark.sql.functions import col, from_unixtime, udf, struct
from pyspark.sql.types import StringType
from db.mongodb import MongoDBClient

# ========== FUNCIONES GLOBALES COMPATIBLES CON SPARK ==========

translator = Translator()

def clean_text(text):
    if not text:
        return ""
    text_no_urls = re.sub(r'https?://\S+', '', text)
    text_no_html = re.sub(r'<.*?>', '', text_no_urls)
    text_clean = unescape(text_no_html)
    return text_clean.strip()

def correct_spelling(text):
    return text  # placeholder

def translate_to_english(text):
    if not text or not isinstance(text, str):
        return ""
    try:
        translated = translator.translate(text, dest="en")
        return translated.text
    except Exception as e:
        logging.warning(f"Error translating: {e}")
        return text

def clean_and_translate(text):
    cleaned = clean_text(text)
    corrected = correct_spelling(cleaned)
    return translate_to_english(corrected)

clean_and_translate_udf = udf(clean_and_translate, StringType())

# =============================================================

class PipelineLandingToTrusted:
    def __init__(self, spark, mongo_uri, db_name):
        self.spark = spark
        self.mongo = MongoDBClient(uri=mongo_uri, db_name=db_name)

    def load_ndjson_files(self, folder_path, prefix):
        files = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
                 if f.endswith(".ndjson") and f.startswith(prefix)]
        if not files:
            return None
        return self.spark.read.json(files)

    def run_reviews(self):
        df = self.load_ndjson_files("landing_zone/api_steam/", "reviews_")
        if df is None:
            logging.warning("No se encontraron reseñas.")
            return

        df = df.withColumn("review_clean", clean_and_translate_udf(col("review"))) \
               .withColumn("timestamp_created", from_unixtime(col("timestamp_created")).cast("date")) \
               .withColumn("timestamp_updated", from_unixtime(col("timestamp_updated")).cast("date"))

        df.dropDuplicates(["recommendationid"]) \
          .write \
          .format("mongo") \
          .mode("append") \
          .option("uri", self.mongo.uri) \
          .option("database", self.mongo.db_name) \
          .option("collection", self.mongo.reviews.name) \
          .save()

        logging.info("Reviews insertadas en MongoDB usando escritura distribuida.")

    def run_steam_games(self):
        path = "landing_zone/api_steam/steam_games.ndjson"
        if not os.path.exists(path):
            logging.warning("No se encontró steam_games.ndjson")
            return

        df = self.spark.read.json(path)

        def clean_game_json(game_json):
            details = game_json.get("details", {})
            cleaned = {}

            for field in ["name", "detailed_description", "about_the_game", "short_description", "supported_languages", "legal_notice"]:
                cleaned[field] = clean_text(details.get(field, ""))

            for platform in ["pc_requirements", "mac_requirements", "linux_requirements"]:
                cleaned[platform] = {}
                platform_data = details.get(platform, {})
                if isinstance(platform_data, dict):
                    for req_type in ["minimum", "recommended"]:
                        cleaned[platform][req_type] = clean_text(platform_data.get(req_type, ""))
                else:
                    cleaned[platform]["minimum"] = clean_text(str(platform_data))
                    cleaned[platform]["recommended"] = ""

            cleaned["appid"] = game_json.get("appid")
            cleaned["is_free"] = details.get("is_free", False)
            cleaned["required_age"] = details.get("required_age", "")
            cleaned["developers"] = details.get("developers", [])
            cleaned["publishers"] = details.get("publishers", [])
            cleaned["price_overview"] = details.get("price_overview", {})
            cleaned["platforms"] = details.get("platforms", {})
            cleaned["categories"] = [cat.get("description") for cat in details.get("categories", [])]
            cleaned["genres"] = [gen.get("description") for gen in details.get("genres", [])]

            release_date = details.get("release_date", {}).get("date", "")
            try:
                cleaned["release_date"] = datetime.strptime(release_date, "%b %d, %Y").strftime("%Y-%m-%d")
            except:
                cleaned["release_date"] = release_date

            cleaned["recommendations_total"] = details.get("recommendations", {}).get("total", 0)
            cleaned["metacritic_score"] = details.get("metacritic", {}).get("score")
            cleaned["age_rating"] = None  # normalización futura
            return cleaned

        df = df.rdd.map(lambda row: clean_game_json(row.asDict())).toDF()
        df.write \
          .format("mongo") \
          .mode("append") \
          .option("uri", self.mongo.uri) \
          .option("database", self.mongo.db_name) \
          .option("collection", self.mongo.juegos.name) \
          .save()

        logging.info("Juegos insertados en MongoDB usando escritura distribuida.")

    def run(self):
        """
        Ejecuta todo el pipeline de trusted zone.
        """
        logging.info("========== INICIO DE PIPELINE ==========")
        logging.info("===== INICIO DE PIPELINE DE LIMPIEZA Y TRANSFORMACIÓN =====")
        self.run_steam_games()
        self.run_reviews()

    def stop(self):
        """
        Detiene la sesión de Spark.
        """
        self.spark.stop()