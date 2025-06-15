import logging
import os
from data_ingestion.api_steam import ApiSteam
from data_ingestion.api_youtube import ApiYoutube
from funciones_trusted import PipelineLandingToTrusted
from pyspark.sql import SparkSession
from dotenv import load_dotenv


# Carga las variables de entorno del archivo .env
load_dotenv()

def setup_logging(log_dir="logs", log_file="pipeline.log"):
    os.makedirs(log_dir, exist_ok=True)
    full_path = os.path.join(log_dir, log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(full_path, mode="a", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

class PipelineIngest:
    def __init__(self, appids):
        self.appids = appids
        self.appi_key_youtube = os.getenv("YOUTUBE_API_KEY")

    def run(self):
        logging.info(f"Iniciando ingesta de {len(self.appids)} juegos en ApiSteam…")
        steam = ApiSteam(self.appids)
        nombre_juegos = steam.run()

        logging.info(f"Iniciando ingesta YouTube para {len(nombre_juegos)} juegos…")
        youtube = ApiYoutube(nombre_juegos, self.appi_key_youtube)
        youtube.run()

def main():
    # Lista de APPIDs a procesar
    appids_to_process = [
        570940,
        374320,
        335300,
        1245620,
        1627720,
        1903340,
        814380,
        2680010,
        1501750,
        236430,
    ]

    setup_logging()

    logging.info("========== INICIO DE PIPELINE ==========")

    # # ===== INGESTA DE DATOS  --> LANDING ZONE =====
    # logging.info("===== INICIO DE PIPELINE DE INGESTA =====")
    # pipelineI = PipelineIngest(appids_to_process)
    # pipelineI.run()

    # ===== LANDING ZONE --> TRUSTED ZONE =====
    logging.info("===== INICIO DE PIPELINE DE LIMPIEZA Y TRANSFORMACIÓN =====")
    spark = SparkSession.builder.appName("TrustedZone").getOrCreate()
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db = os.getenv("MONGO_DB")

    pipelineLT = PipelineLandingToTrusted(spark, mongo_uri, mongo_db)
    pipelineLT.run()
    pipelineLT.stop()
    logging.info("===== PIPELINE DE LIMPIEZA COMPLETADO =====")

    # ===== TRUSTED ZONE --> EXPLOTATION ZONE =====
    # logging.info("===== INICIO DE PIPELINE DE EXPLOTACIÓN =====")
    # pipelineTE = PipelineTustedExplotationZone()
    # pipelineTE.run()

    logging.info("========== PIPELINE COMPLETO ==========")

if __name__ == "__main__":
    main()
