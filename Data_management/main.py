import logging
import os
from data_ingestion.api_steam import ApiSteam
from data_ingestion.api_youtube import ApiYoutube
from landing_to_trusted.funciones_trusted import PipelineLandingToTrusted
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from db.mongodb import MongoDBClient


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

        # POR LIMITACIONES DE RECURSOS SE OBTENDRA REVIEWS SOLO ALGUNOS APPIDS QUE CAEN EN EL MISMA CLASIFICACION SEGUN EL CLUSTER
        # EN UN ESCENARIO RELISTA SE CAPTURARIAN TODOS LOS REVIEWS DE TODOS LOS APPIDS REGISTRADOS.
        

        # logging.info(f"Iniciando ingesta YouTube para {len(nombre_juegos)} juegos…")
        # youtube = ApiYoutube(nombre_juegos, self.appi_key_youtube)
        # youtube.run()

        # PARA EL WEBSCRAPING DE STEAM_BASE SE DEBE USAR COMO INPUT NDJONS DE LA LANDING ZONE
        # SE DEBE LLAMAR AL ARCHIVO steambase_appname.py (donde comienza todo)

class PipelineTustedExplotationZone:
    def __init__(self, trusted_client: MongoDBClient, explo_client: MongoDBClient):
        pass


    def run(self):
        logging.info(f"Comienza la extraccion de trusted_zone para mover a explotation_zone")




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

    mongo_uri = "mongodb://host.docker.internal:27017"
    mongo_db_trusted = "trusted_zone"
    mongo_db_explotation = "explotation_zone"

    logging.info("========== INICIO DE PIPELINE ==========")

    # # ===== INGESTA DE DATOS  --> LANDING ZONE =====
    logging.info("===== INICIO DE PIPELINE DE INGESTA DE DATOS =====")
    # pipelineI = PipelineIngest(appids_to_process)
    # pipelineI.run()
    logging.info("✅ DATOS EXTRAIDOS Y GUARDADOS EN LA LANDING ZONE")

    # ===== LANDING ZONE --> TRUSTED ZONE =====
    logging.info("===== INICIO DE PIPELINE DE LANDING ZONE A TRUSTED ZONE =====")

    trusted_client    = MongoDBClient(uri=mongo_uri, db_name="trusted_zone")

    spark = (
        SparkSession.builder
            .appName("TrustedZone")
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .config("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db_trusted}.juegos_steam")
            .getOrCreate()
    )

    # pipelineLT = PipelineLandingToTrusted(spark,trusted_client)
    # pipelineLT.run()
    # pipelineLT.stop()
    logging.info("✅ PIPELINE DE LANDING ZONE A TRUSTED ZONE COMPLETADO")

    # ===== TRUSTED ZONE --> EXPLOTATION ZONE =====
    logging.info("===== INICIO DE PIPELINE DE LANDING ZONE A TRUSTED ZONE A EXPLOTATION ZONE =====")

    # Creamos un cliente PARA CADA ZONA:
    trusted_client    = MongoDBClient(uri=mongo_uri, db_name=mongo_db_trusted)
    exploitation_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_explotation)

    pipelineTE = PipelineTustedExplotationZone(trusted_client,exploitation_client)
    pipelineTE.run()
    logging.info("✅ PIPELINE DE LANDING ZONE A TRUSTED ZONE COMPLETADO")
    
    logging.info("✅ PIPELINE COMPLETO ")

if __name__ == "__main__":
    main()
