import logging
import os
from data_ingestion.api_steam import ApiSteam
from data_ingestion.api_youtube import ApiYoutube
from landing_to_trusted.funciones_trusted import PipelineLandingToTrusted
from trusted_to_explotation.explotation_zone import TrustedToExploitation
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from db.mongodb import MongoDBClient
import sys
sys.stdout.reconfigure(encoding='utf-8')

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
    def __init__(self, trusted_client):
        self.trusted_client = trusted_client
        self.appi_key_youtube = os.getenv("YOUTUBE_API_KEY")
        # POR LIMITACIONES DE RECURSOS SE OBTENDRA REVIEWS SOLO ALGUNOS APPIDS QUE CAEN EN EL MISMA CLASIFICACION SEGUN EL CLUSTER
        # EN UN ESCENARIO RELISTA SE CAPTURARIAN TODOS LOS REVIEWS DE TODOS LOS APPIDS REGISTRADOS.
        self.modo='MVP'
        if self.modo == 'MVP': # se capturara todas las reviews de un grupo selecto de APPIDs
            self.appids_to_process_reviews=[]
        else: # Modo Realista
            self.appids_to_process_reviews = None

            

    def run(self):
        logging.info(f"Iniciando ingesta de juegos y reviews en ApiSteam…")
        steam = ApiSteam(self.trusted_client,self.appids_to_process_reviews)
        nombre_juegos = steam.run()


        # logging.info(f"Iniciando ingesta YouTube para {len(nombre_juegos)} juegos…")
        # youtube = ApiYoutube(nombre_juegos, self.appi_key_youtube)
        # youtube.run()

        # PARA EL WEBSCRAPING DE STEAM_BASE SE DEBE USAR COMO INPUT NDJONS DE LA LANDING ZONE
        # SE DEBE LLAMAR AL ARCHIVO steambase_appname.py (donde comienza todo)

class PipelineTustedExplotationZone:
    def __init__(self, trusted_client: MongoDBClient, explo_client: MongoDBClient):
        self.trusted_client = trusted_client
        self.explo_client = explo_client


    def run(self):
        logging.info(f"Comienza la extraccion de trusted_zone para mover a explotation_zone")
        spark = (
            SparkSession.builder
            .appName("TrustedToExploitation") \
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .config("spark.sql.shuffle.partitions", "128") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.memory.fraction", "0.6") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "1g") \
            .getOrCreate()
        )

        job = TrustedToExploitation(spark, self.trusted_client, self.explo_client)
        job.run()
        
        spark.stop()



def main():


    setup_logging()

    mongo_uri = "mongodb://host.docker.internal:27017"
    mongo_db_trusted = "trusted_zone"
    mongo_db_explotation = "explotation_zone"
    trusted_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_trusted)

    logging.info("========== INICIO DE PIPELINE ==========")

    # # ===== INGESTA DE DATOS  --> LANDING ZONE =====
    logging.info("===== INICIO DE PIPELINE DE INGESTA DE DATOS =====")
    
    pipelineI = PipelineIngest(trusted_client)
    pipelineI.run()
    logging.info("✅ INGESTA ➜ LANDING ")

    # ===== LANDING ZONE --> TRUSTED ZONE =====
    logging.info("===== INICIO DE PIPELINE DE LANDING ZONE A TRUSTED ZONE =====")



    spark = (
        SparkSession.builder
            .appName("TrustedZone")
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .config("spark.mongodb.output.uri", f"{mongo_uri}/{mongo_db_trusted}.juegos_steam")
            .getOrCreate()
    )

    pipelineLT = PipelineLandingToTrusted(spark,trusted_client)
    pipelineLT.run()
    pipelineLT.stop()
    logging.info("✅ LANDING ➜ TRUSTED ")

    # ===== TRUSTED ZONE --> EXPLOTATION ZONE =====
    logging.info("===== INICIO DE PIPELINE DE TRUSTED ZONE A EXPLOTATION ZONE =====")

    # Creamos un cliente PARA CADA ZONA:
    trusted_client    = MongoDBClient(uri=mongo_uri, db_name=mongo_db_trusted)
    exploitation_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_explotation)

    pipelineTE = PipelineTustedExplotationZone(trusted_client,exploitation_client)
    pipelineTE.run()
    logging.info("✅ TRUSTED ➜ EXPLOTATION completado")
    
    logging.info("✅ PIPELINE COMPLETO MANAGEMENT ")

if __name__ == "__main__":
    main()
