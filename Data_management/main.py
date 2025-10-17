# -*- coding: utf-8 -*-
import logging
import os
import argparse
import sys
from dotenv import load_dotenv
from data_ingestion.api_steam import ApiSteam
from data_ingestion.api_youtube import ApiYoutube
from landing_to_trusted.funciones_trusted import PipelineLandingToTrustedSteam
from trusted_to_exploitation.explotation_zone import TrustedToExploitation
from pyspark.sql import SparkSession
from db.mongodb import MongoDBClient
from data_ingestion.web_scraping_steambase.steamcharts_scraper import main as scraper_main
from landing_to_trusted.clean_steamcharts_data import main as cleaner_main
from trusted_to_exploitation.deploy_to_explotation import main as deployer_main

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
            self.appids_to_process =  [] # CS2, Dota 2, Baldur's Gate 3, GTA V, Stardew Valley
        else: # Modo Realista
            self.appids_to_process = None
            
            

    def run(self):
        logging.info(f"Iniciando ingesta de juegos, reviews y news en ApiSteam…")
        steam = ApiSteam(self.trusted_client,self.appids_to_process)
        nombre_juegos = steam.run()
        scraper_main(self.appids_to_process) # Historico



        # logging.info(f"Iniciando ingesta YouTube para {len(nombre_juegos)} juegos…")
        # youtube = ApiYoutube(nombre_juegos, self.appi_key_youtube)
        # youtube.run()

class PipelineLandingtoTrusted:

    def __init__(self, mongo_uri, mongo_db_trusted):
            self.mongo_uri = mongo_uri
            self.mongo_db_trusted = mongo_db_trusted

    def run(self):
        spark = (
        SparkSession.builder
            .appName("TrustedZone")
            .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
            .config("spark.mongodb.output.uri", f"{self.mongo_uri}/{self.mongo_db_trusted}.juegos_steam")
            .getOrCreate()
        )
        trusted_client = MongoDBClient(uri=self.mongo_uri, db_name=self.mongo_db_trusted)
        pipelineLT = PipelineLandingToTrustedSteam(spark,trusted_client)
        pipelineLT.run()
        pipelineLT.stop()

        # Historico
        cleaner_main()
        

class PipelineTustedExplotationZone:
    def __init__(self, trusted_client: MongoDBClient, explo_client: MongoDBClient):
        self.trusted_client = trusted_client
        self.explo_client = explo_client

    def run(self):

        logging.info(f"Comienza la extraccion de trusted_zone para mover a exploitation_zone")
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

        # Historico
        deployer_main()
        
def main():
    try:
        setup_logging(log_file="app_pipeline.log")

        mongo_uri = "mongodb://host.docker.internal:27017"
        mongo_db_trusted = "trusted_zone"
        mongo_db_exploitation = "exploitation_zone"
        trusted_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_trusted)

        logging.info("========== INICIO DE PIPELINE APP ==========")

        # # ===== INGESTA DE DATOS  --> LANDING ZONE =====
        # logging.info("===== INICIO DE PIPELINE DE INGESTA DE DATOS ====")
        # pipelineI = PipelineIngest(trusted_client)
        # pipelineI.run()
        # logging.info("✅ INGESTA ➜ LANDING ")

        # ===== LANDING ZONE --> TRUSTED ZONE =====
        logging.info("===== INICIO DE PIPELINE DE LANDING ZONE A TRUSTED ZONE ====")
        pipelineLT = PipelineLandingtoTrusted(mongo_uri,mongo_db_trusted)
        pipelineLT.run()
        logging.info("✅ LANDING ➜ TRUSTED ")

        # ===== TRUSTED ZONE --> EXPLOITATION ZONE =====
        logging.info("===== INICIO DE PIPELINE DE TRUSTED ZONE A EXPLOTATION ZONE =====")

        # Creamos un cliente PARA CADA ZONA:
        trusted_client    = MongoDBClient(uri=mongo_uri, db_name=mongo_db_trusted)
        exploitation_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_exploitation)
        
        pipelineTE = PipelineTustedExplotationZone(trusted_client,exploitation_client)
        pipelineTE.run()
        logging.info("✅ TRUSTED ➜ EXPLOTATION completado")
        
        logging.info("✅ PIPELINE APP COMPLETO ✅")
        
    except Exception:
        logging.exception("💥 Pipeline abortado por excepción no capturada")
        sys.exit(1)
if __name__ == "__main__":
    main()