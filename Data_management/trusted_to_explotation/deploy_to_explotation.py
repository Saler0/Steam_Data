# -*- coding: utf-8 -*-
"""
Este script realiza el proceso de ETL (Extract, Transform, Load) desde la
Trusted Zone a la Exploitation Zone dentro de la base de datos PostgreSQL.

Su función es leer los datos consolidados de la tabla 'trusted_zone' y 
copiarlos a la tabla 'exploitation_zone', reemplazándola por completo para 
asegurar que los datos de explotación estén siempre sincronizados.
"""

import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "deploy_to_explotation.log")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ==============================================================================
# CONFIGURACIÓN DE BASE DE DATOS
# ==============================================================================

# Configuración de la base de datos PostgreSQL desde variables de entorno
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "steam_data_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SOURCE_TABLE = 'trusted_zone'
DESTINATION_TABLE = 'exploitation_zone'

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """
    Orquesta la extracción desde la tabla trusted y la carga en la tabla de explotación.
    """
    logging.info(f"Iniciando despliegue de '{SOURCE_TABLE}' a '{DESTINATION_TABLE}'...")

    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            
            # 1. Verificar si la tabla de origen existe
            query = text(f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{SOURCE_TABLE}');")
            source_table_exists = conn.execute(query).scalar()

            if not source_table_exists:
                logging.warning(f"[AVISO] La tabla de origen '{SOURCE_TABLE}' no existe. No hay datos para desplegar.")
                return

            # 2. Extraer todos los datos de la tabla de origen
            logging.info(f"Extrayendo datos de '{SOURCE_TABLE}'...")
            df_trusted = pd.read_sql(f'SELECT * FROM {SOURCE_TABLE}', conn)

            if df_trusted.empty:
                logging.info("La tabla de origen está vacía. No hay nada que desplegar.")
                return
            
            logging.info(f"Se extrajeron {len(df_trusted)} filas.")

            # 3. Cargar los datos en la tabla de destino, reemplazándola si existe
            logging.info(f"Cargando datos en '{DESTINATION_TABLE}'. Se reemplazará si ya existe.")
            df_trusted.to_sql(
                DESTINATION_TABLE, 
                conn, 
                if_exists='replace', # <--- Clave: reemplaza la tabla por completo
                index=False
            )
            # Se añade un commit explícito para asegurar que la transacción se guarde permanentemente.
            conn.commit()
            logging.info(f"Se cargaron {len(df_trusted)} filas exitosamente en '{DESTINATION_TABLE}'.")

    except Exception as e:
        logging.error(f"[ERROR] Ocurrió un error durante el proceso de ETL a la zona de explotación: {e}")
        return

    logging.info(f"✅ Proceso de despliegue a la Exploitation Zone completado.")

if __name__ == '__main__':
    main()