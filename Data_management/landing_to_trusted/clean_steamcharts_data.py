# -*- coding: utf-8 -*-
"""
Este script limpia los datos de Steamcharts que han sido recopilados por el scraper.
Lee el archivo CSV de la landing_zone, realiza transformaciones y limpiezas,
y guarda el resultado en una base de datos PostgreSQL.

La lógica es incremental: solo se añaden los registros (appid-mes) que no existen 
previamente en la base de datos.
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
LOG_FILE = os.path.join(LOG_DIR, "cleaning_to_trusted.log")
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
# CONFIGURACIÓN DE RUTAS Y BASE DE DATOS
# ==============================================================================

BASE_DIR = os.path.join(os.path.dirname(__file__), '..')
LANDING_CSV_PATH = os.path.join(BASE_DIR, 'landing_zone', 'steamchart', 'steamcharts_data.csv')
TABLE_NAME = 'trusted_zone'

# Configuración de la base de datos PostgreSQL desde variables de entorno
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "steam_data_db")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# ==============================================================================
# FUNCIONES DE LIMPIEZA
# ==============================================================================

def clean_data_pandas(df):
    """Realiza la limpieza de datos en el DataFrame usando pandas."""
    logging.info("Iniciando limpieza de datos con pandas...")
    df_cleaned = df.copy()
    
    df_cleaned['avg_players'] = pd.to_numeric(df_cleaned['avg_players'], errors='coerce').fillna(0)
    df_cleaned['month_date'] = pd.to_datetime(df_cleaned['month_date'])
    df_cleaned['appid'] = df_cleaned['appid'].astype('int32')
    df_cleaned['name'] = df_cleaned['name'].astype('str')
    df_cleaned['avg_players'] = df_cleaned['avg_players'].astype('int32')
    
    logging.info(f"Limpieza completada. El DataFrame tiene {len(df_cleaned)} filas.")
    return df_cleaned

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main():
    """Orquesta la lectura, limpieza y guardado incremental en PostgreSQL."""
    if not os.path.exists(LANDING_CSV_PATH):
        logging.critical(f"[CRITICAL] El archivo de entrada no se encontró en: {LANDING_CSV_PATH}")
        return

    logging.info(f"Leyendo datos crudos desde {LANDING_CSV_PATH}")
    df_new_raw = pd.read_csv(LANDING_CSV_PATH)

    required_columns = ['appid', 'name', 'month_date', 'avg_players']
    if not all(col in df_new_raw.columns for col in required_columns):
        logging.critical(f"[CRITICAL] El CSV no contiene las columnas requeridas: {required_columns}")
        return
    
    df_new_raw = df_new_raw[required_columns]
    df_new_cleaned = clean_data_pandas(df_new_raw)

    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            # --- Lógica Incremental ---
            logging.info(f"Conectado a PostgreSQL. Verificando la tabla '{TABLE_NAME}'...")
            
            # Comprobar si la tabla ya existe
            query = text(f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{TABLE_NAME}');")
            table_exists = conn.execute(query).scalar()

            if table_exists:
                logging.info(f"Tabla '{TABLE_NAME}' existente. Leyendo datos para comparación.")
                df_existing = pd.read_sql(f'SELECT appid, month_date FROM {TABLE_NAME}', conn)
                df_existing['month_date'] = pd.to_datetime(df_existing['month_date'])
                
                df_merged = df_new_cleaned.merge(df_existing, on=['appid', 'month_date'], how='left', indicator=True)
                df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
            else:
                logging.info(f"La tabla '{TABLE_NAME}' no existe. Se insertarán todos los datos.")
                df_to_insert = df_new_cleaned

            if not df_to_insert.empty:
                logging.info(f"Se encontraron {len(df_to_insert)} filas nuevas para añadir a PostgreSQL.")
                df_to_insert.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
                logging.info(f"Guardado en PostgreSQL completado. Se añadieron {len(df_to_insert)} filas.")
            else:
                logging.info("No se encontraron filas nuevas para añadir. La base de datos ya está actualizada.")

    except Exception as e:
        logging.error(f"[ERROR] Ocurrió un error al conectar o escribir en PostgreSQL: {e}")
        return

    logging.info(f"✅ Proceso de limpieza y guardado en la Trusted Zone (PostgreSQL) completado.")

if __name__ == '__main__':
    main()