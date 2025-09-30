"""
Este script recopila datos históricos de jugadores de juegos de Steam desde
Steamcharts.com. Recibe una lista de appids, extrae los datos de jugadores 
y los guarda en un único archivo CSV.
"""

import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from datetime import datetime
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ==============================================================================
# CONFIGURACIÓN DE LOGGING
# ==============================================================================

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "steamcharts_scraper.log")
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
# CONFIGURACIÓN DEL SCRIPT
# ==============================================================================

REQUEST_TIMEOUT = 10
SLEEP_INTERVAL = 2.0
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'landing_zone', 'steamchart')
OUTPUT_CSV_PATH = os.path.join(OUTPUT_DIR, 'steamcharts_data.csv')

# Cargar variables de entorno para la conexión a la BD
load_dotenv()

# ==============================================================================
# FUNCIONES DE SCRAPING Y PROCESAMIENTO
# ==============================================================================

def setup_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def scrape_steamcharts(session, appid):
    """
    Obtiene los datos de jugadores y el nombre del juego desde Steamcharts.
    """
    url = f"https://steamcharts.com/app/{appid}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        
        soup = BeautifulSoup(res.text, 'html.parser')
        
        game_name_tag = soup.find('h1', id='app-title')
        game_name = game_name_tag.text.strip() if game_name_tag else "Unknown"

        tables = pd.read_html(res.text)
        if not tables:
            logging.warning(f"[WARNING] No se encontraron tablas para el appid {appid}. Saltando.")
            return None

        df = tables[-1]
        df = df.dropna(axis=1, how='all')

        current_month_str = datetime.now().strftime("%B %Y")
        df['Month'] = df['Month'].replace('Last 30 Days', current_month_str)
        
        df['Month'] = pd.to_datetime(df['Month'], format='%B %Y').dt.strftime('%Y-%m-01')

        df['appid'] = appid
        df['name'] = game_name

        rename_map = {
            'Avg. Players': 'avg_players',
            'Gain': 'gain',
            '% Gain': 'gain_percent',
            'Peak Players': 'peak_players',
            'Month': 'month_date'
        }
        df = df.rename(columns=rename_map)

        return df

    except requests.RequestException as e:
        logging.error(f"[ERROR] Error al scrapear Steamcharts para appid {appid}: {e}")
        return None
    except (ValueError, IndexError) as e:
        logging.warning(f"[WARNING] No se encontró una tabla válida en Steamcharts para appid {appid}. Puede que no tenga datos. Error: {e}")
        return None

def get_game_details(appid, session):
    """
    Obtiene detalles de un juego desde la API de Steam usando la sesión proporcionada.
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=english"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        # Usamos la sesión que ya tiene reintentos para la API de Steam también.
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        raw = res.json()
        section = raw.get(str(appid), {})
        if not section.get("success", False):
            logging.warning(f"La API de Steam no devolvió 'success' para el appid {appid}.")
            return None
        return section.get("data")
    except requests.RequestException as e:
        logging.error(f"Error al contactar la API de Steam para appid {appid}: {e}")
        return None

def is_game_unreleased(appid, session):
    """
    Verifica si un juego aún no ha sido lanzado o no tiene datos de jugadores.
    Devuelve True si se debe omitir el scraping, False en caso contrario.
    """
    details = get_game_details(appid, session)
    
    if not details:
        logging.warning(f"No se pudieron obtener detalles de Steam para {appid}. Se omitirá por precaución.")
        return True

    # 1. Es un video, dlc, etc.? Steamcharts es para juegos.
    game_type = details.get("type")
    if game_type not in ["game", "demo"]: # A veces las demos tienen jugadores
        logging.info(f"El appid {appid} es de tipo '{game_type}', no un juego. Se omitirá.")
        return True

    # 2. Verificamos la fecha de lanzamiento
    release_date_info = details.get("release_date", {})
    
    if release_date_info.get("coming_soon", False):
        logging.info(f"El juego {appid} está marcado como 'coming soon'. Se omitirá.")
        return True

    release_date_str = release_date_info.get("date", "").lower()
    if not release_date_str or any(keyword in release_date_str for keyword in ["coming soon", "tba", "to be announced"]):
        logging.info(f"El juego {appid} tiene una fecha de lanzamiento no definida ('{release_date_str}'). Se omitirá.")
        return True
    
    logging.info(f"El juego {appid} parece estar lanzado. Se procederá con el scraping.")
    return False

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main(appids):
    """
    Función principal que orquesta el scraping y guardado de datos.
    """
    # --- Lógica para determinar si el script debe ejecutarse ---
    # Se conecta a PostgreSQL para ver si ya existen datos históricos.
    
    DB_USER = os.getenv("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
    DB_PORT = os.getenv("POSTGRES_PORT", "5432")
    DB_NAME = os.getenv("POSTGRES_DB", "steam_data_db")
    TABLE_NAME = 'trusted_zone'
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    data_exists = False
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            query = text(f"SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = '{TABLE_NAME}');")
            data_exists = conn.execute(query).scalar()
    except Exception as e:
        logging.warning(f"[WARNING] No se pudo conectar a PostgreSQL para verificar data histórica. Se procederá con el scraping. Error: {e}")

    is_first_day = datetime.now().day == 1

    # if data_exists and not is_first_day:
    #     logging.info("El scraping se omite. La data histórica ya existe en PostgreSQL y no es el primer día del mes.")
    #     return
    
    # if data_exists and is_first_day:
    #     logging.info("Iniciando scraping mensual de actualización (los datos ya existen en PostgreSQL).")
    
    # if not data_exists:
    #     logging.info("No se encontró data histórica en PostgreSQL. Iniciando scraping completo por primera vez.")


    if not appids:
        logging.critical("[CRITICAL] La lista de appids está vacía. Finalizando.")
        return

    session = setup_session()
    all_data = []
    total_appids = len(appids)
    
    logging.info(f"Iniciando scraping para {total_appids} appids.")

    for i, appid in enumerate(appids):
        logging.info(f"[{i+1}/{total_appids}] Procesando appid: {appid}")
        
        if is_game_unreleased(appid, session):
            time.sleep(1)  # Pausa para no saturar la API de Steam
            continue

        game_df = scrape_steamcharts(session, appid)
        
        if game_df is not None and not game_df.empty:
            all_data.append(game_df)
            logging.info(f"Se encontraron {len(game_df)} registros para el appid {appid}.")
        else:
            logging.info(f"No se encontraron datos para el appid {appid}.")
        
        logging.info(f"Pausa de {SLEEP_INTERVAL} segundos.")
        time.sleep(SLEEP_INTERVAL)

    if not all_data:
        logging.warning("No se recopiló ningún dato. No se generará ningún archivo CSV.")
        return

    final_df = pd.concat(all_data, ignore_index=True)    
    final_df = final_df[['appid', 'name', 'month_date', 'avg_players']]

    # Guardar en CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    final_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf-8')
    logging.info(f"✅ Proceso de scraping completado. Datos guardados en: {OUTPUT_CSV_PATH}")

    # Guardar en PostgreSQL
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            final_df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
            logging.info(f"✅ Datos guardados en la tabla {TABLE_NAME} de PostgreSQL.")
    except Exception as e:
        logging.error(f"[ERROR] No se pudo guardar en PostgreSQL: {e}")


if __name__ == '__main__':
    example_appids = [730, 570, 1086940] 
    main(example_appids)