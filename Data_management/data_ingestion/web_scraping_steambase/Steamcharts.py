# -*- coding: utf-8 -*-
"""
Este script recopila datos históricos de jugadores de juegos de Steam desde
Steamcharts.com. Lee una lista de juegos desde un archivo NDJSON,
filtra por tipo "game", extrae los datos de jugadores y los guarda en una
base de datos SQLite, actualizando solo los meses nuevos.
"""

import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import json
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import re
from datetime import datetime
import logging
import sqlite3

# ==============================================================================
# CONFIGURACIÓN
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraping_log.txt", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

REQUEST_TIMEOUT = 10
SLEEP_INTERVAL = 10.0
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

# ==============================================================================
# FUNCIONES DE BASE DE DATOS
# ==============================================================================

def get_db_connection(db_path):
    """Establece conexión con la base de datos."""
    return sqlite3.connect(db_path)

def get_last_months(conn):
    """
    Obtiene el último mes procesado para cada appid en la base de datos.
    """
    query = "SELECT appid, MAX(month) FROM player_counts GROUP BY appid"
    try:
        df = pd.read_sql_query(query, conn)
        return pd.Series(df['MAX(month)'].values, index=df.appid).to_dict()
    except Exception as e:
        logging.error(f"Error al obtener los últimos meses procesados: {e}")
        return {}

def insert_data(conn, data_df, appid, game_name):
    cursor = conn.cursor()
    rename_map = {
        'Avg. Players': 'avg_players',
        'Gain': 'gain',
        '% Gain': 'gain_percent',
        'Peak Players': 'peak_players',
        'Month': 'month'
    }
    data_df = data_df.rename(columns=rename_map)
    data_df['appid'] = appid
    data_df['name'] = game_name

    for index, row in data_df.iterrows():
        try:
            cursor.execute('''
                INSERT OR IGNORE INTO player_counts 
                (appid, name, month, avg_players, gain, gain_percent, peak_players)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                row.get('appid'),
                row.get('name'),
                row.get('month'),
                row.get('avg_players'),
                row.get('gain', None),
                row.get('gain_percent', None),
                row.get('peak_players')
            ))
        except Exception as e:
            logging.error(f"Error insertando la fila {row}: {e}")

    conn.commit()

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

def scrape_steamcharts(appid, name, last_month=None):
    """
    Obtiene los datos de jugadores de Steamcharts, filtrando por meses nuevos.
    """
    url = f"https://steamcharts.com/app/{appid}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        
        tables = pd.read_html(res.text)
        df = tables[-1]
        
        df = df.dropna(axis=1, how='all')

        current_month_str = datetime.now().strftime("%B %Y")
        df['Month'] = df['Month'].replace('Last 30 Days', current_month_str)
        
        df['Month'] = pd.to_datetime(df['Month'], format='%B %Y').dt.strftime('%Y-%m')

        if last_month:
            df = df[df['Month'] > last_month]
        
        return df

    except requests.RequestException as e:
        logging.error(f"[ERROR] Error al scrapear Steamcharts para '{name}' ({appid}): {e}")
        return None
    except (ValueError, IndexError) as e:
        logging.warning(f"[WARNING] No se encontró una tabla válida en Steamcharts para '{name}' ({appid}). Puede que no tenga datos. Error: {e}")
        return None

def load_games(input_file):
    games = []
    logging.info(f"Cargando juegos desde el archivo NDJSON: {input_file}")
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    details = data.get("details", data)
                    
                    if details.get("type") == "game":
                        appid = data.get("appid")
                        name = details.get("name")
                        
                        if appid and name:
                            games.append({"appid": int(appid), "name": name})
                        else:
                            logging.warning(f"Entrada de tipo 'game' encontrada pero sin 'appid' o 'name': {data}")

                except (json.JSONDecodeError, ValueError) as e:
                    logging.warning(f"No se pudo decodificar o procesar la línea en el archivo de entrada: {line.strip()} - Error: {e}")
                    continue
        return games
    except FileNotFoundError:
        logging.critical(f"[CRITICAL] Archivo de entrada no encontrado: {input_file}")
        return []

# ==============================================================================
# FUNCIÓN PRINCIPAL
# ==============================================================================

def main(input_file, db_path):
    session = setup_session()
    db_conn = get_db_connection(db_path)
    
    last_months = get_last_months(db_conn)
    logging.info(f"Se encontraron datos del último mes para {len(last_months)} juegos en la base de datos.")
    
    all_games = load_games(input_file)
    if not all_games:
        logging.critical("[CRITICAL] No se encontraron juegos de tipo 'game' para procesar. Finalizando.")
        return

    total_games = len(all_games)
    logging.info(f"Se van a verificar {total_games} juegos para posibles actualizaciones.")
    
    for i, game in enumerate(all_games):
        appid = game.get("appid")
        name = game.get("name")
        
        if not appid or not name:
            logging.warning(f"Saltando entrada inválida: {game}")
            continue

        last_processed_month = last_months.get(appid)
        
        logging.info(f"[{i+1}/{total_games}] Procesando: {name} ({appid}). Último mes en BD: {last_processed_month or 'Ninguno'}")
        
        game_df = scrape_steamcharts(appid, name, last_month=last_processed_month)
        
        if game_df is not None and not game_df.empty:
            logging.info(f"Se encontraron {len(game_df)} meses nuevos para '{name}'.")
            insert_data(db_conn, game_df, appid, name)
        else:
            logging.info(f"No se encontraron meses nuevos o no hay datos para '{name}' ({appid}).")
        
        logging.info(f"Pausa de {SLEEP_INTERVAL} segundos.")
        time.sleep(SLEEP_INTERVAL)
    
    db_conn.close()
    logging.info("✅ Proceso de actualización completado.")