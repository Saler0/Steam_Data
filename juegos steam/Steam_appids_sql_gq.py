import requests
import json
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone
import sqlite3
import math

# Configuración
INPUT_JSON = "all_steam_games.json"  # Archivo con appids y nombres
DB_NAME = "steam_player_counts.db"  # Nombre de la base de datos SQLite
REQUEST_TIMEOUT = 10  # Tiempo de espera por solicitud
RETRY_ATTEMPTS = 3  # Reintentos para errores 429, 500, etc.
BATCH_SIZE = 50  # Tamaño del lote para procesamiento
BASE_SLEEP_INTERVAL = 1.0  # Pausa base entre solicitudes (1 por segundo)
MAX_RETRIES_PER_APPID = 2  # Máximo de reintentos por appid

def setup_session():
    """Configura una sesión con reintentos."""
    session = requests.Session()
    retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_current_players(appid, session, retries=0):
    """Obtiene el número de jugadores concurrentes desde la API de Steam con reintentos."""
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid}"
    try:
        res = session.get(url, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        return data.get("response", {}).get("player_count", 0)
    except requests.RequestException as e:
        if retries < MAX_RETRIES_PER_APPID and 500 <= res.status_code <= 504:
            print(f"⚠️ Reintentando para appid {appid} (intento {retries + 1}/{MAX_RETRIES_PER_APPID}): {e}")
            time.sleep(BASE_SLEEP_INTERVAL * (retries + 1))  # Aumentar pausa en reintentos
            return get_current_players(appid, session, retries + 1)
        print(f"❌ Error al obtener jugadores para appid {appid}: {e}")
        return None

def get_current_timestamp():
    """Obtiene el timestamp actual en formato ISO 8601."""
    return datetime.now(timezone.utc).isoformat()

def load_games():
    """Carga los juegos desde el archivo JSON unificado."""
    try:
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Archivo {INPUT_JSON} no encontrado.")
        return []
    except json.JSONDecodeError:
        print(f"❌ Error al decodificar el archivo JSON {INPUT_JSON}.")
        return []

def init_db():
    """Inicializa la base de datos SQLite."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_counts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            appid INTEGER,
            name TEXT,
            current_players INTEGER,
            timestamp TEXT
        )
    ''')
    conn.commit()
    return conn

def save_to_db(conn, data):
    """Guarda los datos en la base de datos."""
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO player_counts (appid, name, current_players, timestamp)
        VALUES (?, ?, ?, ?)
    ''', [(d["appid"], d["name"], d["current_players"], d["timestamp"]) for d in data])
    conn.commit()

def main():
    # Configurar sesión
    session = setup_session()
    
    # Inicializar base de datos
    conn = init_db()
    
    # Cargar juegos desde el archivo JSON
    games = load_games()
    if not games:
        print("❌ No se encontraron juegos para procesar.")
        conn.close()
        return
    
    # Procesar en lotes
    total_games = len(games)
    for i in range(0, total_games, BATCH_SIZE):
        batch = games[i:i + BATCH_SIZE]
        results = []
        
        for j, game in enumerate(batch):
            appid = game.get("appid")
            name = game.get("name")
            print(f"[{i + j + 1}/{total_games}] 📦 Procesando: {name} ({appid})")
            
            # Obtener jugadores actuales y timestamp
            current_players = get_current_players(appid, session)
            timestamp = get_current_timestamp()
            
            if current_players is not None:
                results.append({
                    "appid": appid,
                    "name": name,
                    "current_players": current_players,
                    "timestamp": timestamp
                })
            else:
                results.append({
                    "appid": appid,
                    "name": name,
                    "current_players": -1,  # Indicador de fallo
                    "timestamp": timestamp
                })
            
            # Pausa dinámica
            time.sleep(BASE_SLEEP_INTERVAL * (1 + j / BATCH_SIZE))  # Aumenta ligeramente la pausa por lote
        
        # Guardar lote en la base de datos
        save_to_db(conn, results)
        print(f"💾 Guardado lote {i // BATCH_SIZE + 1} con {len(results)} juegos")
    
    # Guardado final y cierre
    conn.close()
    print(f"✅ Proceso completado. Datos guardados en {DB_NAME}")

if __name__ == "__main__":
    main()