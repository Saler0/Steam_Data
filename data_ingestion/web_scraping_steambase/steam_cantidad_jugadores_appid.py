import requests
import json
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timezone

# Configuración
INPUT_JSON = "all_steam_games.json"  # Archivo con appids y nombres
OUTPUT_CSV = "steam_player_counts_with_timestamp.csv"
REQUEST_TIMEOUT = 10  # Tiempo de espera por solicitud
RETRY_ATTEMPTS = 3  # Reintentos para errores 429, 500, etc.
SLEEP_INTERVAL_API = 1.0  # Pausa entre solicitudes a la API (1 por segundo)

def setup_session():
    """Configura una sesión con reintentos."""
    session = requests.Session()
    retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_current_players(appid, session):
    """Obtiene el número de jugadores concurrentes desde la API de Steam."""
    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={appid}"
    try:
        res = session.get(url, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        return data.get("response", {}).get("player_count", 0)
    except requests.RequestException as e:
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

def main():
    # Configurar sesión
    session = setup_session()
    
    # Cargar juegos desde el archivo JSON
    games = load_games()
    if not games:
        print("❌ No se encontraron juegos para procesar.")
        return
    
    # Lista para almacenar resultados
    results = []
    
    # Procesar cada juego
    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name")
        print(f"[{i+1}] 📦 Procesando: {name} ({appid})")
        
        # Obtener jugadores actuales y timestamp
        current_players = get_current_players(appid, session)
        timestamp = get_current_timestamp()
        
        # Almacenar resultados
        results.append({
            "appid": appid,
            "name": name,
            "current_players": current_players,
            "timestamp": timestamp
        })
        
        # Guardado parcial cada 100 juegos
        if (i + 1) % 100 == 0:
            df = pd.DataFrame(results)
            df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
            print(f"💾 Guardado parcial: {len(results)} juegos")
        
        # Pausa para respetar límites de la API
        time.sleep(SLEEP_INTERVAL_API)
    
    # Guardado final
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Proceso completado. Datos guardados en {OUTPUT_CSV}")

if __name__ == "__main__":
    main()