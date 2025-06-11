import requests
import json
import time
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

# Configuración
INPUT_JSON = "all_steam_games.json"  # Archivo con appids y nombres
REQUEST_TIMEOUT = 10  # Tiempo de espera por solicitud
RETRY_ATTEMPTS = 5  # Reintentos
SLEEP_INTERVAL = 10.0  # Pausa de 10 segundos (~6 solicitudes por minuto)
ERROR_DELAY = 60.0  # Pausa adicional de 60 segundos tras error
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

# Crear carpetas para almacenar los archivos
CHARTS_DIR = "charts"
INFO_DIR = "info"
os.makedirs(CHARTS_DIR, exist_ok=True)
os.makedirs(INFO_DIR, exist_ok=True)

def setup_session():
    """Configura una sesión con reintentos."""
    session = requests.Session()
    retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def download_page(url, filename, session):
    """Descarga una página y la guarda como archivo HTML."""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        with open(filename, "w", encoding="utf-8") as f:
            f.write(res.text)
        print(f"💾 Descargado: {filename}")
    except requests.RequestException as e:
        print(f"❌ Error al descargar {url}: {e}")
        time.sleep(ERROR_DELAY)

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

def main():
    # Configurar sesión
    session = setup_session()
    
    # Cargar juegos desde el archivo JSON
    games = load_games()
    if not games:
        print("❌ No se encontraron juegos para procesar. Verifica el archivo JSON.")
        return
    
    # Descargar páginas para cada juego
    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name")
        print(f"[{i+1}] 📦 Procesando: {name} ({appid})")
        
        # Descargar página de charts
        charts_url = f"https://steamdb.info/app/{appid}/charts/"
        charts_filename = os.path.join(CHARTS_DIR, f"steamdb_app_{appid}_charts.html")
        if not os.path.exists(charts_filename):
            download_page(charts_url, charts_filename, session)
        
        # Descargar página de info
        info_url = f"https://steamdb.info/app/{appid}/info/"
        info_filename = os.path.join(INFO_DIR, f"steamdb_app_{appid}_info.html")
        if not os.path.exists(info_filename):
            download_page(info_url, info_filename, session)
        
        # Pausa para respetar límites del servidor
        time.sleep(SLEEP_INTERVAL)
    
    print("✅ Descarga completada.")

if __name__ == "__main__":
    main()