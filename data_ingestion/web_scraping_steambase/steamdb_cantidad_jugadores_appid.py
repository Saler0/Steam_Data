import requests
from bs4 import BeautifulSoup
import json
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from datetime import datetime

# Configuración
INPUT_JSON = "all_steam_games.json"  # Archivo con appids y nombres
OUTPUT_CSV = "steamdb_historical_data.csv"  # Archivo CSV de salida
REQUEST_TIMEOUT = 10  # Tiempo de espera por solicitud
RETRY_ATTEMPTS = 5  # Reintentos
SLEEP_INTERVAL_SCRAPE = 10.0  # Pausa de 10 segundos (~6 solicitudes por minuto)
SAVE_INTERVAL = 20  # Guardar cada 20 juegos
ERROR_DELAY = 60.0  # Pausa adicional de 60 segundos tras error (incluyendo 403)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

def setup_session():
    """Configura una sesión con reintentos."""
    session = requests.Session()
    retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=2, status_forcelist=[403, 429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def scrape_steamdb(appid, session):
    """Scrapea datos históricos de SteamDB desde la página de charts."""
    url = f"https://steamdb.info/app/{appid}/charts/"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # Buscar la tabla de datos históricos
        table = soup.find("table", class_="table")  # Clase típica en SteamDB charts
        if not table:
            print(f"⚠️ No se encontró la tabla de datos históricos para appid {appid}")
            return None
        
        # Extraer encabezados
        headers = [th.text.strip() for th in table.find("thead").find_all("th") if th.text.strip()]
        expected_headers = ["Month", "Peak", "Gain", "% Gain"]  # Columnas solicitadas
        if not all(header in headers for header in expected_headers):
            print(f"⚠️ Encabezados no coinciden con los esperados para appid {appid}: {headers}")
            return None
        
        # Extraer datos de las filas
        rows = []
        for row in table.find("tbody").find_all("tr"):
            cols = [td.text.strip() for td in row.find_all("td")]
            if len(cols) >= len(expected_headers):
                try:
                    month = cols[headers.index("Month")]
                    peak = float(cols[headers.index("Peak")].replace(",", "")) if cols[headers.index("Peak")] else "N/A"
                    gain = float(cols[headers.index("Gain")].replace(",", "").replace("+", "")) if cols[headers.index("Gain")] else "N/A"
                    percent_gain = float(cols[headers.index("% Gain")].replace("%", "").replace("+", "")) if cols[headers.index("% Gain")] else "N/A"
                    rows.append([month, peak, gain, percent_gain])
                except (ValueError, IndexError) as e:
                    print(f"⚠️ Error al procesar fila para appid {appid}: {e}")
                    continue
        
        if not rows:
            print(f"⚠️ No se encontraron filas válidas para appid {appid}")
            return None
        
        # Crear DataFrame con los datos
        df = pd.DataFrame(rows, columns=["Month", "Peak", "Gain", "% Gain"])
        return df
    
    except requests.RequestException as e:
        print(f"❌ Error al scrapear SteamDB para appid {appid}: {e}")
        time.sleep(ERROR_DELAY)  # Pausa mayor tras error, incluyendo 403
        return None

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
    
    # Lista para almacenar resultados
    all_data = []
    
    # Procesar cada juego
    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name")
        print(f"[{i+1}] 📦 Procesando: {name} ({appid})")
        
        # Obtener datos históricos de SteamDB
        df_game = scrape_steamdb(appid, session)
        
        # Almacenar resultados
        if df_game is not None:
            df_game.insert(0, "appid", appid)
            df_game.insert(1, "name", name)
            all_data.append(df_game)
        
        # Guardado parcial cada 20 juegos
        if (i + 1) % SAVE_INTERVAL == 0:
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
                print(f"💾 Guardado parcial: {len(combined_df)} registros")
            else:
                print("⚠️ No hay datos para guardar parcialmente.")
        
        # Pausa para respetar límites del servidor
        time.sleep(SLEEP_INTERVAL_SCRAPE)
    
    # Guardado final
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        print(f"✅ Proceso completado. Datos guardados en {OUTPUT_CSV}")
    else:
        print("❌ No se obtuvieron datos para guardar.")

if __name__ == "__main__":
    main()