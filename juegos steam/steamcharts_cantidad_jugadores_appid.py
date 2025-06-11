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
OUTPUT_CSV = "steam_historical_players_by_month.csv"
REQUEST_TIMEOUT = 10  # Tiempo de espera por solicitud
RETRY_ATTEMPTS = 5  # Aumentar reintentos a 5
SLEEP_INTERVAL_SCRAPE = 10.0  # Pausa de 10 segundos (~6 solicitudes por minuto)
SAVE_INTERVAL = 20  # Guardar cada 20 juegos
ERROR_DELAY = 30.0  # Pausa adicional de 30 segundos tras un error
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

def setup_session():
    """Configura una sesión con reintentos."""
    session = requests.Session()
    retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def scrape_steam_charts(appid, session):
    """Scrapea datos históricos por mes de Steam Charts con manejo mejorado."""
    url = f"https://steamcharts.com/app/{appid}"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # Verificar si la tabla de datos históricos existe
        table = soup.find("table", id="app-hours-played")
        if not table:
            print(f"⚠️ No se encontró la tabla de datos históricos para appid {appid}")
            return None
        
        # Extraer datos de la tabla
        monthly_data = {}
        for row in table.find_all("tr")[1:]:  # Saltar encabezado
            cols = row.find_all("td")
            if len(cols) >= 2:
                month_year = cols[0].text.strip()
                try:
                    month_year = datetime.strptime(month_year, "%B %Y").strftime("%Y-%m")
                except ValueError:
                    print(f"⚠️ Formato de mes/año inválido para appid {appid}: {month_year}")
                    continue
                avg_players = cols[1].text.strip()
                try:
                    avg_players = float(avg_players.replace(",", ""))
                except ValueError:
                    avg_players = "N/A"
                    print(f"⚠️ Valor de jugadores no válido para appid {appid}, mes {month_year}: {avg_players}")
                monthly_data[month_year] = avg_players
        
        return monthly_data if monthly_data else None
    except requests.RequestException as e:
        print(f"❌ Error al scrapear Steam Charts para appid {appid}: {e}")
        time.sleep(ERROR_DELAY)  # Pausa adicional tras error
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
    results = []
    all_months = set()  # Para rastrear todos los meses encontrados
    
    # Procesar cada juego
    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name")
        print(f"[{i+1}] 📦 Procesando: {name} ({appid})")
        
        # Obtener datos históricos
        monthly_data = scrape_steam_charts(appid, session)
        
        # Almacenar resultados
        result = {
            "appid": appid,
            "name": name
        }
        if monthly_data:
            result.update(monthly_data)
            all_months.update(monthly_data.keys())
        else:
            print(f"⚠️ No se obtuvieron datos históricos para appid {appid}")
        
        results.append(result)
        
        # Guardado parcial cada 20 juegos
        if (i + 1) % SAVE_INTERVAL == 0:
            # Crear DataFrame con todos los meses como columnas
            df = pd.DataFrame(results)
            for month in sorted(all_months):
                if month not in df.columns:
                    df[month] = "N/A"
            df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
            print(f"💾 Guardado parcial: {len(results)} juegos")
        
        # Pausa para respetar límites del servidor
        time.sleep(SLEEP_INTERVAL_SCRAPE)
    
    # Guardado final
    df = pd.DataFrame(results)
    for month in sorted(all_months):
        if month not in df.columns:
            df[month] = "N/A"
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"✅ Proceso completado. Datos guardados en {OUTPUT_CSV}")

if __name__ == "__main__":
    main()