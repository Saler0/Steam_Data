import requests
from bs4 import BeautifulSoup
import json
import time
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from datetime import datetime
import re
import os

# Configuración
INPUT_JSON = "unprocessed_games_part_17.json"
OUTPUT_JSON = "steambase_historical_players_by_month_part_17.json"
PROCESSED_FILE = "processed_games_part17.json"
REQUEST_TIMEOUT = 10
RETRY_ATTEMPTS = 5
SLEEP_INTERVAL_SCRAPE = 10.0
SAVE_INTERVAL = 20
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
]

# Mapeo manual para corregir nombres
name_mapping = {
    "ULTRATORO 2 Demo": "ultratoro",
    "King of Kinks Wallpaper": "king-of-kinks",
    "忍堕とし": "nin-datoshi",
    "Sailist Demo": "sailist",
    "Naheulbeuk's Dungeon Master Demo": "naheulbeuks-dungeon-master",
    "Twist & Turn": "twist-and-turn",
    "ONE PIECE: PIRATE WARRIORS 4 Character Pass 2": "one-piece-pirate-warriors-4",
    "Dead City: Sci-Fi Pack": "dead-city",
    "Pine Hearts Demo": "pine-hearts",
    "Tenders fight Demo": "tenders-fight",
    "Probo Rush Demo": "probo-rush"
}

# Configuración para Steam API
STEAM_API_KEY = "57CA4651BA0DEA92783B179593FBE41A"
STEAM_API_URL = "http://api.steampowered.com/ISteamApps/GetAppList/v2/"
STEAM_STORE_URL = "https://store.steampowered.com/app/{appid}"

def setup_session():
    session = requests.Session()
    retries = Retry(total=RETRY_ATTEMPTS, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def normalize_game_name(name):
    mapped_name = name_mapping.get(name, name)
    # Eliminar "DEMO" si está presente
    mapped_name = re.sub(r'\bDEMO\b', '', mapped_name, flags=re.IGNORECASE).strip()
    return re.sub(r'[^\w\s-]', '', mapped_name).strip().lower().replace(' ', '-')

def load_processed_games():
    if os.path.exists(PROCESSED_FILE):
        with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    return set()

def save_processed_games(processed_appids):
    with open(PROCESSED_FILE, "w", encoding="utf-8") as f:
        json.dump(list(processed_appids), f, ensure_ascii=False, indent=4)

def is_dlc(appid, session):
    """Verifica si el juego es un DLC scraping la página de Steam."""
    url = STEAM_STORE_URL.format(appid=appid)
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        if "DLC" in soup.title.string or "Expansion" in soup.get_text():
            print(f"⏭️ Saltando: AppID {appid} es un DLC.")
            return True
        return False
    except requests.RequestException:
        return False

def verify_game_name_with_steam(appid, current_name):
    """Obtiene el nombre oficial del juego desde la API de Steam."""
    try:
        url = f"http://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2/?key={STEAM_API_KEY}&appid={appid}"
        res = requests.get(url, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        data = res.json()
        if data.get("game", {}).get("gameName"):
            steam_name = data["game"]["gameName"]
            normalized_steam_name = normalize_game_name(steam_name)
            if normalized_steam_name != normalize_game_name(current_name):
                print(f"⚠️ Discrepancia encontrada: {current_name} (tu JSON) vs {steam_name} (Steam) para AppID {appid}")
            return steam_name
        print(f"⚠️ No se encontró nombre para AppID {appid} en Steam API.")
        return current_name
    except requests.RequestException as e:
        print(f"❌ Error al verificar con Steam API: {e}")
        return current_name

def scrape_steambase_charts(game_name, appid, session):
    normalized_name = normalize_game_name(game_name)
    url = f"https://steambase.io/games/{normalized_name}/steam-charts"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        table = soup.find("table", class_="w-full caption-bottom text-sm")
        if not table:
            print(f"⚠️ No se encontró tabla para {game_name} ({normalized_name}). Verificando nombre...")
            corrected_name = verify_game_name_with_steam(appid, game_name)
            if corrected_name != game_name:
                print(f"🔄 Corrigiendo nombre a {corrected_name}")
                normalized_name = normalize_game_name(corrected_name)
                url = f"https://steambase.io/games/{normalized_name}/steam-charts"
                res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")
                table = soup.find("table", class_="w-full caption-bottom text-sm")
                if not table:
                    print(f"❌ Nombre corregido {corrected_name} tampoco funciona.")
                    return None, normalized_name
        
        if not table:
            print(f"❌ No se pudo encontrar la tabla para {game_name} después de corrección.")
            return None, normalized_name

        monthly_data = {}
        for row in table.find_all("tr")[1:]:
            cols = row.find_all("td")
            if len(cols) >= 5:
                month_year = cols[0].text.strip()
                try:
                    month_year = datetime.strptime(month_year, "%B %Y").strftime("%Y-%m")
                except ValueError:
                    print(f"⚠️ Formato de mes/año inválido para {game_name}: {month_year}")
                    continue
                
                avg_players = cols[1].text.strip()
                try:
                    avg_players = float(avg_players.replace(".", ""))
                except ValueError:
                    avg_players = "N/A"
                
                gain_loss = cols[2].text.strip()
                try:
                    gain_loss = float(gain_loss.replace(".", "").replace("+", "").replace("-", "-"))
                except ValueError:
                    gain_loss = "N/A"
                
                gain_loss_percent = cols[3].text.strip().replace("%", "")
                try:
                    gain_loss_percent = float(gain_loss_percent.replace(".", "").replace("+", "").replace("-", "-"))
                except ValueError:
                    gain_loss_percent = "N/A"
                
                peak_players = cols[4].text.strip()
                try:
                    peak_players = float(peak_players.replace(".", ""))
                except ValueError:
                    peak_players = "N/A"
                
                monthly_data[month_year] = {
                    "avg_players": avg_players,
                    "gain_loss": gain_loss,
                    "gain_loss_percent": gain_loss_percent,
                    "peak_players": peak_players
                }
        
        return monthly_data if monthly_data else None, normalized_name
    except requests.RequestException as e:
        print(f"❌ Error al scrapear Steambase para {game_name}: {e}")
        return None, normalized_name

def load_games():
    try:
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Archivo {INPUT_JSON} no encontrado.")
        return []
    except json.JSONDecodeError:
        print(f"❌ Error al decodificar el archivo JSON {INPUT_JSON}.")
        return []

def load_existing_data():
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Error al decodificar {OUTPUT_JSON}, comenzando desde cero.")
            return []
    return []

def save_data(data):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def is_dlc(appid, session):
    """Verifica si el juego es un DLC scraping la página de Steam."""
    url = STEAM_STORE_URL.format(appid=appid)
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    try:
        res = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        # Verificar si soup.title existe antes de acceder a .string
        title_text = soup.title.string if soup.title and soup.title.string else ""
        if "DLC" in title_text or "Expansion" in soup.get_text():
            print(f"⏭️ Saltando: AppID {appid} es un DLC.")
            return True
        return False
    except requests.RequestException as e:
        print(f"⚠️ No se pudo verificar si AppID {appid} es un DLC: {e}")
        return False  # Si falla, asumimos que no es DLC y continuamos

def main():
    session = setup_session()
    
    games = load_games()
    if not games:
        print("❌ No se encontraron juegos para procesar. Verifica el archivo JSON.")
        return
    
    processed_appids = load_processed_games()
    
    results = load_existing_data()
    if not results:
        results = []

    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name")
        if appid in processed_appids:
            print(f"[{i+1}] ⏩ Saltando: {name} ({appid}) (ya procesado)")
            continue
        
        # Verificar si es DLC
        if is_dlc(appid, session):
            processed_appids.add(appid)
            save_processed_games(processed_appids)
            continue
        
        print(f"[{i+1}] 📦 Procesando: {name} ({appid})")
        
        monthly_data, normalized_name = scrape_steambase_charts(name, appid, session)
        
        result = {
            "appid": appid,
            "name": name,
            "normalized_name": normalized_name,
            "monthly_data": monthly_data if monthly_data else {}
        }
        
        results.append(result)
        processed_appids.add(appid)
        save_processed_games(processed_appids)
        
        if (i + 1) % SAVE_INTERVAL == 0:
            save_data(results)
            print(f"💾 Guardado parcial: {len(results)} juegos")
        
        time.sleep(SLEEP_INTERVAL_SCRAPE)
    
    save_data(results)
    print(f"✅ Proceso completado. Datos guardados en {OUTPUT_JSON}")

if __name__ == "__main__":
    main()