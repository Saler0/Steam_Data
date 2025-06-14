from playwright.sync_api import sync_playwright
import json
import pandas as pd
import time
import os

# Configuración
INPUT_JSON = "all_steam_games.json"  # Archivo con appids y nombres
OUTPUT_DIR = "iframe_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
SLEEP_INTERVAL = 15.0  # Pausa de 15 segundos entre solicitudes

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

def extract_iframe_data(appid):
    """Extrae datos del iframe de SteamDB."""
    url = f"https://steamdb.info/embed/?appid={appid}"
    filename = os.path.join(OUTPUT_DIR, f"steamdb_app_{appid}_iframe_data.json")
    
    with sync_playwright() as p:
        # Configurar el navegador (usamos Firefox como ejemplo)
        browser = p.firefox.launch(headless=True)  # headless=True para no mostrar el navegador
        page = browser.new_page()
        
        try:
            # Navegar a la URL
            print(f"📦 Procesando appid {appid}")
            page.goto(url, wait_until="networkidle")
            time.sleep(5)  # Esperar a que el gráfico se renderice
            
            # Intentar extraer los datos del gráfico (depende de la implementación de Highcharts)
            script_content = page.content()
            soup = BeautifulSoup(script_content, "html.parser")
            script_tags = soup.find_all("script")
            
            data = None
            for script in script_tags:
                if "Highcharts" in script.text:
                    # Buscar un objeto JSON o datos en el script
                    json_str = script.text.split("series:")[1].split("]}")[0] + "]}"
                    try:
                        data = json.loads(json_str.replace("'", "\""))
                        break
                    except json.JSONDecodeError:
                        continue
            
            if data:
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                print(f"💾 Datos extraídos y guardados en {filename}")
            else:
                print(f"⚠️ No se pudieron extraer datos para appid {appid}")
            
        except Exception as e:
            print(f"❌ Error al procesar {appid}: {e}")
        
        finally:
            browser.close()
    
    return data is not None

def main():
    games = load_games()
    if not games:
        print("❌ No se encontraron juegos para procesar. Verifica el archivo JSON.")
        return
    
    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name")
        if not extract_iframe_data(appid):
            print(f"⚠️ Fallo al extraer datos para {name} ({appid})")
        time.sleep(SLEEP_INTERVAL)
    
    print("✅ Proceso completado.")

if __name__ == "__main__":
    main()