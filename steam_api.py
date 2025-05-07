import requests
import json
import time
import os
from datetime import datetime, timezone

def get_all_games():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    res = requests.get(url)
    return res.json()["applist"]["apps"]

def get_game_details(appid):
    try:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=english"
        res = requests.get(url, timeout=10)
        data = res.json().get(str(appid), {})

        if not isinstance(data, dict):
            return None, False # no hay detalles disponibles

        if data.get("success") is False:
            return None, False # no hay detalles disponibles
        
        return data.get("data", None), False  # sin error
    except Exception as e:
        print(f"❌ Error al obtener detalles para {appid}: {e}")
        return None, True  # hubo error

# se obtinene las 100 reseñas mas recientes del juego en ingles    
def get_game_reviews(appid, num=100):
    try:
        url = f"https://store.steampowered.com/appreviews/{appid}"
        params = {
            "json": 1,
            "num_per_page": num,
            "filter": "recent",
            "language": "english"
        }
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        return data.get("reviews", []), False  # sin error
    except Exception as e:
        print(f"❌ Error al obtener reseñas para {appid}: {e}")
        return [], True  # hubo error

# Cargar progreso anterior si existe
def load_existing_data(filename):
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


# MAIN
if __name__ == "__main__":
    output_file = "steam_games_data.json"
    juegos_extraidos = load_existing_data(output_file)
    juegos_procesados = {j["appid"] for j in juegos_extraidos}

    games = get_all_games()
    print(f"🎮 Total de juegos encontrados: {len(games)}")
    
    for i, game in enumerate(games):
        appid = game.get("appid")
        name = game.get("name", "").strip()

        if not name or appid in juegos_procesados:
            print(f"[{i+1}] ⏩ App sin nombre o ya procesado ({appid}), se omite.")
            continue

        print(f"\n[{i+1}] 📦 Procesando: {name} ({appid})")

        juego_info = {
            "appid": appid,
            "name": name,
        }

        # Detalles del juego
        details, details_error = get_game_details(appid)
        if details_error:
            juego_info["error_details"] = True
        elif details:
            juego_info["details"] = details
        else:
            juego_info["no_details_available"] = True

        # Reseñas
        reviews, reviews_error = get_game_reviews(appid)
        if reviews_error:
            juego_info["error_reviews"] = True
            juego_info["reviews"] = []
        elif not reviews:
            juego_info["no_reviews_available"] = True
            juego_info["reviews"] = []
        else:
            juego_info["reviews"] = reviews

        juegos_extraidos.append(juego_info)

        # Guardado parcial cada 100 juegos
        if len(juegos_extraidos) % 100 == 0:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(juegos_extraidos, f, indent=2, ensure_ascii=False)
            print(f"💾 Guardado parcial: {len(juegos_extraidos)} juegos")
        
        time.sleep(0.9)

    # Guardado final
    with open("steam_games_data.json", "w", encoding="utf-8") as f:
        json.dump(juegos_extraidos, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Datos guardados de {len(juegos_extraidos)} juegos en steam_games_data.json")