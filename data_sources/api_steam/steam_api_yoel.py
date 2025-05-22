import requests
import json
import time
import os
import glob

MAX_PER_PART = 5200
INPUT_FILE = "steam_appids_remaining_yoel.json"
OUTPUT_PREFIX = "steam_games_data_yoel_part"

def get_game_details(appid):
    try:
        url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=english"
        res = requests.get(url, timeout=10)
        # 1) Parseo seguro del JSON
        raw_text = res.content.decode("utf-8-sig")
        raw = json.loads(raw_text)

        if not isinstance(raw, dict):
            # la respuesta no es un dict: nada que procesar
            return None, False

        # 2) Extraigo la sección de este appid
        app_section = raw.get(str(appid))
        if not isinstance(app_section, dict):
            # viene null, o un tipo inesperado
            return None, False

        # 3) Compruebo el success
        if app_section.get("success") is not True:
            return None, False

        # 4) Devuelvo todos los datos crudos
        return app_section.get("data"), False
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
        raw_text = res.content.decode("utf-8-sig")
        data = json.loads(raw_text)
        return data.get("reviews", []), False  # sin error
    except Exception as e:
        print(f"❌ Error al obtener reseñas para {appid}: {e}")
        return [], True  # hubo error


# Detecta archivos steam_games_data_part1.json, part2.json, …
def find_existing_parts():
    pattern = f"{OUTPUT_PREFIX}*.json"
    parts = glob.glob(pattern)
    # Devuelve lista de tuplas (número, nombre archivo), ordenada
    result = []
    for p in parts:
        name = os.path.basename(p)
        try:
            n = int(name.replace(OUTPUT_PREFIX, "").replace(".json", ""))
            result.append((n, p))
        except:
            pass
    return sorted(result, key=lambda x: x[0])

def load_appids_from_file(filename):
    with open(filename, "r", encoding="utf-8") as f:
        return json.load(f)

def load_part(file):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)

def save_part(data, part_num):
    fname = f"{OUTPUT_PREFIX}{part_num}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 Guardado parte {part_num}: {len(data)} juegos → {fname}")


# MAIN
if __name__ == "__main__":

    # 1) Detectar partes y cargar processed set completo
    parts = find_existing_parts()
    juegos_procesados = set()
    if parts:
        # Carga todos los appid de todas las partes
        for _, part_file in parts:
            data = load_part(part_file)
            juegos_procesados.update(j["appid"] for j in data)
        # Decide si continuar la última parte o arrancar una nueva
        last_part_num, last_file = parts[-1]
        last_data = load_part(last_file)
        if len(last_data) < MAX_PER_PART:
            juegos_extraidos = last_data
            print(f"🔄 Reanudando parte {last_part_num}, {len(juegos_extraidos)} juegos ya extraídos")
        else:
            last_part_num += 1
            juegos_extraidos = []
            print(f"✨ Todas las partes previas llenas, arrancando parte {last_part_num}")
    else:
        last_part_num = 1
        juegos_extraidos = []
        print("✨ Comenzando en parte 1")


    # 2) Obtener lista de todos los juegos
    games =  load_appids_from_file(INPUT_FILE)
    print(f"🎮 Total de juegos disponibles: {len(games)}")


    # 3) Iterar y llenar partes
    for i, game in enumerate(games):
        appid = game.get("appid")
        name  = game.get("name","").strip()

        if not name or appid in juegos_procesados:
            print(f"[{i+1}] ⏩ App sin nombre o ya procesado ({appid}), se omite.")
            continue

        print(f"\n[{i+1}] 📦 Procesando: {name} ({appid})")

        # si la parte actual ya llegó a MAX_PER_PART, la guardo y avanzo
        if len(juegos_extraidos) >= MAX_PER_PART:
            save_part(juegos_extraidos, last_part_num)
            last_part_num += 1
            juegos_extraidos = []
            print(f"➡️ Pasando a parte {last_part_num}")

        # Procesar este juego
        details, err_d = get_game_details(appid)
        reviews, err_r = get_game_reviews(appid)

        juego_info = {
            "appid": appid,
            "name": name,
            "details": details if details else None,
            "error_details": bool(err_d),
            "reviews": reviews if reviews else [],
            "error_reviews": bool(err_r)
        }
        juegos_extraidos.append(juego_info)
        juegos_procesados.add(appid)

        if len(juegos_extraidos) % 100 == 0:
            save_part(juegos_extraidos, last_part_num)
            print(f"💾 Guardado parcial dentro de parte {last_part_num}: {len(juegos_extraidos)} juegos")

        # Sleep para respetar rate limits
        time.sleep(0.8)

    # 4) Guardado final de la última parte
    save_part(juegos_extraidos, last_part_num)
    print("✅ Proceso completado.")