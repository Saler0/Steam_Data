import json
import glob
import os
import pandas as pd

# Prefijo de los archivos generados
OUTPUT_PREFIX = "steam_games_data_gustavo_part"
OUTPUT_JSON = "all_steam_games.json"
OUTPUT_CSV = "all_steam_games.csv"

def find_existing_parts():
    """Detecta todos los archivos steam_games_data_gustavo_part*.json."""
    pattern = f"{OUTPUT_PREFIX}*.json"
    parts = glob.glob(pattern)
    # Devuelve lista de tuplas (número, nombre archivo), ordenada
    result = []
    for p in parts:
        name = os.path.basename(p)
        try:
            n = int(name.replace(OUTPUT_PREFIX, "").replace(".json", ""))
            result.append((n, p))
        except ValueError:
            pass
    return sorted(result, key=lambda x: x[0])

def load_part(file):
    """Carga los datos de un archivo JSON."""
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)

def unify_game_data():
    """Unifica los datos de todos los archivos JSON, extrayendo solo appid y name."""
    # Encontrar todas las partes
    parts = find_existing_parts()
    if not parts:
        print("❌ No se encontraron archivos JSON con el prefijo especificado.")
        return None

    # Conjunto para evitar duplicados y lista para almacenar datos
    seen_appids = set()
    unified_games = []

    # Iterar sobre cada archivo
    for part_num, part_file in parts:
        print(f"📂 Procesando parte {part_num}: {part_file}")
        data = load_part(part_file)
        
        # Extraer appid y name de cada juego
        for game in data:
            appid = game.get("appid")
            name = game.get("name", "").strip()
            
            # Filtrar juegos sin nombre o duplicados
            if not name or appid in seen_appids:
                continue
            
            unified_games.append({
                "appid": appid,
                "name": name
            })
            seen_appids.add(appid)
    
    print(f"🎮 Total de juegos únicos procesados: {len(unified_games)}")
    return unified_games

def save_unified_data(data):
    """Guarda los datos unificados en archivos JSON y CSV."""
    if not data:
        print("❌ No hay datos para guardar.")
        return
    
    # Guardar en JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"💾 Datos unificados guardados en {OUTPUT_JSON}")

    # Guardar en CSV
    df = pd.DataFrame(data)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"💾 Datos unificados guardados en {OUTPUT_CSV}")

# MAIN
if __name__ == "__main__":
    # Unificar los datos
    unified_games = unify_game_data()
    
    # Guardar los datos unificados
    save_unified_data(unified_games)
    print("✅ Proceso de unificación completado.")