import requests
import json
import glob
import os

# Nombres de los archivos de división
names = [
    "steam_appids_remaining_milenko.json",
    "steam_appids_remaining_yoel.json",
    "steam_appids_remaining_alejandro.json",
    "steam_appids_remaining_gustavo.json",
]
MAX_PARTS = len(names)

# 1) Obtener todos los appid desde Steam
def get_all_games():
    url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
    res = requests.get(url)
    return res.json()["applist"]["apps"]


# 2) Leer todos los appid ya procesados en tus JSON de partes
def load_processed_appids(part_pattern="steam_games_data_part*.json"):
    processed = set()
    for filepath in glob.glob(part_pattern):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                chunk = json.load(f)
            processed.update(item["appid"] for item in chunk if "appid" in item)
        except Exception as e:
            print(f"⚠️ No se pudo leer {filepath}: {e}")
    return processed

# 3) Divide una lista en N partes lo más iguales posibles
def split_list(alist, parts=4):
    n = len(alist)
    k, m = divmod(n, parts)
    splits = []
    start = 0
    for i in range(parts):
        end = start + k + (1 if i < m else 0)
        splits.append(alist[start:end])
        start = end
    return splits

# 4) Guardar las partes con nombres específicos def save_splits:
def save_splits(chunks, filenames):
    for chunk, fname in zip(chunks, filenames):
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(chunk, f, indent=2, ensure_ascii=False)
        print(f"💾 Guardado {len(chunk)} IDs en {fname}")

# 5) Flujo principal
def main():
    games = get_all_games()
    
    all_appids_list = [g["appid"] for g in games]
    print(f"🎮 Total de appid entries: {len(all_appids_list)}")

    processed = load_processed_appids()
    print(f"✔️ Appids procesados encontrados: {len(processed)}")

    # 1. Deduplicar por appid, quedarte con un único nombre por appid
    appid_map = {}
    for g in games:
        appid = g["appid"]
        name = g.get("name", "").strip()
        if appid not in processed and name and appid not in appid_map:
            appid_map[appid] = name

    # 2. Convertir a lista de dicts
    remaining = [{"appid": aid, "name": appid_map[aid]} for aid in sorted(appid_map)]
    print(f"⏳ Juegos restantes únicos por procesar: {len(remaining)}")

    chunks = split_list(remaining, parts=MAX_PARTS)
    save_splits(chunks, names)

if __name__ == "__main__":
    main()
