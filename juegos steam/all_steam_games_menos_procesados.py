import json
import os

# Definición de los nombres de los archivos
INPUT_JSON = "all_steam_games.json"
PROCESSED_FILE = "processed_games.json"
OUTPUT_JSON = "unprocessed_games.json"

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

def load_processed_games():
    if os.path.exists(PROCESSED_FILE):
        try:
            with open(PROCESSED_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"⚠️ Error al decodificar {PROCESSED_FILE}, asumiendo lista vacía.")
            return []
    return []

def save_unprocessed_games(unprocessed_games):
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(unprocessed_games, f, ensure_ascii=False, indent=4)
    print(f"✅ Lista de juegos no procesados guardada en {OUTPUT_JSON}")

def main():
    # Cargar los juegos y los AppIDs procesados
    games = load_games()
    processed_appids = load_processed_games()

    # Convertir a conjuntos para una resta eficiente
    processed_set = set(processed_appids)

    # Filtrar los juegos no procesados y mantener el formato {appid, name}
    unprocessed_games = [
        {"appid": game["appid"], "name": game["name"]}
        for game in games
        if game.get("appid") is not None and game["appid"] not in processed_set
    ]

    # Contar y mostrar el número de AppIDs restantes
    remaining_count = len(unprocessed_games)
    if remaining_count > 0:
        print(f"🔍 Te quedan {remaining_count} AppIDs por procesar.")
        save_unprocessed_games(unprocessed_games)
    else:
        print("✅ Todos los juegos ya han sido procesados.")

if __name__ == "__main__":
    main()
    