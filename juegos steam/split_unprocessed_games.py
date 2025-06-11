import json
import os

# Configuración
INPUT_JSON = "unprocessed_games.json"  # Archivo de entrada con los juegos
NUM_PARTS = 20  # Número de partes en las que dividir
OUTPUT_PREFIX = "unprocessed_games_part_"

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

def split_json(games, num_parts):
    total_games = len(games)
    games_per_part = total_games // num_parts
    remainder = total_games % num_parts

    for i in range(num_parts):
        start_idx = i * games_per_part
        end_idx = start_idx + games_per_part if i < num_parts - 1 else start_idx + games_per_part + remainder
        part_games = games[start_idx:end_idx]
        
        output_file = f"{OUTPUT_PREFIX}{i+1}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(part_games, f, ensure_ascii=False, indent=4)
        print(f"✅ Parte {i+1} guardada en {output_file} con {len(part_games)} juegos.")

def main():
    games = load_games()
    if not games:
        return
    split_json(games, NUM_PARTS)
    print(f"✅ Archivo dividido en {NUM_PARTS} partes.")

if __name__ == "__main__":
    main()