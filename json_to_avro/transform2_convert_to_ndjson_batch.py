import os
import json
import re

input_dir = r"C:\json_al_cluster_upc_correcion[]yespacios"
output_dir = r"C:\njson_al_cluster_upc"

os.makedirs(output_dir, exist_ok=True)

def fix_keys(obj):
    """Corrige claves que no cumplen con el estándar Avro: empiezan con número."""
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            new_key = key
            if re.match(r"^\d", key):  # empieza con número
                new_key = f"code_{key}"
            new_obj[new_key] = fix_keys(value)
        return new_obj
    elif isinstance(obj, list):
        return [fix_keys(item) for item in obj]
    else:
        return obj

def split_and_clean_json(raw_text):
    # separa objetos por heurística: se asume que terminan en } y comienzan en {
    raw_text = raw_text.strip()
    objects = raw_text.split('}\n{')
    cleaned = []
    for i, obj in enumerate(objects):
        if not obj.startswith('{'):
            obj = '{' + obj
        if not obj.endswith('}'):
            obj = obj + '}'
        try:
            json_obj = json.loads(obj)
            fixed_obj = fix_keys(json_obj)
            cleaned.append(fixed_obj)
        except json.JSONDecodeError:
            print(f"❌ Error parsing object #{i}")
    return cleaned

for filename in os.listdir(input_dir):
    if filename.endswith(".json"):
        input_path = os.path.join(input_dir, filename)
        output_path = os.path.join(output_dir, filename.replace(".json", ".ndjson"))

        with open(input_path, "r", encoding="utf-8") as f:
            raw_content = f.read()

        entries = split_and_clean_json(raw_content)

        with open(output_path, "w", encoding="utf-8") as f_out:
            for entry in entries:
                f_out.write(json.dumps(entry, ensure_ascii=False) + "\n")

        print(f"✅ Procesado: {filename} → {output_path}")