import os
import json
import subprocess

input_folder = r"C:\data_que_subir_al_cluster_upc"
output_folder = r"C:\data_que_subir_al_cluste_upc_corregida"
os.makedirs(output_folder, exist_ok=True)

for filename in os.listdir(input_folder):
    if filename.endswith(".json"):
        input_path = os.path.join(input_folder, filename)
        output_path = os.path.join(output_folder, filename.replace(".json", "_out.json"))
        print(filename)
        # Ejecutar jq con la transformación deseada
        with open(output_path, "w") as outfile:
            subprocess.run(["jq", ".[]", input_path], stdout=outfile)
