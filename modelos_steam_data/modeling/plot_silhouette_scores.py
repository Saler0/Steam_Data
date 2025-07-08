import pandas as pd
import matplotlib.pyplot as plt
import os

DATA_DIR = "data/processed"
OUTPUT_DIR = "reports/figures"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Cargar CSV ===
df = pd.read_csv(f"{DATA_DIR}/kmeans_silhouette_scores_latest.csv")

# === Graficar ===
plt.figure(figsize=(10, 5))
plt.plot(df["k"], df["silhouette_score"], marker="o")
plt.title("Silhouette Score vs. Número de Clusters (k)")
plt.xlabel("Número de clusters (k)")
plt.ylabel("Silhouette Score")
plt.grid(True)
plt.tight_layout()

# === Guardar y mostrar ===
output_path = f"{OUTPUT_DIR}/silhouette_vs_k.png"
plt.savefig(output_path)
plt.show()

print(f"✅ Gráfico guardado en: {output_path}")
