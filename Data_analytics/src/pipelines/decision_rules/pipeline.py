import sys
import yaml
import argparse
from pymongo import MongoClient
from pathlib import Path
import pandas as pd
import json

# Importaciones refactorizadas
from src.utils.spark_utils import get_spark_session
from src.utils.mlflow_utils import start_mlflow_run, log_mlflow_metrics
from src.utils.io import write_parquet_any

from src.pipelines.decision_rules.reglas_decision import (
    regla_precio,
    justificacion_precio,
    saturacion_cluster_1,
    saturacion_cluster_2,
    saturacion_cluster_3,
    justificacion_actividad,
    experiencia_jugador,
    deteccion_abandono,
    limitaciones_tecnicas,
    evaluacion_limitaciones,
    publishers_estudios,
    prioridad_idiomas,
    resena_EarlyAccess_Regalo,
)


# ------------------------
# Stage 1: prepare
# ------------------------
def prepare(params):
    """Extraer datos de MongoDB con Spark y guardarlos como parquet."""
    spark = get_spark_session("SteamPrepare", config={"spark.mongodb.input.uri": params["mongo"]["uri"]})
    
    client = MongoClient(params["mongo"]["uri"])
    db = client[params["mongo"]["database"]]
    collection = db[params["mongo"]["collection"]]
    data = list(collection.find({}))

    df = spark.createDataFrame(data)

    output_path = "data/prepared/"
    Path(output_path).mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(output_path)
    print(f"Datos preparados en {output_path}")


# ------------------------
# Stage 2: apply_rules
# ------------------------
def apply_rules(params):
    """Aplicar reglas al dataset preparado."""
    spark = get_spark_session("SteamApplyRules")
    df = spark.read.parquet("data/prepared/")

    pdf = df.toPandas()

    results = []
    for _, row in pdf.iterrows():
        res = {
            "app_id": row.get("app_id"),
            "precio": regla_precio(row.get("price"), row.get("median_price"), row.get("all_included")),
            "justificacion_precio": justificacion_precio(
                row.get("price"),
                row.get("median_price"),
                row.get("all_included"),
                row.get("avg_playtime"),
                row.get("launch_price"),
            ),
            "saturacion1": saturacion_cluster_1(row.get("k_neighbors"), row.get("cluster_age")),
            "saturacion2": saturacion_cluster_2(row.get("total_reviews"), row.get("std_dev"), row.get("mean_score")),
            "saturacion3": saturacion_cluster_3(
                row.get("pct_recent_launches"),
                row.get("playtime_last_two_weeks"),
                row.get("review_positive_ratio"),
                row.get("review_neutral_ratio"),
            ),
            "actividad": justificacion_actividad(
                row.get("activity_change"),
                row.get("twitch_mentions"),
                row.get("patch_correlation"),
                row.get("f2p_switch"),
            ),
            "experiencia": experiencia_jugador(row.get("hours_played"), row.get("median_hours")),
            "abandono": deteccion_abandono(
                row.get("abandoned_after_review"),
                row.get("review_positive"),
                row.get("hours_last_2w"),
            ),
            "limitaciones": limitaciones_tecnicas(
                row.get("ram_gb"),
                row.get("median_ram_gb"),
                row.get("platforms"),
                row.get("median_platforms"),
                row.get("steam_deck_verified"),
                row.get("requires_connection"),
                row.get("install_size_gb"),
                row.get("p75_install_size"),
            ),
            "eval_limitaciones": evaluacion_limitaciones(row.get("num_limitaciones")),
            "publishers": publishers_estudios(
                row.get("pct_publi_potente"),
                row.get("pct_dev_potente"),
                row.get("pct_ips"),
                row.get("mean_score"),
            ),
            "idiomas": prioridad_idiomas(
                row.get("reviews_en"),
                row.get("reviews_es"),
                row.get("pct_es"),
                row.get("reviews_other"),
                row.get("pct_other"),
            ),
            "resena_extra": resena_EarlyAccess_Regalo(row.get("early_access"), row.get("gifted")),
        }
        results.append(res)

    res_df = pd.DataFrame(results)
    output_dir = Path("data/with_rules/")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "with_rules.parquet"
    res_df.to_parquet(output_file, index=False)
    print(f"Reglas aplicadas, resultados en {output_file}")


# ------------------------
# Stage 3: evaluate
# ------------------------
def evaluate(params):
    """Registrar métricas agregadas en MLflow."""
    df = pd.read_parquet("data/with_rules/with_rules.parquet")

    metrics = {
        "n_juegos": len(df),
        "pct_economicos": (df["precio"] == "Juego considerado económico frente al mercado").mean(),
        "pct_precios_altos": (df["precio"].str.contains("alto")).mean(),
    }

    # Guardar para DVC
    reports_dir = Path("outputs/reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    with open(reports_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    # Uso de la función de utilidad para MLflow
    start_mlflow_run(
        experiment_name=params["mlflow"]["experiment_name"],
        run_name="evaluate_rules",
        tracking_uri=params["mlflow"]["tracking_uri"]
    )
    log_mlflow_metrics(metrics)

    print("Evaluación completada, métricas registradas en MLflow y reports/metrics.json")


# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stage", required=True, choices=["prepare", "apply_rules", "evaluate"], help="La etapa del pipeline a ejecutar.")
    ap.add_argument("--config", default="params.yaml", help="Ruta al archivo de configuración YAML.")
    args = ap.parse_args()
    
    # Cargar los parámetros desde la ruta especificada
    with open(args.config, "r") as f:
        params = yaml.safe_load(f)

    if args.stage == "prepare":
        prepare(params)
    elif args.stage == "apply_rules":
        apply_rules(params)
    elif args.stage == "evaluate":
        evaluate(params)
    else:
        # Esto no debería ocurrir debido a choices
        print("❌ Uso: python pipeline.py [prepare|apply_rules|evaluate]")
