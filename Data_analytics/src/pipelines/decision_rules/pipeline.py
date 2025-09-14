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
    """Extraer y enriquecer datos para reglas: une metadatos, clústeres y agregados de reviews.

    Fuentes disponibles (mejor esfuerzo):
      - Mongo juegos (params.mongo)
      - Parquet de metadatos (data/processed/game_metadata.parquet)
      - Clústeres (data/processed/clusters.parquet)
      - Reviews mensuales (data/warehouse/reviews_monthly.parquet)

    Salida: data/prepared/ con columnas esperadas por apply_rules.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    spark = get_spark_session("SteamPrepare", config={"spark.mongodb.input.uri": params["mongo"]["uri"]})

    # 1) Base: intentar metadatos parquet; si no, caer a Mongo
    base = None
    meta_path = "data/processed/game_metadata.parquet"
    try:
        base = spark.read.parquet(meta_path)
        print(f"[INFO] Cargando metadatos desde {meta_path}")
    except Exception:
        print(f"[WARN] No se pudo cargar {meta_path}; usando Mongo {params['mongo']['database']}.{params['mongo']['collection']}")
        client = MongoClient(params["mongo"]["uri"])
        db = client[params["mongo"]["database"]]
        collection = db[params["mongo"]["collection"]]
        data = list(collection.find({}))
        try:
            client.close()
        except Exception:
            pass
        if not data:
            raise SystemExit("No hay datos base para preparar")
        base = spark.createDataFrame(data)

    # Normalizaciones mínimas
    if 'appid' not in base.columns and 'app_id' in base.columns:
        base = base.withColumnRenamed('app_id', 'appid')
    base = base.withColumn('appid', F.col('appid').cast('string'))

    # Derivar conteo de plataformas si viene como array
    platforms_count = None
    if 'platforms' in base.columns:
        platforms_count = F.when(F.col('platforms').isNull(), F.lit(None)) \
                           .otherwise(F.size(F.col('platforms')))
        base = base.withColumn('platforms_count', platforms_count)

    # 2) Clusters
    clu = None
    try:
        clu = spark.read.parquet('data/processed/clusters.parquet') \
                  .withColumn('appid', F.col('appid').cast('string'))
    except Exception:
        print('[WARN] clusters.parquet no disponible; se omiten agregados por clúster')

    df = base if clu is None else base.join(clu.select('appid','cluster_id'), on='appid', how='left')

    # 3) Reviews mensuales agregadas (pos/neg/total)
    reviews_agg = None
    try:
        rv = spark.read.parquet('data/warehouse/reviews_monthly.parquet') \
                .withColumn('appid', F.col('appid').cast('string'))
        g = (rv.groupBy('appid')
               .agg(F.sum(F.col('pos')).alias('reviews_pos_total'),
                    F.sum(F.col('neg')).alias('reviews_neg_total'),
                    F.sum(F.col('total_reviews')).alias('total_reviews')))
        g = g.withColumn('review_positive_ratio',
                          F.when(F.col('total_reviews') > 0,
                                 F.col('reviews_pos_total')/F.col('total_reviews'))
                           .otherwise(F.lit(None))) \
             .withColumn('review_neutral_ratio', F.lit(None).cast('double'))
        reviews_agg = g
    except Exception:
        print('[WARN] reviews_monthly.parquet no disponible; ratios de reviews ausentes')

    if reviews_agg is not None:
        df = df.join(reviews_agg, on='appid', how='left')

    # 3.b) Agregados por idioma desde Mongo (opcional)
    try:
        import os
        from pymongo import MongoClient as _MC
        rv_uri = os.getenv('MONGO_URI', params.get('mongo', {}).get('uri'))
        rv_db = os.getenv('MONGO_DB_REVIEWS', 'exploitation_zone')
        rv_coll = os.getenv('MONGO_COLL_REVIEWS', 'steam_reviews')
        if rv_uri:
            mcli = _MC(rv_uri)
            cursor = mcli[rv_db][rv_coll].aggregate([
                {"$match": {"language": {"$ne": None}}},
                {"$group": {"_id": {"appid": "$appid", "language": {"$toLower": "$language"}}, "n": {"$sum": 1}}},
            ], allowDiskUse=True)
            rows = list(cursor)
            try:
                mcli.close()
            except Exception:
                pass
            if rows:
                import pandas as _pd
                pdf_lang = _pd.DataFrame(rows)
                pdf_lang['appid'] = pdf_lang['_id'].apply(lambda x: str(x.get('appid')))
                pdf_lang['language'] = pdf_lang['_id'].apply(lambda x: str(x.get('language') or ''))
                pdf_lang = pdf_lang[['appid','language','n']]
                # Agregados EN/ES/OTROS
                def _is_es(s: str) -> bool:
                    s = (s or '').lower()
                    return s.startswith('spanish') or s.startswith('españ')
                def _is_en(s: str) -> bool:
                    s = (s or '').lower()
                    return s.startswith('english')
                grp = pdf_lang.groupby('appid')
                agg_rows = []
                for aid, g in grp:
                    n_en = int(g[g['language'].map(_is_en)]['n'].sum())
                    n_es = int(g[g['language'].map(_is_es)]['n'].sum())
                    n_tot = int(g['n'].sum())
                    n_other = max(0, n_tot - n_en - n_es)
                    pct_es = (n_es / n_tot) if n_tot else None
                    pct_other = (n_other / n_tot) if n_tot else None
                    agg_rows.append({
                        'appid': aid,
                        'reviews_en': n_en,
                        'reviews_es': n_es,
                        'reviews_other': n_other,
                        'pct_es': pct_es,
                        'pct_other': pct_other,
                    })
                sdf_lang = spark.createDataFrame(_pd.DataFrame(agg_rows))
                df = df.join(sdf_lang, on='appid', how='left')
    except Exception as e:
        print(f"[WARN] No se pudieron calcular agregados por idioma: {e}")

    # 4) Agregados por clúster (medianas, p75, vecinos, edad, lanzamientos recientes)
    if 'cluster_id' in df.columns:
        w_clu = Window.partitionBy('cluster_id')
        # price, install_size_gb, ram_gb, platforms_count pueden no existir
        def pct_approx(colname, p):
            return F.expr(f"percentile_approx({colname}, {p})")

        if 'price' in df.columns:
            df = df.withColumn('median_price', pct_approx('price', 0.5).over(w_clu))
        if 'install_size_gb' in df.columns:
            df = df.withColumn('p75_install_size', pct_approx('install_size_gb', 0.75).over(w_clu))
        if 'ram_gb' in df.columns:
            df = df.withColumn('median_ram_gb', pct_approx('ram_gb', 0.5).over(w_clu))
        if 'platforms_count' in df.columns:
            df = df.withColumn('median_platforms', pct_approx('platforms_count', 0.5).over(w_clu))

        # vecinos en clúster
        df = df.withColumn('k_neighbors', F.count(F.lit(1)).over(w_clu) - F.lit(1))

        # edad del clúster (años) y lanzamientos recientes (≤ 12 meses)
        if 'release_date' in df.columns:
            dt = F.to_date('release_date')
            df = df.withColumn('release_date_dt', dt)
            df = df.withColumn('age_years', F.months_between(F.current_date(), F.col('release_date_dt'))/F.lit(12.0))
            df = df.withColumn('cluster_age', F.avg('age_years').over(w_clu))
            recent_flag = F.when(F.months_between(F.current_date(), F.col('release_date_dt')) <= 12, F.lit(1)).otherwise(F.lit(0))
            df = df.withColumn('is_recent_launch', recent_flag)
            df = df.withColumn('pct_recent_launches', F.avg('is_recent_launch').over(w_clu))

        # comparativas de verificación/conexión en el clúster
        if 'steam_deck_verified' in df.columns:
            df = df.withColumn('otros_verificados', F.avg(F.when(F.col('steam_deck_verified') == True, 1).otherwise(0)).over(w_clu) > 0.5)
        if 'requires_connection' in df.columns:
            df = df.withColumn('otros_requieren_conexion', F.avg(F.when(F.col('requires_connection') == True, 1).otherwise(0)).over(w_clu) > 0.5)

    # 4.b) Señales de actividad desde artefactos de analytics (opcional)
    try:
        evp = 'outputs/events/events.parquet'
        exp = 'outputs/events/explanations.parquet'
        from pathlib import Path as _P
        if _P(evp).exists():
            ev = spark.read.parquet(evp).withColumn('appid', F.col('appid').cast('string'))
            act = ev.groupBy('appid').agg((F.count(F.lit(1)) > 0).alias('activity_change'))
            df = df.join(act, on='appid', how='left')
        if _P(exp).exists():
            ex = spark.read.parquet(exp).withColumn('appid', F.col('appid').cast('string'))
            # Booleans de presencia
            has_twitch = F.max(F.when(F.col('twitch_spike') == True, 1).otherwise(0))
            yt_mentions = None
            if 'yt_mentions' in ex.columns:
                yt_mentions = F.sum(F.coalesce(F.col('yt_mentions').cast('int'), F.lit(0)))
            agg = ex.groupBy('appid').agg(
                has_twitch.alias('twitch_spike_any'),
                (F.sum(F.coalesce(F.col('news_patch').cast('int'), F.lit(0))) > 0).alias('patch_correlation')
            )
            if yt_mentions is not None:
                agg = agg.withColumnRenamed('twitch_spike_any', 'twitch_spike_any')
                # menciones_twitch_youtube = twitch OR yt > 0
                agg = agg.join(ex.groupBy('appid').agg(yt_mentions.alias('yt_sum')), on='appid', how='left')
                agg = agg.withColumn('twitch_mentions', (F.col('twitch_spike_any') == 1) | (F.col('yt_sum') > 0))
                agg = agg.drop('yt_sum')
            else:
                agg = agg.withColumn('twitch_mentions', F.col('twitch_spike_any') == 1)
            agg = agg.drop('twitch_spike_any')
            # f2p_switch no se deriva aquí (requiere fuente externa); lo dejamos nulo
            df = df.join(agg, on='appid', how='left')
            if 'f2p_switch' not in df.columns:
                df = df.withColumn('f2p_switch', F.lit(None).cast('boolean'))
    except Exception as e:
        print(f"[WARN] No se pudieron derivar señales de actividad: {e}")

    # 5) Campos esperados por reglas que quizás no existan: crearlos como nulos si faltan
    expected = [
        'median_price','median_hours','launch_price','std_dev','mean_score',
        'playtime_last_two_weeks','review_positive_ratio','review_neutral_ratio',
        'pct_publi_potente','pct_dev_potente','pct_ips','reviews_en','reviews_es','pct_es','reviews_other','pct_other',
        'steam_deck_verified','requires_connection','install_size_gb','platforms_count'
    ]
    for c in expected:
        if c not in df.columns:
            if c in ['steam_deck_verified','requires_connection']:
                df = df.withColumn(c, F.lit(None).cast('boolean'))
            else:
                df = df.withColumn(c, F.lit(None).cast('double'))

    # 6) Escribir salida
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
        # Campos numéricos crudos que pueden ser útiles aguas abajo
        hours_played = row.get("hours_played")
        median_hours = row.get("median_hours")
        hours_last_2w = row.get("hours_last_2w")
        abandoned_after_review = row.get("abandoned_after_review")
        review_positive = row.get("review_positive")

        # Derivados simples que queremos persistir
        try:
            playtime_ratio = float(hours_played) / float(median_hours) if hours_played is not None and median_hours not in (None, 0) else None
        except Exception:
            playtime_ratio = None

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
            "experiencia": experiencia_jugador(hours_played, median_hours),
            "abandono": deteccion_abandono(
                abandoned_after_review,
                hours_last_2w,
                review_positive,
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
        # Adjuntar métricas crudas y derivadas para consumo externo
        res.update({
            "hours_played": hours_played,
            "median_hours": median_hours,
            "hours_last_2w": hours_last_2w,
            "abandoned_after_review": abandoned_after_review,
            "review_positive": review_positive,
            "playtime_ratio": playtime_ratio,
        })

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
