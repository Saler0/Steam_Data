#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Motor de reglas de decisión para datos de Steam con Spark + MongoDB + MLflow.
Incluye las funciones:
- regla_precio
- justificacion_precio
- saturacion_cluster_1
- saturacion_cluster_2
- saturacion_cluster_3
- justificacion_actividad
- experiencia_jugador
- deteccion_abandono
- limitaciones_tecnicas
- evaluacion_limitaciones
- publishers_estudios
- prioridad_idiomas
- resena_EarlyAccess_Regalo

Modo de uso (ejemplo):
  python modeling/reglas_decision.py --rule precio \
    --mongo-uri "mongodb://user:pass@host:27017" --database steam --collection games \
    --output "reports/rules/precio" --mlflow-experiment "steam_reglas"

Notas:
- Asume que el conector Mongo para Spark está disponible en el entorno de ejecución
  (por ejemplo, --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0).
- Este script aplica **una** regla por ejecución (--rule ...). Define múltiples stages DVC,
  cada uno llamando a este script con su conjunto de parámetros y salida específica.
"""

from typing import Optional, Dict, List, Any
import argparse
import os

# Spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, BooleanType, IntegerType, ArrayType, MapType
)

# MLflow
import mlflow


# ======================================================================================
# Reglas puras (sin Spark)
# ======================================================================================

def regla_precio(
    precio: Optional[float],
    mediana_segmento: Optional[float],
    bajo_umbral: float = 0.10,
    alto_umbral: float = 0.10,  
    ) -> str:
    """
    - Si precio < mediana * (1 - bajo_umbral): "Juego considerado económico frente al mercado"
    - Si precio < mediana * (1 + alto_umbral): "Juego con precio alineado al estándar del segmento"
    - Si precio >= mediana * (1 + alto_umbral): "Precio superior al estándar → requiere justificación"
    """
    if precio is None or mediana_segmento is None:
        return "Datos insuficientes para evaluar precio"
    try:
        p = float(precio); m = float(mediana_segmento)
    except Exception:
        return "Datos insuficientes para evaluar precio"
    if m <= 0:
        return "Datos insuficientes para evaluar precio"

    if p < m * (1.0 - bajo_umbral):
        return "Juego considerado económico frente al mercado"
    if p < m * (1.0 + alto_umbral):
        return "Juego con precio alineado al estándar del segmento"
    return "Precio superior al estándar → requiere justificación"


def justificacion_precio(
    incluye_todo_sin_dlcs: Optional[bool],
    tiempo_jugado_medio: Optional[float],
    tiempo_jugado_medio_cluster: Optional[float],
    precio_actual: Optional[float],
    precio_lanzamiento_cluster: Optional[float],
    ) -> str:
    """
    Árbol de justificación cuando el precio es alto:
      1) ¿Incluye todo sin DLCs/microtransacciones? → "Precio más alto, pero justificado por contenido completo"
      2) ¿Tiempo jugado medio < tiempo jugado medio del clúster? → "Precio elevado con poco tiempo jugado"
      3) ¿Precio actual > precio lanzamiento clúster? → "Precio superior al de lanzamiento típico del clúster"
      4) Si no: "Sin estrategia de rebajas podría perder competitividad"
    """
    if incluye_todo_sin_dlcs is True:
        return "Precio más alto, pero justificado por contenido completo"

    try:
        if (tiempo_jugado_medio is not None and tiempo_jugado_medio_cluster is not None and
            float(tiempo_jugado_medio) < float(tiempo_jugado_medio_cluster)):
            return "Precio elevado con poco tiempo jugado"
    except Exception:
        pass

    try:
        if (precio_actual is not None and precio_lanzamiento_cluster is not None and
            float(precio_actual) > float(precio_lanzamiento_cluster)):
            return "Precio superior al de lanzamiento típico del clúster"
    except Exception:
        pass

    return "Sin estrategia de rebajas podría perder competitividad"


def saturacion_cluster_1(
    num_vecinos: Optional[int],
    edad_media_cluster: Optional[float],
    umbral_vecinos: int = 20,
    umbral_edad: float = 6.0
    ) -> str:
    """
    1) num_vecinos >= 20:
         - edad >= 6 → "Segmento muy saturado y envejecido"
         - else     → "Segmento poco saturado y envejecido"
    2) num_vecinos < 20:
         - edad >= 6 → "Segmento emergente"
         - else     → "Segmento muy saturado y reciente"
    """
    if num_vecinos is None or edad_media_cluster is None:
        return "Datos insuficientes para evaluar saturación del clúster"
    try:
        vecinos = int(num_vecinos); edad = float(edad_media_cluster)
    except Exception:
        return "Datos insuficientes para evaluar saturación del clúster"

    if vecinos >= umbral_vecinos:
        return ("Segmento muy saturado y envejecido"
                if edad >= umbral_edad else
                "Segmento poco saturado y envejecido")
    else:
        return ("Segmento emergente"
                if edad >= umbral_edad else
                "Segmento muy saturado y reciente")


def saturacion_cluster_2(
    total_reviews: Optional[int],
    desviacion: Optional[float],
    nota_media: Optional[float],
    umbral_reviews: int = 1000,
    umbral_desviacion: float = 0.15,
    umbral_nota: float = 0.70   
    ) -> str:
    """
    1) total_reviews < 1000 → "Tamaño de muestra bajo"
    2) desviacion > 0.15   → "Opiniones dispersas"
    3) nota_media < 0.70   → "Mala reputación histórica del segmento"
    4) else                → "Alta exigencia del segmento"
    """
    if total_reviews is None or desviacion is None or nota_media is None:
        return "Datos insuficientes para evaluar cluster (reviews/reputación)"
    try:
        reviews = int(total_reviews); desv = float(desviacion); nota = float(nota_media)
    except Exception:
        return "Datos inválidos para evaluar cluster"

    if reviews < umbral_reviews:
        return "Tamaño de muestra bajo"
    if desv > umbral_desviacion:
        return "Opiniones dispersas"
    if nota < umbral_nota:
        return "Mala reputación histórica del segmento"
    return "Alta exigencia del segmento"


def saturacion_cluster_3(
    vecinos_lanzamiento_proximo: Optional[float],
    playtime_last_two_weeks: Optional[float],
    reviews_positivas: Optional[float],
    reviews_neutrales: Optional[float],
    umbral_vecinos: float = 0.5,
    umbral_playtime: float = 0.0,
    umbral_reviews_positivas: float = 0.6,
    umbral_reviews_neutrales: float = 0.3   
    ) -> str:
    """
    Rama Sí (>=50% vecinos con lanzamiento próximo):
      - playtime == 0 → "Entorno inactivo"
      - playtime > 0:
         - pos >= 0.6 → "Alta expectativa"
         - neu >= 0.3 → "Expectativa moderada"
         - else       → "Expectativa baja"
    Rama No:
      - playtime == 0 → "Entorno dinámico"
      - playtime > 0:
         - pos >= 0.6 → "Alta expectativa identificada"
         - neu >= 0.3 → "Expectativa moderada"
         - else       → "Expectativa baja"
    """
    if (vecinos_lanzamiento_proximo is None or playtime_last_two_weeks is None or
        reviews_positivas is None or reviews_neutrales is None):
        return "Datos insuficientes para evaluar saturación cluster 3"
    try:
        vecinos = float(vecinos_lanzamiento_proximo)
        play = float(playtime_last_two_weeks)
        pos = float(reviews_positivas)
        neu = float(reviews_neutrales)
    except Exception:
        return "Datos inválidos para evaluar saturación cluster 3"

    if vecinos >= umbral_vecinos:
        if play <= umbral_playtime:
            return "Entorno inactivo"
        return ("Alta expectativa" if pos >= umbral_reviews_positivas
                else "Expectativa moderada" if neu >= umbral_reviews_neutrales
                else "Expectativa baja")
    else:
        if play <= umbral_playtime:
            return "Entorno dinámico"
        return ("Alta expectativa identificada" if pos >= umbral_reviews_positivas
                else "Expectativa moderada" if neu >= umbral_reviews_neutrales
                else "Expectativa baja")


def justificacion_actividad(
    cambios_significativos: Optional[bool],
    menciones_twitch_youtube: Optional[bool],
    correlacion_parches_rebajas: Optional[bool],
    cambio_modelo: Optional[bool],
    ) -> str:
    """
    - No cambios → "Período de estabilidad"
    - Sí cambios:
       - menciones ↑ o correlación con parches/rebajas o cambio de modelo → "Cambios positivos explicados → crecimiento esperado"
       - else → "Sin causa clara → investigar más o revisar datos"
    """
    if cambios_significativos is None:
        return "Datos insuficientes para evaluar cambios en actividad"
    if not cambios_significativos:
        return "Período de estabilidad"
    if menciones_twitch_youtube or correlacion_parches_rebajas or cambio_modelo:
        return "Cambios positivos explicados → crecimiento esperado"
    return "Sin causa clara → investigar más o revisar datos"


def experiencia_jugador(
    horas: Optional[float],
    mediana_horas: Optional[float]
    ) -> str:
    """
    - horas ≤ 0.1 * mediana → "Nuevo"
    - horas ≤ 0.5 * mediana → "Intermedio"
    - horas ≤ 1.5 * mediana → "Experto"
    - else                  → "Veterano"
    """
    if horas is None or mediana_horas is None:
        return "Datos insuficientes para evaluar experiencia del jugador"
    try:
        h = float(horas); m = float(mediana_horas)
    except Exception:
        return "Datos inválidos para evaluar experiencia del jugador"
    if m <= 0:
        return "Datos inválidos (mediana no puede ser <= 0)"

    if h <= 0.1 * m:
        return "Nuevo"
    elif h <= 0.5 * m:
        return "Intermedio"
    elif h <= 1.5 * m:
        return "Experto"
    else:
        return "Veterano"


def deteccion_abandono(
    abandono_post_review: Optional[bool],
    ultimas_2_semanas: Optional[float],
    resena_positiva: Optional[bool]
    ) -> str:
    """
    - NO abandono tras reseña:
        0h → "Jugador inactivo"; <2h → "Jugador poco activo";
        <10h → "Jugador activo ocasional"; <30h → "Jugador activo frecuente";
        else → "Jugador muy activo"
    - SÍ abandono tras reseña:
        positiva → "Motivo de abandono ajeno al juego"
        negativa → "Abandono por reseña confirmado"
    """
    if abandono_post_review is None:
        return "Datos insuficientes para evaluar abandono"

    if not abandono_post_review:
        if ultimas_2_semanas is None:
            return "Datos insuficientes para evaluar actividad reciente"
        try:
            h = float(ultimas_2_semanas)
        except Exception:
            return "Datos inválidos para actividad reciente"
        if h == 0:
            return "Jugador inactivo"
        elif h < 2:
            return "Jugador poco activo"
        elif h < 10:
            return "Jugador activo ocasional"
        elif h < 30:
            return "Jugador activo frecuente"
        else:
            return "Jugador muy activo"
    else:
        if resena_positiva is None:
            return "Datos insuficientes para evaluar abandono post-reseña"
        return ("Motivo de abandono ajeno al juego"
                if resena_positiva else "Abandono por reseña confirmado")


def limitaciones_tecnicas(
    ram_juego: Optional[float],
    ram_mediana_cluster: Optional[float],
    plataformas_juego: Optional[int],
    plataformas_cluster_mayoria: Optional[int],
    verificado_steam_deck: Optional[bool],
    otros_verificados: Optional[bool],
    requiere_conexion: Optional[bool],
    otros_requieren_conexion: Optional[bool],
    tamano_instalacion: Optional[float],
    p75_tamano_cluster: Optional[float]
    ) -> Dict[str, bool]:
    """
    Devuelve flags:
      - ram_superior
      - soporte_limitado_plataformas
      - sin_verificacion_steam_deck
      - dependencia_conexion
      - juego_pesado
    """
    d: Dict[str, bool] = {}

    d["ram_superior"] = (ram_juego is not None and ram_mediana_cluster is not None
                         and float(ram_juego) > float(ram_mediana_cluster))

    if plataformas_juego is not None and plataformas_cluster_mayoria is not None:
        d["soporte_limitado_plataformas"] = int(plataformas_juego) < int(plataformas_cluster_mayoria)
    else:
        d["soporte_limitado_plataformas"] = False

    if verificado_steam_deck is not None and otros_verificados is not None:
        d["sin_verificacion_steam_deck"] = (not bool(verificado_steam_deck)) and bool(otros_verificados)
    else:
        d["sin_verificacion_steam_deck"] = False

    if requiere_conexion is not None and otros_requieren_conexion is not None:
        d["dependencia_conexion"] = bool(requiere_conexion) and (not bool(otros_requieren_conexion))
    else:
        d["dependencia_conexion"] = False

    d["juego_pesado"] = (tamano_instalacion is not None and p75_tamano_cluster is not None
                         and float(tamano_instalacion) > float(p75_tamano_cluster))

    return d


def evaluacion_limitaciones(
    d: Dict[str, bool]) -> str:
    total = int(sum(bool(v) for v in d.values()))
    if total == 0:
        return "Juego optimizado y competitivo técnicamente"
    elif total <= 2:
        return "Juego competitivo, pero con 1-2 debilidades técnicas a vigilar"
    else:
        return "Varias limitaciones técnicas detectadas → riesgo competitivo elevado"


def publishers_estudios(
    pct_publi_potente: Optional[float],
    pct_dev_potente: Optional[float],
    pct_ips_famosas: Optional[float],
    nota_media: Optional[float] 
    ) -> List[str]:
    res: List[str] = []
    if pct_publi_potente is not None and float(pct_publi_potente) >= 0.5:
        res.append("Segmento dominado por grandes editoras")
    if pct_dev_potente is not None and float(pct_dev_potente) >= 0.5:
        res.append("Segmento dominado por estudios reconocidos")
    if pct_ips_famosas is not None and float(pct_ips_famosas) >= 0.3:
        res.append("Alta visibilidad por IPs reconocidas")
    if nota_media is not None and float(nota_media) >= 80:
        res.append("Compiten con estándares de calidad altos")
    if not res:
        res.append("Sin características dominantes de publishers/estudios")
    return res


def prioridad_idiomas(
    reviews_ingles: Optional[int],
    reviews_espanol: Optional[int],
    total_reviews: Optional[int],
    otros_idiomas: Optional[Dict[str, int]] = None
    ) -> List[str]:
    res: List[str] = []
    if reviews_ingles is not None and int(reviews_ingles) >= 1000:
        res.append("Inglés obligatorio: base del mercado global")

    if (reviews_espanol is not None and total_reviews is not None and
        (int(reviews_espanol) >= 1000 or
         (int(reviews_espanol) >= 300 and int(total_reviews) > 0 and
          (int(reviews_espanol) / int(total_reviews)) >= 0.20))):
        res.append("Español prioritario: volumen o comunidad significativa")

    if otros_idiomas and total_reviews and int(total_reviews) > 0:
        for idioma, n_reviews in otros_idiomas.items():
            try:
                n = int(n_reviews)
                if n >= 1000 or (n >= 300 and (n / int(total_reviews)) >= 0.15):
                    res.append(f"Idioma adicional recomendado ({idioma})")
            except Exception:
                continue

    if not res:
        res.append("Sin idiomas prioritarios detectados")
    return res


def resena_EarlyAccess_Regalo(
    fue_early_access: Optional[bool],
    fue_regalado: Optional[bool] 
    ) -> List[str]:
    res: List[str] = []
    if fue_early_access:
        res.append("Early Access")
    if fue_regalado:
        res.append("Regalado")
    if not res:
        res.append("Reseña estándar")
    return res


# ======================================================================================
# Infra: Spark + Mongo + MLflow
# ======================================================================================

def build_spark(app_name: str = "reglas_decision") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def leer_desde_mongo(
    spark: SparkSession,
    mongo_uri: str,
    database: str,
    collection: str,
) -> DataFrame:
    return (
        spark.read.format("mongodb")
        .option("uri", mongo_uri)
        .option("database", database)
        .option("collection", collection)
        .load()
    )


def write_with_summary_and_log(
    df: DataFrame,
    output_path: str,
    key_col: str,
    decision_col: str,
    mlflow_prefix: str
) -> None:
    # persistir resultados (id + decision)
    (df.select(F.col(key_col).cast("string").alias("game_id"), F.col(decision_col))
       .write.mode("overwrite").parquet(output_path))

    # resumen de conteos
    cnt = df.groupBy(decision_col).count()
    cnt.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path + "_summary")

    # logging de métricas
    for row in cnt.collect():
        k = str(row[decision_col]) if row[decision_col] is not None else "NULL"
        mlflow.log_metric(f"{mlflow_prefix}__count__{k}", int(row["count"]))


# ======================================================================================
# Ejecutores por regla (envuelven las funciones puras con UDF y MLflow)
# ======================================================================================

def ejecutar_regla_precio(args: argparse.Namespace) -> None:
    spark = build_spark("regla_precio")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="regla_precio"):
        mlflow.log_params({
            "col_precio": args.col_precio,
            "col_mediana": args.col_mediana,
            "bajo_umbral": args.precio_bajo_umbral,
            "alto_umbral": args.precio_alto_umbral,
            "db": args.database, "coll": args.collection,
        })

        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda p, m: regla_precio(p, m, args.precio_bajo_umbral, args.precio_alto_umbral),
            StringType()
        )

        out = df.withColumn(
            "decision_precio",
            udf_dec(F.col(args.col_precio).cast(DoubleType()),
                    F.col(args.col_mediana).cast(DoubleType()))
        )

        write_with_summary_and_log(out, args.output, "_id", "decision_precio", "precio")
    spark.stop()


def ejecutar_justificacion_precio(args: argparse.Namespace) -> None:
    spark = build_spark("justificacion_precio")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="justificacion_precio"):
        mlflow.log_params({
            "col_incluye_todo": args.col_incluye_todo,
            "col_tiempo_medio": args.col_tiempo_medio,
            "col_tiempo_medio_cluster": args.col_tiempo_medio_cluster,
            "col_precio_actual": args.col_precio_actual,
            "col_precio_lanzamiento_cluster": args.col_precio_lanzamiento_cluster,
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda inc, t, tc, p, pc: justificacion_precio(inc, t, tc, p, pc),
            StringType()
        )

        out = df.withColumn(
            "justificacion_precio",
            udf_dec(F.col(args.col_incluye_todo).cast(BooleanType()),
                    F.col(args.col_tiempo_medio).cast(DoubleType()),
                    F.col(args.col_tiempo_medio_cluster).cast(DoubleType()),
                    F.col(args.col_precio_actual).cast(DoubleType()),
                    F.col(args.col_precio_lanzamiento_cluster).cast(DoubleType()))
        )

        write_with_summary_and_log(out, args.output, "_id", "justificacion_precio", "just_precio")
    spark.stop()


def ejecutar_saturacion_cluster_1(args: argparse.Namespace) -> None:
    spark = build_spark("saturacion_cluster_1")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="saturacion_cluster_1"):
        mlflow.log_params({
            "col_vecinos": args.col_num_vecinos,
            "col_edad": args.col_edad_media,
            "umbral_vecinos": args.umbral_vecinos,
            "umbral_edad": args.umbral_edad,
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda n, e: saturacion_cluster_1(n, e, args.umbral_vecinos, args.umbral_edad),
            StringType()
        )

        out = df.withColumn(
            "saturacion_cluster_1",
            udf_dec(F.col(args.col_num_vecinos).cast(IntegerType()),
                    F.col(args.col_edad_media).cast(DoubleType()))
        )
        write_with_summary_and_log(out, args.output, "_id", "saturacion_cluster_1", "sat1")
    spark.stop()


def ejecutar_saturacion_cluster_2(args: argparse.Namespace) -> None:
    spark = build_spark("saturacion_cluster_2")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="saturacion_cluster_2"):
        mlflow.log_params({
            "col_total_reviews": args.col_total_reviews,
            "col_desviacion": args.col_desviacion,
            "col_nota_media": args.col_nota_media,
            "umbral_reviews": args.umbral_reviews,
            "umbral_desviacion": args.umbral_desviacion,
            "umbral_nota": args.umbral_nota,
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda tr, d, n: saturacion_cluster_2(tr, d, n, args.umbral_reviews, args.umbral_desviacion, args.umbral_nota),
            StringType()
        )

        out = df.withColumn(
            "saturacion_cluster_2",
            udf_dec(F.col(args.col_total_reviews).cast(IntegerType()),
                    F.col(args.col_desviacion).cast(DoubleType()),
                    F.col(args.col_nota_media).cast(DoubleType()))
        )
        write_with_summary_and_log(out, args.output, "_id", "saturacion_cluster_2", "sat2")
    spark.stop()


def ejecutar_saturacion_cluster_3(args: argparse.Namespace) -> None:
    spark = build_spark("saturacion_cluster_3")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="saturacion_cluster_3"):
        mlflow.log_params({
            "cols": {
                "vecinos_lanzamiento": args.col_vecinos_lanzamiento,
                "playtime_2w": args.col_playtime_2w,
                "reviews_pos": args.col_reviews_pos,
                "reviews_neu": args.col_reviews_neu
            },
            "umbral_vecinos": args.umbral_vecinos_lanzamiento,
            "umbral_playtime": args.umbral_playtime_cero,
            "umbral_pos": args.umbral_reviews_pos,
            "umbral_neu": args.umbral_reviews_neu,
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda v, p, rp, rn: saturacion_cluster_3(
                v, p, rp, rn,
                args.umbral_vecinos_lanzamiento,
                args.umbral_playtime_cero,
                args.umbral_reviews_pos,
                args.umbral_reviews_neu
            ),
            StringType()
        )

        out = df.withColumn(
            "saturacion_cluster_3",
            udf_dec(F.col(args.col_vecinos_lanzamiento).cast(DoubleType()),
                    F.col(args.col_playtime_2w).cast(DoubleType()),
                    F.col(args.col_reviews_pos).cast(DoubleType()),
                    F.col(args.col_reviews_neu).cast(DoubleType()))
        )
        write_with_summary_and_log(out, args.output, "_id", "saturacion_cluster_3", "sat3")
    spark.stop()


def ejecutar_justificacion_actividad(args: argparse.Namespace) -> None:
    spark = build_spark("justificacion_actividad")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="justificacion_actividad"):
        mlflow.log_params({
            "cols": {
                "cambios": args.col_cambios_significativos,
                "menciones": args.col_menciones_streaming,
                "parches_rebajas": args.col_correlacion_parches_rebajas,
                "cambio_modelo": args.col_cambio_modelo
            },
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda c, m, pr, cm: justificacion_actividad(c, m, pr, cm),
            StringType()
        )

        out = df.withColumn(
            "justificacion_actividad",
            udf_dec(F.col(args.col_cambios_significativos).cast(BooleanType()),
                    F.col(args.col_menciones_streaming).cast(BooleanType()),
                    F.col(args.col_correlacion_parches_rebajas).cast(BooleanType()),
                    F.col(args.col_cambio_modelo).cast(BooleanType()))
        )
        write_with_summary_and_log(out, args.output, "_id", "justificacion_actividad", "just_act")
    spark.stop()


def ejecutar_experiencia_jugador(args: argparse.Namespace) -> None:
    spark = build_spark("experiencia_jugador")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="experiencia_jugador"):
        mlflow.log_params({
            "col_horas": args.col_horas_total,
            "col_mediana_horas": args.col_mediana_horas,
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda h, m: experiencia_jugador(h, m),
            StringType()
        )

        out = df.withColumn(
            "experiencia_jugador",
            udf_dec(F.col(args.col_horas_total).cast(DoubleType()),
                    F.col(args.col_mediana_horas).cast(DoubleType()))
        )
        write_with_summary_and_log(out, args.output, "_id", "experiencia_jugador", "exp")
    spark.stop()


def ejecutar_deteccion_abandono(args: argparse.Namespace) -> None:
    spark = build_spark("deteccion_abandono")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="deteccion_abandono"):
        mlflow.log_params({
            "cols": {
                "abandono_post_review": args.col_abandono_post_review,
                "ultimas_2w": args.col_ultimas_2w,
                "resena_pos": args.col_resena_positiva
            },
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_dec = F.udf(
            lambda a, h, rp: deteccion_abandono(a, h, rp),
            StringType()
        )

        out = df.withColumn(
            "deteccion_abandono",
            udf_dec(F.col(args.col_abandono_post_review).cast(BooleanType()),
                    F.col(args.col_ultimas_2w).cast(DoubleType()),
                    F.col(args.col_resena_positiva).cast(BooleanType()))
        )
        write_with_summary_and_log(out, args.output, "_id", "deteccion_abandono", "abandono")
    spark.stop()


def ejecutar_limitaciones_tecnicas(args: argparse.Namespace) -> None:
    spark = build_spark("limitaciones_tecnicas")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="limitaciones_tecnicas"):
        mlflow.log_params({
            "cols": {
                "ram_juego": args.col_ram_juego,
                "ram_mediana_cluster": args.col_ram_mediana_cluster,
                "plataformas_juego": args.col_plataformas_juego,
                "plataformas_cluster_mayoria": args.col_plataformas_cluster_mayoria,
                "steam_deck_ok": args.col_steam_deck_ok,
                "otros_steam_deck_ok": args.col_otros_steam_deck_ok,
                "requiere_conexion": args.col_requiere_conexion,
                "otros_requieren_conexion": args.col_otros_requieren_conexion,
                "tamano_instalacion": args.col_tamano_instalacion,
                "p75_cluster": args.col_p75_tamano_cluster
            },
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        # UDF que devuelve un map<string,bool> y además el resumen
        def _limit_map(
            rj, rmc, pj, pcm, sdo, od, rc, orc, ti, p75
        ) -> Dict[str, bool]:
            return limitaciones_tecnicas(rj, rmc, pj, pcm, sdo, od, rc, orc, ti, p75)

        def _limit_eval(d: Dict[str, bool]) -> str:
            if d is None:
                return "Sin información suficiente"
            return evaluacion_limitaciones(d)

        udf_map = F.udf(_limit_map, MapType(StringType(), BooleanType()))
        udf_eval = F.udf(_limit_eval, StringType())

        out = (df
               .withColumn("limitaciones_map", udf_map(
                   F.col(args.col_ram_juego).cast(DoubleType()),
                   F.col(args.col_ram_mediana_cluster).cast(DoubleType()),
                   F.col(args.col_plataformas_juego).cast(IntegerType()),
                   F.col(args.col_plataformas_cluster_mayoria).cast(IntegerType()),
                   F.col(args.col_steam_deck_ok).cast(BooleanType()),
                   F.col(args.col_otros_steam_deck_ok).cast(BooleanType()),
                   F.col(args.col_requiere_conexion).cast(BooleanType()),
                   F.col(args.col_otros_requieren_conexion).cast(BooleanType()),
                   F.col(args.col_tamano_instalacion).cast(DoubleType()),
                   F.col(args.col_p75_tamano_cluster).cast(DoubleType())
               ))
               .withColumn("limitaciones_eval", udf_eval(F.col("limitaciones_map"))))

        # Persistimos ambas cosas: mapa y evaluación resumen
        (out.select(F.col("_id").cast("string").alias("game_id"),
                    "limitaciones_eval", "limitaciones_map")
         .write.mode("overwrite").parquet(args.output))

        # resumen por la evaluación global
        cnt = out.groupBy("limitaciones_eval").count()
        cnt.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "_summary")
        for row in cnt.collect():
            k = str(row["limitaciones_eval"]) if row["limitaciones_eval"] is not None else "NULL"
            mlflow.log_metric(f"limit__count__{k}", int(row["count"]))
    spark.stop()


def ejecutar_publishers_estudios(args: argparse.Namespace) -> None:
    spark = build_spark("publishers_estudios")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="publishers_estudios"):
        mlflow.log_params({
            "cols": {
                "pct_publi": args.col_pct_publi_potente,
                "pct_dev": args.col_pct_dev_potente,
                "pct_ips": args.col_pct_ips_famosas,
                "nota_media": args.col_nota_media_estudios
            },
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_arr = F.udf(
            lambda ppub, pdev, pips, nota: publishers_estudios(ppub, pdev, pips, nota),
            ArrayType(StringType())
        )

        out = df.withColumn(
            "publishers_estudios",
            udf_arr(F.col(args.col_pct_publi_potente).cast(DoubleType()),
                    F.col(args.col_pct_dev_potente).cast(DoubleType()),
                    F.col(args.col_pct_ips_famosas).cast(DoubleType()),
                    F.col(args.col_nota_media_estudios).cast(DoubleType()))
        )

        # Guardamos como: id, publishers_estudios (array<string>)
        (out.select(F.col("_id").cast("string").alias("game_id"),
                    F.col("publishers_estudios"))
         .write.mode("overwrite").parquet(args.output))

        # Métricas: contamos ocurrencias de cada etiqueta
        exploded = out.select(F.explode(F.col("publishers_estudios")).alias("tag"))
        cnt = exploded.groupBy("tag").count()
        cnt.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "_summary")
        for row in cnt.collect():
            mlflow.log_metric(f"pubest__count__{row['tag']}", int(row["count"]))
    spark.stop()


def ejecutar_prioridad_idiomas(args: argparse.Namespace) -> None:
    spark = build_spark("prioridad_idiomas")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="prioridad_idiomas"):
        mlflow.log_params({
            "cols": {
                "eng": args.col_reviews_ingles,
                "spa": args.col_reviews_espanol,
                "total": args.col_total_reviews,
                "otros": args.col_otros_idiomas_map
            },
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        # otros_idiomas es un map<string,int> en el DF
        udf_arr = F.udf(
            lambda en, es, tot, otros: prioridad_idiomas(en, es, tot, otros),
            ArrayType(StringType())
        )

        out = df.withColumn(
            "prioridad_idiomas",
            udf_arr(F.col(args.col_reviews_ingles).cast(IntegerType()),
                    F.col(args.col_reviews_espanol).cast(IntegerType()),
                    F.col(args.col_total_reviews).cast(IntegerType()),
                    F.col(args.col_otros_idiomas_map).cast(MapType(StringType(), IntegerType())))
        )

        (out.select(F.col("_id").cast("string").alias("game_id"),
                    F.col("prioridad_idiomas"))
         .write.mode("overwrite").parquet(args.output))

        exploded = out.select(F.explode(F.col("prioridad_idiomas")).alias("tag"))
        cnt = exploded.groupBy("tag").count()
        cnt.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "_summary")
        for row in cnt.collect():
            mlflow.log_metric(f"langprio__count__{row['tag']}", int(row["count"]))
    spark.stop()


def ejecutar_resena_early_regalo(args: argparse.Namespace) -> None:
    spark = build_spark("resena_early_regalo")
    mlflow.set_experiment(args.mlflow_experiment)

    with mlflow.start_run(run_name="resena_early_regalo"):
        mlflow.log_params({
            "cols": {
                "early": args.col_fue_early_access,
                "regalo": args.col_fue_regalado
            },
            "db": args.database, "coll": args.collection,
        })
        df = leer_desde_mongo(spark, args.mongo_uri, args.database, args.collection)

        udf_arr = F.udf(
            lambda e, r: resena_EarlyAccess_Regalo(e, r),
            ArrayType(StringType())
        )

        out = df.withColumn(
            "resena_flags",
            udf_arr(F.col(args.col_fue_early_access).cast(BooleanType()),
                    F.col(args.col_fue_regalado).cast(BooleanType()))
        )

        (out.select(F.col("_id").cast("string").alias("game_id"),
                    F.col("resena_flags"))
         .write.mode("overwrite").parquet(args.output))

        exploded = out.select(F.explode(F.col("resena_flags")).alias("flag"))
        cnt = exploded.groupBy("flag").count()
        cnt.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output + "_summary")
        for row in cnt.collect():
            mlflow.log_metric(f"resena__count__{row['flag']}", int(row["count"]))
    spark.stop()


# ======================================================================================
# CLI
# ======================================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Motor de reglas de decisión (Spark + MongoDB + MLflow)")

    # Fuente de datos
    p.add_argument("--mongo-uri", type=str, required=True)
    p.add_argument("--database", type=str, required=True)
    p.add_argument("--collection", type=str, required=True)

    # Destino
    p.add_argument("--output", type=str, required=True)

    # Selección de regla
    p.add_argument("--rule", type=str, required=True, choices=[
        "precio", "justificacion_precio",
        "saturacion_cluster_1", "saturacion_cluster_2", "saturacion_cluster_3",
        "justificacion_actividad",
        "experiencia_jugador", "deteccion_abandono",
        "limitaciones_tecnicas",
        "publishers_estudios", "prioridad_idiomas",
        "resena_early_regalo"
    ])

    # MLflow
    p.add_argument("--mlflow-experiment", type=str, default="steam_reglas")

    # ---------- Parámetros / columnas por regla ----------
    # precio
    p.add_argument("--col-precio", dest="col_precio", default="price_final")
    p.add_argument("--col-mediana", dest="col_mediana", default="median_price_segment")
    p.add_argument("--precio-bajo-umbral", dest="precio_bajo_umbral", type=float, default=0.10)
    p.add_argument("--precio-alto-umbral", dest="precio_alto_umbral", type=float, default=0.10)

    # justificacion_precio
    p.add_argument("--col-incluye-todo", dest="col_incluye_todo", default="includes_all_content")
    p.add_argument("--col-tiempo-medio", dest="col_tiempo_medio", default="avg_playtime")
    p.add_argument("--col-tiempo-medio-cluster", dest="col_tiempo_medio_cluster", default="cluster_avg_playtime")
    p.add_argument("--col-precio-actual", dest="col_precio_actual", default="price_final")
    p.add_argument("--col-precio-lanzamiento-cluster", dest="col_precio_lanzamiento_cluster", default="cluster_launch_price")

    # saturacion_cluster_1
    p.add_argument("--col-num-vecinos", dest="col_num_vecinos", default="k_neighbors")
    p.add_argument("--col-edad-media", dest="col_edad_media", default="cluster_age_years")
    p.add_argument("--umbral-vecinos", dest="umbral_vecinos", type=int, default=20)
    p.add_argument("--umbral-edad", dest="umbral_edad", type=float, default=6.0)

    # saturacion_cluster_2
    p.add_argument("--col-total-reviews", dest="col_total_reviews", default="cluster_reviews_total")
    p.add_argument("--col-desviacion", dest="col_desviacion", default="cluster_reviews_std_ratio")
    p.add_argument("--col-nota-media", dest="col_nota_media", default="cluster_score_ratio")
    p.add_argument("--umbral-reviews", dest="umbral_reviews", type=int, default=1000)
    p.add_argument("--umbral-desviacion", dest="umbral_desviacion", type=float, default=0.15)
    p.add_argument("--umbral-nota", dest="umbral_nota", type=float, default=0.70)

    # saturacion_cluster_3
    p.add_argument("--col-vecinos-lanzamiento", dest="col_vecinos_lanzamiento", default="ratio_neighbors_comingsoon")
    p.add_argument("--col-playtime-2w", dest="col_playtime_2w", default="playtime_last_two_weeks")
    p.add_argument("--col-reviews-pos", dest="col_reviews_pos", default="ratio_positive_reviews")
    p.add_argument("--col-reviews-neu", dest="col_reviews_neu", default="ratio_neutral_reviews")
    p.add_argument("--umbral-vecinos-lanzamiento", dest="umbral_vecinos_lanzamiento", type=float, default=0.5)
    p.add_argument("--umbral-playtime-cero", dest="umbral_playtime_cero", type=float, default=0.0)
    p.add_argument("--umbral-reviews-pos", dest="umbral_reviews_pos", type=float, default=0.6)
    p.add_argument("--umbral-reviews-neu", dest="umbral_reviews_neu", type=float, default=0.3)

    # justificacion_actividad
    p.add_argument("--col-cambios-significativos", dest="col_cambios_significativos", default="activity_significant_change")
    p.add_argument("--col-menciones-streaming", dest="col_menciones_streaming", default="mentions_streaming_increase")
    p.add_argument("--col-correlacion-parches-rebajas", dest="col_correlacion_parches_rebajas", default="correlated_patches_discounts")
    p.add_argument("--col-cambio-modelo", dest="col_cambio_modelo", default="business_model_change")

    # experiencia_jugador
    p.add_argument("--col-horas-total", dest="col_horas_total", default="total_playtime_hours")
    p.add_argument("--col-mediana-horas", dest="col_mediana_horas", default="cluster_median_playtime_hours")

    # deteccion_abandono
    p.add_argument("--col-abandono-post-review", dest="col_abandono_post_review", default="abandoned_after_review")
    p.add_argument("--col-ultimas-2w", dest="col_ultimas_2w", default="playtime_last_two_weeks")
    p.add_argument("--col-resena-positiva", dest="col_resena_positiva", default="last_review_positive")

    # limitaciones_tecnicas
    p.add_argument("--col-ram-juego", dest="col_ram_juego", default="req_ram_gb")
    p.add_argument("--col-ram-mediana-cluster", dest="col_ram_mediana_cluster", default="cluster_median_ram_gb")
    p.add_argument("--col-plataformas-juego", dest="col_plataformas_juego", default="platforms_count")
    p.add_argument("--col-plataformas-cluster-mayoria", dest="col_plataformas_cluster_mayoria", default="cluster_mode_platforms_count")
    p.add_argument("--col-steam-deck-ok", dest="col_steam_deck_ok", default="steam_deck_verified")
    p.add_argument("--col-otros-steam-deck-ok", dest="col_otros_steam_deck_ok", default="cluster_others_steam_deck_verified")
    p.add_argument("--col-requiere-conexion", dest="col_requiere_conexion", default="always_online_required")
    p.add_argument("--col-otros-requieren-conexion", dest="col_otros_requieren_conexion", default="cluster_others_always_online")
    p.add_argument("--col-tamano-instalacion", dest="col_tamano_instalacion", default="install_size_gb")
    p.add_argument("--col-p75-tamano-cluster", dest="col_p75_tamano_cluster", default="cluster_p75_install_size_gb")

    # publishers_estudios
    p.add_argument("--col-pct-publi-potente", dest="col_pct_publi_potente", default="cluster_pct_big_publishers")
    p.add_argument("--col-pct-dev-potente", dest="col_pct_dev_potente", default="cluster_pct_top_devs")
    p.add_argument("--col-pct-ips-famosas", dest="col_pct_ips_famosas", default="cluster_pct_famous_ips")
    p.add_argument("--col-nota-media-estudios", dest="col_nota_media_estudios", default="cluster_avg_score")

    # prioridad_idiomas
    p.add_argument("--col-reviews-ingles", dest="col_reviews_ingles", default="reviews_en")
    p.add_argument("--col-reviews-espanol", dest="col_reviews_espanol", default="reviews_es")
    p.add_argument("--col-total-reviews", dest="col_total_reviews_global", default="reviews_total")
    p.add_argument("--col-otros-idiomas-map", dest="col_otros_idiomas_map", default="reviews_lang_map")

    # resena_EarlyAccess_Regalo
    p.add_argument("--col-fue-early-access", dest="col_fue_early_access", default="review_in_early_access")
    p.add_argument("--col-fue-regalado", dest="col_fue_regalado", default="game_was_gifted")

    return p.parse_args()


def main():
    args = parse_args()

    # Normaliza nombre de col para prioridad_idiomas (evita colisión con col_total_reviews usada en sat_cluster_2)
    args.col_total_reviews = getattr(args, "col_total_reviews_global", "reviews_total")

    # Ruteo por regla
    if args.rule == "precio":
        ejecutar_regla_precio(args)
    elif args.rule == "justificacion_precio":
        ejecutar_justificacion_precio(args)
    elif args.rule == "saturacion_cluster_1":
        ejecutar_saturacion_cluster_1(args)
    elif args.rule == "saturacion_cluster_2":
        ejecutar_saturacion_cluster_2(args)
    elif args.rule == "saturacion_cluster_3":
        ejecutar_saturacion_cluster_3(args)
    elif args.rule == "justificacion_actividad":
        ejecutar_justificacion_actividad(args)
    elif args.rule == "experiencia_jugador":
        ejecutar_experiencia_jugador(args)
    elif args.rule == "deteccion_abandono":
        ejecutar_deteccion_abandono(args)
    elif args.rule == "limitaciones_tecnicas":
        ejecutar_limitaciones_tecnicas(args)
    elif args.rule == "publishers_estudios":
        ejecutar_publishers_estudios(args)
    elif args.rule == "prioridad_idiomas":
        ejecutar_prioridad_idiomas(args)
    elif args.rule == "resena_early_regalo":
        ejecutar_resena_early_regalo(args)
    else:
        raise ValueError(f"Regla no soportada: {args.rule}")


if __name__ == "__main__":
    main()
