# src/utils/spark_utils.py

from pyspark.sql import SparkSession
from pyspark import SparkConf

def get_spark_session(app_name: str, config: dict = None) -> SparkSession:
    """
    Crea y retorna una sesión de Spark.
    
    Args:
        app_name (str): Nombre de la aplicación Spark.
        config (dict): Diccionario de configuraciones adicionales para Spark.
        
    Returns:
        SparkSession: La sesión de Spark.
    """
    conf = SparkConf()
    if config:
        for k, v in config.items():
            conf.set(k, v)
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(conf=conf)
        .getOrCreate()
    )
    
    print(f"[INFO] Sesión de Spark '{app_name}' creada.")
    return spark