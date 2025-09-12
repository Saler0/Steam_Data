# src/utils/mlflow_utils.py

import mlflow
from typing import Any, Dict

def start_mlflow_run(experiment_name: str, run_name: str, tracking_uri: str = None):
    """
    Configura y comienza una nueva corrida de MLflow.
    
    Args:
        experiment_name (str): Nombre del experimento de MLflow.
        run_name (str): Nombre de la corrida específica.
        tracking_uri (str): URI del servidor de seguimiento de MLflow.
    """
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)
    
    mlflow.set_experiment(experiment_name)
    mlflow.start_run(run_name=run_name)
    print(f"[INFO] MLflow: Run '{run_name}' iniciado en experimento '{experiment_name}'.")

def log_mlflow_params(params: Dict[str, Any]):
    """Registra un diccionario de parámetros en la corrida actual de MLflow."""
    mlflow.log_params(params)
    print(f"[INFO] MLflow: {len(params)} parámetros registrados.")

def log_mlflow_metrics(metrics: Dict[str, float]):
    """Registra un diccionario de métricas en la corrida actual de MLflow."""
    for key, value in metrics.items():
        mlflow.log_metric(key, value)
    print(f"[INFO] MLflow: {len(metrics)} métricas registradas.")

def log_mlflow_artifacts(local_path: str, artifact_path: str = None):
    """
    Registra un archivo o directorio como un artefacto de la corrida de MLflow.
    
    Args:
        local_path (str): Ruta local del archivo o directorio a registrar.
        artifact_path (str): Ruta dentro de los artefactos de la corrida.
    """
    mlflow.log_artifact(local_path, artifact_path)
    print(f"[INFO] MLflow: Artefacto '{local_path}' registrado.")