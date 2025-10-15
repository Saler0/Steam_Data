# src/utils/mlflow_utils.py

import mlflow
from typing import Any, Dict, Optional
from pathlib import Path
from datetime import datetime

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


def make_standard_run_name(prefix: str = "", script_path: Optional[str] = None, suffix: Optional[str] = None) -> str:
    """Genera un run_name estandarizado: {prefix}{script_name}_{YYYYMMDD_HHMM}[_{suffix}]"""
    script = Path(script_path).stem if script_path else "run"
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    parts = [f"{prefix}{script}_{ts}"]
    if suffix:
        parts.append(str(suffix))
    return "_".join(parts)


def set_standard_tags(script_path: Optional[str] = None, extra: Optional[Dict[str, Any]] = None) -> None:
    """Configura tags comunes: script, timestamp, y tags extra opcionales."""
    try:
        script = Path(script_path).stem if script_path else "run"
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        mlflow.set_tag("script", script)
        mlflow.set_tag("timestamp", ts)
        if extra:
            for k, v in extra.items():
                mlflow.set_tag(str(k), v if v is None or isinstance(v, (str, int, float, bool)) else str(v))
    except Exception:
        pass
