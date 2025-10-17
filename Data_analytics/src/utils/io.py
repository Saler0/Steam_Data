from __future__ import annotations
from pathlib import Path
from typing import Any
import pandas as pd

def _is_gcs(path: Any) -> bool:
    return isinstance(path, str) and path.startswith("gs://")

def path_exists(path: Any) -> bool:
    if _is_gcs(path):
        try:
            import fsspec
            fs = fsspec.filesystem("gcs")
            return fs.exists(path)
        except Exception:
            return False
    try:
        return Path(path).exists()
    except Exception:
        return False

def read_parquet_any(path: Any, **kwargs) -> pd.DataFrame:
    return pd.read_parquet(path, **kwargs)

def read_csv_any(path: Any, **kwargs) -> pd.DataFrame:
    return pd.read_csv(path, **kwargs)

def read_json_any(path: Any, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_json(path, **kwargs)
    except ValueError:
        kwargs = {**kwargs, "lines": True}
        return pd.read_json(path, **kwargs)


def is_gcs(path: Any) -> bool:
    return isinstance(path, str) and path.startswith("gs://")

def makedirs_if_local(path: Any):
    """Crea directorios SOLO si es local (para GCS no hace falta)."""
    if isinstance(path, str) and path.startswith("gs://"):
        return
    try:
        Path(path).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

def write_csv_any(df: pd.DataFrame, path: Any, **kwargs):
    return df.to_csv(path, **kwargs)

def write_parquet_any(df: pd.DataFrame, path: Any, **kwargs):
    return df.to_parquet(path, **kwargs)


def write_json_any(obj: Any, path: Any, **kwargs):
    import json
    # Encoder seguro: convierte tipos de NumPy y arrays a JSON
    def _default(o):
        try:
            import numpy as _np  # type: ignore
            if isinstance(o, _np.ndarray):
                return o.tolist()
            if isinstance(o, (_np.integer,)):
                return int(o)
            if isinstance(o, (_np.floating,)):
                return float(o)
            if isinstance(o, (_np.bool_,)):
                return bool(o)
        except Exception:
            pass
        if hasattr(o, 'item'):
            try:
                return o.item()
            except Exception:
                pass
        return str(o)

    dumps_kwargs = {"ensure_ascii": False, "default": _default}
    dumps_kwargs.update(kwargs or {})

    text = json.dumps(obj, **dumps_kwargs)
    if is_gcs(path):
        import fsspec
        with fsspec.open(path, 'w') as f:
            f.write(text)
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text, encoding='utf-8')
