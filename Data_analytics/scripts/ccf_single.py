#!/usr/bin/env python
from __future__ import annotations

"""Helper para correr CCF/Granger de un único appid sin .bat ni quoting raro.

Uso dentro del contenedor (desde la raíz del repo o Data_analytics):

  python scripts/ccf_single.py --appid 281990

Opciones:
  --base-config configs/ccf_analysis.yaml
  --clusters-out data/processed/_tmp_single_app_clusters.parquet
  --config-out  configs/ccf_single.yaml
  --out-dir     outputs/ccf_analysis/single_{appid}

El script:
  1) Crea un parquet temporal con el appid indicado
  2) Genera un YAML derivado apuntando a ese parquet y a un out-dir específico
  3) Lanza analyze_competitors_ccf.py con ese YAML
"""

import argparse
from pathlib import Path
import subprocess
import sys
import yaml
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run CCF/Granger for a single appid")
    ap.add_argument("--appid", required=True, help="Steam appid (e.g., 281990)")
    ap.add_argument("--base-config", default="configs/ccf_analysis.yaml", help="Base YAML config")
    ap.add_argument("--clusters-out", default="data/processed/_tmp_single_app_clusters.parquet", help="Temp clusters parquet to write")
    ap.add_argument("--config-out", default="configs/ccf_single.yaml", help="Derived config YAML to write")
    ap.add_argument("--out-dir", default=None, help="Output dir for results (defaults to outputs/ccf_analysis/single_{appid})")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    appid = str(args.appid)

    # 1) Parquet temporal de clusters
    clusters_path = Path(args.clusters_out)
    clusters_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"appid": [appid], "cluster_id": ["0"]}).to_parquet(clusters_path, index=False)
    print(f"[OK] Wrote clusters parquet -> {clusters_path}")

    # 2) YAML derivado
    cfg_base = yaml.safe_load(open(args.base_config, "r", encoding="utf-8"))
    out_dir = args.out_dir or f"outputs/ccf_analysis/single_{appid}"
    cfg_base = dict(cfg_base or {})
    cfg_base.setdefault("input_path", {})
    cfg_base["input_path"]["clusters_parquet"] = str(clusters_path)
    cfg_base["output_dir"] = out_dir
    cfg_out_path = Path(args.config_out)
    cfg_out_path.parent.mkdir(parents=True, exist_ok=True)
    open(cfg_out_path, "w", encoding="utf-8").write(yaml.safe_dump(cfg_base, sort_keys=False, allow_unicode=True))
    print(f"[OK] Wrote derived config -> {cfg_out_path}")

    # 3) Ejecutar análisis
    cmd = [
        sys.executable,
        "src/pipelines/ccf_analysis/analyze_competitors_ccf.py",
        "--config",
        str(cfg_out_path),
    ]
    print("[INFO] Running:", " ".join(cmd))
    ret = subprocess.call(cmd)
    if ret != 0:
        sys.exit(ret)
    print(f"[DONE] Check results in: {out_dir}")


if __name__ == "__main__":
    main()

