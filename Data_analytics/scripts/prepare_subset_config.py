#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Genera configs/events_subset.yaml y configs/ccf_subset.yaml apuntando
al parquet temporal de clusters de un subset (vecinos/appids).
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

import yaml


def main() -> None:
    ap = argparse.ArgumentParser(description="Prepara YAMLs de subset para events y CCF")
    ap.add_argument("--events", default="configs/events.yaml", help="YAML base de events")
    ap.add_argument("--ccf", default="configs/ccf_analysis.yaml", help="YAML base de CCF")
    ap.add_argument("--clusters", required=True, help="Ruta al parquet temporal de clusters del subset")
    ap.add_argument("--out-events", default="configs/events_subset.yaml")
    ap.add_argument("--out-ccf", default="configs/ccf_subset.yaml")
    args = ap.parse_args()

    clusters_parquet = args.clusters

    # events subset
    ev_path = Path(args.events)
    if not ev_path.exists():
        raise SystemExit(f"No existe YAML base de events: {ev_path}")
    ev_cfg = yaml.safe_load(ev_path.read_text(encoding="utf-8")) or {}
    ip = ev_cfg.get("input_paths") or {}
    ip["clusters_parquet"] = clusters_parquet
    ev_cfg["input_paths"] = ip
    ev_cfg["clusters_parquet"] = clusters_parquet
    Path(args.out_events).write_text(yaml.safe_dump(ev_cfg, sort_keys=False, allow_unicode=True), encoding="utf-8")

    # ccf subset
    ccf_path = Path(args.ccf)
    if not ccf_path.exists():
        raise SystemExit(f"No existe YAML base de CCF: {ccf_path}")
    ccf_cfg = yaml.safe_load(ccf_path.read_text(encoding="utf-8")) or {}
    ccf_in = ccf_cfg.get("input_path") or {}
    ccf_in["clusters_parquet"] = clusters_parquet
    ccf_cfg["input_path"] = ccf_in
    ccf_cfg["output_dir"] = "outputs/ccf_analysis/subset_neighbors"
    Path(args.out_ccf).write_text(yaml.safe_dump(ccf_cfg, sort_keys=False, allow_unicode=True), encoding="utf-8")

    print("[OK] events_subset.yaml y ccf_subset.yaml generados.")


if __name__ == "__main__":
    main()

