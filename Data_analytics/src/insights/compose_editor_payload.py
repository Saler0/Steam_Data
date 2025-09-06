#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Compone un payload mínimo para un editor/renderer a partir del reporte por juego.
Lee outputs/reports/{appid}.json y escribe outputs/reports/{appid}_editor.json.
"""
import argparse
import json
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--appid", required=True)
    ap.add_argument("--reports_dir", default="outputs/reports")
    args = ap.parse_args()

    in_path = Path(args.reports_dir) / f"{args.appid}.json"
    out_path = Path(args.reports_dir) / f"{args.appid}_editor.json"

    if not in_path.exists():
        raise SystemExit(f"No se encontró el reporte: {in_path}")

    raw = json.loads(in_path.read_text(encoding="utf-8"))

    payload = {
        "appid": raw.get("appid"),
        "name": raw.get("metadata", {}).get("name"),
        "cluster_id": raw.get("cluster", {}).get("cluster_id"),
        "neighbors": raw.get("neighbors", []),
        "resumen": {
            "events": raw.get("events", [])[:10],
            "topics": raw.get("topics", [])[:10],
            "ccf": raw.get("ccf_granger", [])[:10],
        },
        "rules": raw.get("rules_analysis", {}),
        "generated_at": raw.get("generated_at"),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Payload para editor -> {out_path}")


if __name__ == "__main__":
    main()
