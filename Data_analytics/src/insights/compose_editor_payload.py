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

    # Construir un resumen de relevancia si los tópicos tienen anotaciones
    def _relevance_summary(topics_rows):
        try:
            pol_counts = {}
            lbl_counts = {}
            negative_months = []
            high_months = []
            total = 0
            for r in topics_rows or []:
                pol = (r.get('relevance_polarity') or '').lower()
                lbl = (r.get('relevance_label') or r.get('relevance_label_final') or '').lower()
                ym = r.get('event_year_month') or r.get('year_month')
                if pol:
                    pol_counts[pol] = pol_counts.get(pol, 0) + 1
                if lbl:
                    lbl_counts[lbl] = lbl_counts.get(lbl, 0) + 1
                if pol == 'negative' and ym:
                    negative_months.append(ym)
                if lbl == 'high' and ym:
                    high_months.append(ym)
                total += 1
            negative_ratio = (pol_counts.get('negative', 0) / total) if total else 0.0
            return {
                'polarity_counts': pol_counts,
                'label_counts': lbl_counts,
                'negative_ratio': negative_ratio,
                'negative_months': negative_months[:12],
                'high_months': high_months[:12],
                'total_topic_rows': total,
            }
        except Exception:
            return None

    topics_rows = raw.get("topics", []) or []
    relevance_summary = _relevance_summary(topics_rows)

    payload = {
        "appid": raw.get("appid"),
        "name": raw.get("metadata", {}).get("name"),
        "cluster_id": raw.get("cluster", {}).get("cluster_id"),
        "neighbors": raw.get("neighbors", []),
        "resumen": {
            "events": raw.get("events", [])[:10],
            # Incluye campos de relevancia si existen (topics_scored)
            "topics": raw.get("topics", [])[:10],
            "ccf": raw.get("ccf_granger", [])[:10],
        },
        # Alertas de tópicos negativos si están presentes en el reporte
        "alerts": raw.get("alerts", [])[:10],
        "relevance_summary": relevance_summary,
        "rules": raw.get("rules_analysis", {}),
        "generated_at": raw.get("generated_at"),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Payload para editor -> {out_path}")


if __name__ == "__main__":
    main()
