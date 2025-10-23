#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Genera un reporte global de calidad para CCF/Granger sin duplicados por serie.

Produce PNGs e index HTML con:
 - Histograma de p-valores ADF (series únicas por appid+serie)
 - Histograma de p-valores KPSS (series únicas por appid+serie)
 - Proporción de significancia Granger con FDR (XY/YX por par)
 - Porcentaje Ljung–Box OK (residuales AR(1) del predictor), agregado por appid

Entradas (por defecto busca en outputs/ccf_analysis o en un subdirectorio):
 - stationarity_tests.csv
 - summary.parquet

Ejemplo:
  python scripts/plot_ccf_quality.py \
    --base-dir outputs/ccf_analysis/subset_neighbors \
    --out-dir  outputs/ccf_analysis/subset_neighbors/quality
"""
from __future__ import annotations

import argparse
from pathlib import Path
import math
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt


def _load_stationarity_csv(base_dir: Path) -> Optional[pd.DataFrame]:
    for name in ("stationarity_tests.csv",):
        p = base_dir / name
        if p.exists():
            try:
                df = pd.read_csv(p)
                if not df.empty:
                    return df
            except Exception:
                pass
    return None


def _load_summary_parquet(base_dir: Path) -> Optional[pd.DataFrame]:
    p = base_dir / "summary.parquet"
    if not p.exists():
        return None
    try:
        return pq.read_table(p).to_pandas()
    except Exception:
        # Fallback por si pyarrow falla y pandas con engine default funciona
        try:
            return pd.read_parquet(p)
        except Exception:
            return None


def _prioritize_freq(df: pd.DataFrame) -> pd.DataFrame:
    if "freq" not in df.columns:
        return df
    rank = {"M": 0, "Q": 1}
    df = df.copy()
    df["_freq_rank"] = df["freq"].map(rank).fillna(0)
    # Mantener por appid+serie una sola fila (la más prioritaria)
    df = df.sort_values(["appid", "series", "_freq_rank"])\
           .drop_duplicates(["appid", "series"], keep="first")
    return df.drop(columns=["_freq_rank"], errors="ignore")


def _save_hist(values: pd.Series, title: str, out_png: Path) -> None:
    vals = pd.to_numeric(values, errors="coerce").dropna()
    plt.figure(figsize=(8, 4.5), dpi=120)
    plt.hist(vals, bins=30, color="#6C8AED", edgecolor="#3b5dd9")
    plt.title(title)
    plt.xlabel("p-valor")
    plt.ylabel("Conteo")
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def _save_bar(labels, values, title: str, out_png: Path, ylabel: str = "Proporción"):
    plt.figure(figsize=(6.5, 4.0), dpi=120)
    xs = np.arange(len(labels))
    plt.bar(xs, values, color=["#2f7ed8", "#8bbc21"]) if len(values) == 2 else plt.bar(xs, values)
    plt.xticks(xs, labels, rotation=0)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.ylim(0, max(1.0, max(values) * 1.1) if values else 1.0)
    plt.tight_layout()
    out_png.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out_png)
    plt.close()


def _write_index_html(out_dir: Path, entries: list[tuple[str, str]]):
    # entries: [(caption, filename)]
    html = [
        "<!doctype html>",
        "<meta charset='utf-8'>",
        "<title>CCF/Granger Quality Report</title>",
        "<style>body{font-family:system-ui,Segoe UI,Arial;margin:20px;} figure{margin:0 0 24px 0;} figcaption{margin:6px 0 12px 0; font-weight:600;} img{max-width:100%;height:auto;border:1px solid #ddd;padding:6px;border-radius:6px;}</style>",
        "<h1>CCF/Granger — Reporte de Calidad</h1>",
        "<p>Gráficas generadas sin duplicar series por appid+serie (selected).</p>",
    ]
    for caption, fname in entries:
        html += [
            "<figure>",
            f"<figcaption>{caption}</figcaption>",
            f"<img src='{fname}' loading='lazy'>",
            "</figure>",
        ]
    out = out_dir / "quality_report.html"
    out.write_text("\n".join(html), encoding="utf-8")
    print(f"[OK] HTML -> {out}")


def main():
    ap = argparse.ArgumentParser(description="Reporte global de calidad CCF/Granger (PNG+HTML)")
    ap.add_argument("--base-dir", default="outputs/ccf_analysis", help="Directorio con summary.parquet y stationarity_tests.csv")
    ap.add_argument("--out-dir", default=None, help="Directorio de salida (por defecto: base-dir)")
    args = ap.parse_args()

    base = Path(args.base_dir).resolve()
    out_dir = Path(args.out_dir).resolve() if args.out_dir else base
    out_dir.mkdir(parents=True, exist_ok=True)

    tests = _load_stationarity_csv(base)
    summary = _load_summary_parquet(base)

    entries: list[tuple[str, str]] = []

    # 1) ADF/KPSS sin duplicados por appid+serie (solo selected)
    if tests is not None and not tests.empty:
        sel = tests[tests.get("selected").fillna(False) == True].copy()
        if not sel.empty:
            if "appid" in sel.columns and "series" in sel.columns:
                sel["appid"] = sel["appid"].astype(str)
                sel = _prioritize_freq(sel)
                unique_series = sel.drop_duplicates(["appid", "series"])  # ya priorizado
            else:
                unique_series = sel

            if "p_adf" in unique_series.columns:
                p = out_dir / "adf_pvalues_hist.png"
                _save_hist(unique_series["p_adf"], "Distribución p-valores ADF (series únicas)", p)
                entries.append(("Distribución p-valores ADF (series únicas)", p.name))

            if "p_kpss" in unique_series.columns:
                p = out_dir / "kpss_pvalues_hist.png"
                _save_hist(unique_series["p_kpss"], "Distribución p-valores KPSS (series únicas)", p)
                entries.append(("Distribución p-valores KPSS (series únicas)", p.name))
        else:
            print("[WARN] stationarity_tests.csv sin filas selected=true; se omiten ADF/KPSS.")
    else:
        print("[WARN] No se encontró stationarity_tests.csv; se omiten ADF/KPSS.")

    # 2) Ljung–Box (predictor) y FDR (Granger) a partir de summary.parquet
    if summary is not None and not summary.empty:
        df = summary.copy()
        # Ljung: agregamos por appid (ok si todas true)
        lj = df.dropna(subset=["ljung_ok"]) if "ljung_ok" in df.columns else pd.DataFrame()
        if not lj.empty:
            lj_app = lj.groupby("appid")["ljung_ok"].agg(lambda s: bool(np.all(s))).reset_index()
            pct_ok = float(lj_app["ljung_ok"].mean()) if not lj_app.empty else 0.0
            p = out_dir / "ljung_ok_bar.png"
            _save_bar(["OK", "No OK"], [pct_ok, 1.0 - pct_ok], "Ljung–Box (residuales AR(1) predictor) por appid", p)
            entries.append((f"Ljung–Box OK por appid (OK={pct_ok*100:.1f}%)", p.name))

        # FDR Granger por par/dirección (proporciones globales)
        def _prop(col: str) -> float:
            if col not in df.columns:
                return float("nan")
            s = df[col].dropna()
            if s.empty:
                return float("nan")
            # s puede ser bool o 0/1
            try:
                return float(pd.to_numeric(s).mean())
            except Exception:
                return float((s == True).mean())

        xy = _prop("granger_xy_sig_fdr")
        yx = _prop("granger_yx_sig_fdr")
        if not (math.isnan(xy) and math.isnan(yx)):
            vals = [0.0 if math.isnan(xy) else xy, 0.0 if math.isnan(yx) else yx]
            p = out_dir / "granger_fdr_bars.png"
            _save_bar(["Granger X→Y (FDR)", "Granger Y→X (FDR)"], vals, "Significancia de Granger (FDR)", p)
            entries.append(("Significancia de Granger con FDR (global)", p.name))
    else:
        print("[WARN] No se encontró summary.parquet; se omiten Ljung/FDR.")

    # 3) HTML de índice
    _write_index_html(out_dir, entries)
    print(f"[OK] Reporte de calidad generado en -> {out_dir}")


if __name__ == "__main__":
    main()

