#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Dict, Any, List

import numpy as np
import pandas as pd


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Plots for news classifier models (bars + heatmap)')
    ap.add_argument('--meta', default='models/news_best_meta.json', help='Path to best meta JSON (with candidates)')
    ap.add_argument('--out-dir', default='outputs/events', help='Output directory for plots')
    return ap.parse_args(list(argv) if argv is not None else None)


def _pick(metric_dict: Dict[str, Any], *keys: str) -> float | None:
    for k in keys:
        if k in metric_dict and metric_dict[k] is not None:
            try:
                return float(metric_dict[k])
            except Exception:
                continue
    return None


def build_df(candidates: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for c in candidates:
        name = c.get('name') or c.get('model')
        m = c.get('metrics') or {}
        f1_macro = _pick(m, 'cv_f1_macro', 'test_f1_macro', 'f1_macro')
        acc = _pick(m, 'cv_accuracy', 'test_accuracy', 'accuracy')
        f1_weighted = _pick(m, 'cv_f1_weighted', 'test_f1_weighted', 'f1_weighted')
        rows.append({'model': str(name), 'f1_macro': f1_macro, 'accuracy': acc, 'f1_weighted': f1_weighted})
    df = pd.DataFrame(rows)
    return df


def save_plots(df: pd.DataFrame, out_dir: Path) -> List[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []

    # Bars for f1_macro and accuracy
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 4.5))
    idx = np.arange(len(df))
    width = 0.35
    ax.bar(idx - width/2, df['f1_macro'].fillna(0), width, label='f1_macro')
    ax.bar(idx + width/2, df['accuracy'].fillna(0), width, label='accuracy')
    ax.set_xticks(idx)
    ax.set_xticklabels(df['model'], rotation=20, ha='right')
    ax.set_ylabel('Score')
    ax.set_title('Rendimiento por modelo (CV/Test)')
    ax.legend()
    ax.grid(True, axis='y', alpha=0.25)
    p1 = out_dir / 'news_models_bars.png'
    fig.tight_layout()
    fig.savefig(p1, dpi=150)
    plt.close(fig)
    paths.append(p1)

    # Heatmap models x metrics
    metrics = ['f1_macro', 'accuracy', 'f1_weighted']
    df_hm = df.set_index('model')[metrics].astype(float).fillna(0)
    fig2, ax2 = plt.subplots(figsize=(6.5, 0.5 + 0.5 * len(df_hm)))
    im = ax2.imshow(df_hm.values, aspect='auto', cmap='Blues', vmin=0, vmax=1)
    ax2.set_yticks(np.arange(len(df_hm)))
    ax2.set_yticklabels(df_hm.index.tolist())
    ax2.set_xticks(np.arange(len(metrics)))
    ax2.set_xticklabels(metrics)
    ax2.set_title('Heatmap modelos × métricas')
    for i in range(df_hm.shape[0]):
        for j in range(df_hm.shape[1]):
            ax2.text(j, i, f"{df_hm.iloc[i, j]:.2f}", ha='center', va='center', color='black')
    fig2.colorbar(im, ax=ax2, fraction=0.046, pad=0.04)
    p2 = out_dir / 'news_models_heatmap.png'
    fig2.tight_layout()
    fig2.savefig(p2, dpi=150)
    plt.close(fig2)
    paths.append(p2)

    # Save CSV summary
    p3 = out_dir / 'news_models_summary.csv'
    df.to_csv(p3, index=False)
    paths.append(p3)

    return paths


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    meta_path = Path(args.meta)
    if not meta_path.exists():
        raise SystemExit(f'Meta JSON no encontrado: {meta_path}')
    data = json.loads(meta_path.read_text(encoding='utf-8'))
    candidates = data.get('candidates') or []
    if not candidates:
        raise SystemExit('No hay candidatos en news_best_meta.json')
    df = build_df(candidates)
    out_dir = Path(args.out_dir)
    paths = save_plots(df, out_dir)
    print('[OK] Plots generados:', ', '.join(str(p) for p in paths))


if __name__ == '__main__':
    main()

