#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Anota la relevancia de tópicos por (appid, event_year_month) usando la máscara mensual
de consistencia CCF/Granger y, opcionalmente, la magnitud del evento de players.

Entradas:
- topics.parquet (o el generado por Spark/Ray con columnas: appid, event_year_month, topics)
- outputs/ccf_analysis/consistency.parquet (appid, pair_name, year_month, ccf_consistent, local_corr_3m, lead_or_lag)
- outputs/events/events.parquet (para traer zscore de players por mes)

Salida:
- outputs/events/topics_scored.parquet con columnas extra: ccf_consistent, local_corr_3m,
  players_zscore, players_growth_rate, lead_or_lag, relevance_label.
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import yaml

from src.utils.io import read_parquet_any, write_parquet_any


def label_relevance(has_polarity: bool, z: float | None, zthr_cfg: float, high_z: float) -> str:
    if not has_polarity:
        return 'low'
    if z is None or np.isnan(z):
        return 'medium'
    if abs(z) >= float(high_z):
        return 'high'
    if abs(z) >= float(zthr_cfg):
        return 'medium'
    return 'low'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='configs/events.yaml', help='Config YAML con rutas y zscore_threshold')
    ap.add_argument('--pairs', default='players_vs_positive_reviews,players_vs_negative_reviews',
                    help='Lista separada por comas de pares a considerar (como aparecen en consistency.parquet)')
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, 'r', encoding='utf-8'))
    out_dir = Path(cfg.get('output_dir', 'outputs/events'))
    out_dir.mkdir(parents=True, exist_ok=True)

    topics_path = out_dir / 'topics.parquet'
    events_path = out_dir / 'events.parquet'
    ccf_consistency_path = Path('outputs/ccf_analysis/consistency.parquet')

    if not topics_path.exists():
        raise SystemExit(f'No existe {topics_path}. Ejecuta la etapa de tópicos primero.')
    if not ccf_consistency_path.exists():
        raise SystemExit(f'No existe {ccf_consistency_path}. Ejecuta la etapa CCF con consistencia.')

    topics_df = read_parquet_any(topics_path)
    cons_df = read_parquet_any(ccf_consistency_path)
    cons_df['appid'] = cons_df['appid'].astype(str)

    # Si hay eventos, traer zscore y growth_rate de players
    players_df = pd.DataFrame()
    if events_path.exists():
        ev = read_parquet_any(events_path)
        if not ev.empty and 'variable' in ev.columns:
            players_df = (
                ev[ev['variable'] == 'players'][['appid', 'year_month', 'zscore', 'growth_rate']]
                .copy()
            )
            players_df['appid'] = players_df['appid'].astype(str)

    # Normalizar columnas de fechas
    for df, col in [(topics_df, 'event_year_month'), (cons_df, 'year_month')]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    # Preparar pivote de consistencia para múltiples pares
    pair_names = [p.strip() for p in str(args.pairs).split(',') if p.strip()]
    if not pair_names:
        raise SystemExit('Debe especificar al menos un par en --pairs')

    # Derivar alias de columnas por par
    def alias_from_pair(p: str) -> str:
        low = p.lower()
        if 'positive' in low:
            return 'pos'
        if 'negative' in low:
            return 'neg'
        return ''.join([ch if ch.isalnum() else '_' for ch in low])[:16]

    cons_use = cons_df[cons_df['pair_name'].isin(pair_names)].copy()
    cons_use['alias'] = cons_use['pair_name'].apply(alias_from_pair)

    # Armar tabla ancha por (appid, year_month)
    wide = None
    for key in cons_use['alias'].unique():
        sub = cons_use[cons_use['alias'] == key][['appid', 'year_month', 'ccf_consistent', 'local_corr_3m', 'lead_or_lag']]
        sub = sub.rename(columns={
            'ccf_consistent': f'ccf_consistent_{key}',
            'local_corr_3m': f'local_corr_3m_{key}',
            'lead_or_lag': f'lead_or_lag_{key}',
        })
        wide = sub if wide is None else wide.merge(sub, on=['appid', 'year_month'], how='outer')
    if wide is None:
        raise SystemExit('No hay filas en consistency.parquet para los pares solicitados')

    # Join por appid + mes
    merged = topics_df.copy()
    merged['appid'] = merged['appid'].astype(str)
    merged = merged.merge(
        wide,
        left_on=['appid', 'event_year_month'], right_on=['appid', 'year_month'], how='left'
    ).drop(columns=['year_month'], errors='ignore')

    if not players_df.empty:
        merged = merged.merge(
            players_df.rename(columns={'zscore': 'players_zscore', 'growth_rate': 'players_growth_rate'}),
            left_on=['appid', 'event_year_month'], right_on=['appid', 'year_month'], how='left'
        ).drop(columns=['year_month'], errors='ignore')
    else:
        merged['players_zscore'] = np.nan
        merged['players_growth_rate'] = np.nan

    # Calcular polaridad de relevancia en base a flags disponibles
    has_pos = f'ccf_consistent_pos' in merged.columns
    has_neg = f'ccf_consistent_neg' in merged.columns
    def polarity_row(row) -> str:
        pos = bool(row['ccf_consistent_pos']) if has_pos and pd.notna(row.get('ccf_consistent_pos')) else False
        neg = bool(row['ccf_consistent_neg']) if has_neg and pd.notna(row.get('ccf_consistent_neg')) else False
        if pos and neg:
            return 'mixed'
        if pos:
            return 'positive'
        if neg:
            return 'negative'
        return 'neutral'

    merged['relevance_polarity'] = merged.apply(polarity_row, axis=1)
    zthr = float(((cfg.get('detection') or {}).get('zscore_threshold') or 1.5))
    scoring_cfg = cfg.get('topics_scoring', {}) or {}
    high_z = float(scoring_cfg.get('high_z', 2.0))
    penalize_negative = bool(scoring_cfg.get('penalize_negative', True))
    penalize_mixed = bool(scoring_cfg.get('penalize_mixed', True))
    degrade_levels = int(scoring_cfg.get('degrade_levels', 1))

    merged['relevance_label_base'] = [
        label_relevance(pol != 'neutral', float(z) if pd.notna(z) else None, zthr, high_z)
        for pol, z in zip(merged['relevance_polarity'], merged.get('players_zscore', np.nan))
    ]

    # Penalización configurable por polaridad negativa o mixta
    def degrade(label: str, steps: int) -> str:
        order = ['low', 'medium', 'high']
        idx = order.index(label) if label in order else 0
        return order[max(0, idx - steps)]

    def maybe_penalize(lbl: str, pol: str) -> str:
        apply_penalty = (pol == 'negative' and penalize_negative) or (pol == 'mixed' and penalize_mixed)
        return degrade(lbl, degrade_levels) if apply_penalty else lbl

    merged['relevance_label_final'] = [
        maybe_penalize(lbl, pol) for lbl, pol in zip(merged['relevance_label_base'], merged['relevance_polarity'])
    ]
    merged['negative_alert'] = (merged['relevance_polarity'] == 'negative')
    # Alias estándar para consumidores: 'relevance_label' apunta al final
    merged['relevance_label'] = merged['relevance_label_final']

    out_path = out_dir / 'topics_scored.parquet'
    write_parquet_any(merged, out_path)
    print(f"[OK] Tópicos anotados con CCF en -> {out_path}")


if __name__ == '__main__':
    main()
