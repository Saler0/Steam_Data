#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Enriquece eventos con Twitch/YouTube/DLC/News/Topics, paralelizado con Ray.

Incluye utilidades mínimas `load_external_signals` y `enrich_group` para
combinar señales por appid/mes con los eventos detectados.
"""
import argparse
import yaml
from pathlib import Path
from typing import Iterable
import pandas as pd
import os
import sys

# Ensure project root is importable when running as a script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import mlflow
from src.utils.config_utils import expand_env_in_obj
try:
    import ray
    RAY_AVAILABLE = True
except Exception:
    RAY_AVAILABLE = False
from src.utils.io import read_parquet_any, write_parquet_any
from src.ingestion.twitch import load_twitch_monthly
from src.ingestion.youtube import load_youtube_monthly
from src.ingestion.dlcs import load_dlcs_for_game

def _enrich_events_for_game(appid: str, group: pd.DataFrame, cfg: dict,
                            game_name: str | None,
                            news_counts_df: pd.DataFrame | None = None,
                            topics_labels_df: pd.DataFrame | None = None,
                            news_kw_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Enriquece los eventos de un unico juego."""
    signals_cfg = cfg.get('signals', {})
    dlc_cfg = cfg.get('dlc', {})
    months_raw = pd.to_datetime(group['year_month'], errors='coerce').dropna().tolist()
    target_months: list = []
    # Contexto adicional alrededor de cada pico (por ej., ±1 mes)
    ctx_cfg = (cfg.get('signals') or {}).get('context_months', 0)
    try:
        context_months = int(ctx_cfg) if ctx_cfg is not None else 0
    except Exception:
        context_months = 0
    months_set = set()
    for ts in months_raw:
        ts = pd.Timestamp(ts)
        # Normalizar a mes (naive, sin tz) para consistencia
        if ts.tzinfo is not None:
            ts = ts.tz_convert('UTC').tz_localize(None)
        base = ts.to_period('M').to_timestamp()
        months_set.add(base)
        # Expandir margen ±N meses si está configurado
        for k in range(1, max(0, context_months) + 1):
            months_set.add((base - pd.DateOffset(months=k)).to_period('M').to_timestamp())
            months_set.add((base + pd.DateOffset(months=k)).to_period('M').to_timestamp())
    target_months = sorted(months_set)
    external_data = load_external_signals(
        appid,
        signals_cfg,
        dlc_cfg,
        game_name=game_name,
        target_months=target_months,
        news_counts_df=news_counts_df,
        topics_labels_df=topics_labels_df,
    )
    # Adjuntar keywords de noticias si existen
    if news_kw_df is not None and not news_kw_df.empty:
        subkw = news_kw_df[news_kw_df['appid'].astype(str) == str(appid)].copy()
        if not subkw.empty:
            subkw['year_month'] = pd.to_datetime(subkw['year_month'])
            ext_kw = subkw.set_index('year_month')
            ext_kw = ext_kw[['news_keywords','news_patch_keywords']].copy()
            external_data['news_kw'] = ext_kw

    explanations = enrich_group(group, external_data)
    return pd.DataFrame(explanations) if explanations else pd.DataFrame()


_enrich_events_for_game_ray = None
if RAY_AVAILABLE:
    _enrich_events_for_game_ray = ray.remote(_enrich_events_for_game)


def load_external_signals(appid: str, signals_cfg: dict, dlc_cfg: dict,
                          game_name: str | None = None,
                          target_months: Iterable | None = None,
                          news_counts_df: pd.DataFrame | None = None,
                          topics_labels_df: pd.DataFrame | None = None) -> dict:
    """Carga senales externas (Twitch, YouTube) y DLCs para un appid.

    Retorna un dict con dataframes indexados por year_month cuando aplica.
    """
    ext: dict = {}
    # Twitch
    tw_cfg = (signals_cfg or {}).get('twitch', {})
    tw = load_twitch_monthly(appid, tw_cfg, target_months=target_months, game_name=game_name) if tw_cfg else None
    if tw is not None and not tw.empty:
        tw = tw.copy()
        tw['year_month'] = pd.to_datetime(tw['year_month'])
        ext['twitch'] = tw.set_index('year_month')

    # YouTube
    yt_cfg = (signals_cfg or {}).get('youtube', {})
    yt = load_youtube_monthly(appid, yt_cfg, target_months=target_months, game_name=game_name) if yt_cfg else None
    if yt is not None and not yt.empty:
        yt = yt.copy()
        yt['year_month'] = pd.to_datetime(yt['year_month'])
        ext['youtube'] = yt.set_index('year_month')

    # DLCs (desde Mongo)
    if (dlc_cfg or {}).get('enabled', False):
        dlc_mongo_cfg = dlc_cfg.get('mongo_connection', {})
        try:
            dlc_df = load_dlcs_for_game(appid, dlc_mongo_cfg)
            if dlc_df is not None and not dlc_df.empty:
                dlc_df = dlc_df.copy()
                dlc_df['year_month'] = pd.to_datetime(dlc_df['year_month'])
                ext['dlc'] = dlc_df.set_index('year_month')
        except Exception:
            pass

    # Noticias clasificadas (conteos por mes)
    if news_counts_df is not None and not news_counts_df.empty:
        sub = news_counts_df[news_counts_df['appid'].astype(str) == str(appid)].copy()
        if not sub.empty:
            sub['year_month'] = pd.to_datetime(sub['year_month'])
            ext['news_counts'] = sub.set_index('year_month')

    # Topicos etiquetados (lista de etiquetas por mes)
    if topics_labels_df is not None and not topics_labels_df.empty:
        sub = topics_labels_df[topics_labels_df['appid'].astype(str) == str(appid)].copy()
        if not sub.empty:
            sub['year_month'] = pd.to_datetime(sub['year_month'])
            ext['topics_labels'] = sub.set_index('year_month')

    return ext


def enrich_group(group: pd.DataFrame, external_data: dict) -> list[dict]:
    """Crea explicaciones básicas juntando eventos con señales externas.

    Para cada fila de `group` (un appid), busca coincidencias por `year_month`
    en Twitch/YouTube y DLCs y compone un registro simple.
    """
    out: list[dict] = []
    g = group.copy()
    g['year_month'] = pd.to_datetime(g['year_month'])

    # Precalcular picos por variable para reglas causales simples
    try:
        players_peaks = set(pd.to_datetime(g[(g['variable'] == 'players') & (g['direction'] == 'peak')]['year_month']))
        pos_peaks = set(pd.to_datetime(g[(g['variable'].isin(['pos', 'positive'])) & (g['direction'] == 'peak')]['year_month']))
    except Exception:
        players_peaks, pos_peaks = set(), set()

    for _, ev in g.iterrows():
        ym = ev['year_month']
        rec = {
            'appid': str(ev.get('appid')),
            'year_month': ym,
            'variable': ev.get('variable'),
            'direction': ev.get('direction'),
        }
        # Twitch: marcar pico si hay incremento fuerte (heurística básica)
        tw = external_data.get('twitch')
        if tw is not None and ym in tw.index and 'viewers' in tw.columns:
            # comparar con media de 3 meses alrededor si disponible
            try:
                window = tw['viewers'].rolling(3, center=True).mean()
                rec['twitch_spike'] = bool(tw.loc[ym, 'viewers'] >= 1.25 * (window.loc[ym] or 0))
            except Exception:
                rec['twitch_spike'] = True
        # YouTube: número de menciones si existe columna
        yt = external_data.get('youtube')
        if yt is not None and ym in yt.index:
            for cand in ['mentions', 'videos', 'count']:
                if cand in yt.columns:
                    rec['yt_mentions'] = int(yt.loc[ym, cand])
                    break
        # DLC cercano al mes
        dlc = external_data.get('dlc')
        if dlc is not None:
            # buscar algún dlc en el mismo mes
            if ym in dlc.index and 'dlc_name' in dlc.columns:
                rec['dlc_release'] = str(dlc.loc[ym]['dlc_name'])

        # Noticias por categoría (si existen agregados)
        news = external_data.get('news_counts')
        if news is not None and ym in news.index:
            for col in ['news_patch', 'news_marketing', 'news_community', 'news_other']:
                if col in news.columns:
                    try:
                        rec[col] = int(news.loc[ym, col])
                    except Exception:
                        pass

        # Palabras clave de noticias (top-k agregadas por mes)
        news_kw = external_data.get('news_kw')
        if news_kw is not None and ym in news_kw.index:
            try:
                kws = news_kw.loc[ym, 'news_keywords'] if 'news_keywords' in news_kw.columns else None
                if isinstance(kws, list) and kws:
                    rec['news_keywords'] = [str(x) for x in kws if str(x).strip()][:10]
            except Exception:
                pass
            try:
                pk = news_kw.loc[ym, 'news_patch_keywords'] if 'news_patch_keywords' in news_kw.columns else None
                if isinstance(pk, list) and pk:
                    rec['news_patch_keywords'] = [str(x) for x in pk if str(x).strip()][:10]
            except Exception:
                pass

        # Tópicos etiquetados (lista de etiquetas)
        tlabels = external_data.get('topics_labels')
        if tlabels is not None and ym in tlabels.index:
            val = tlabels.loc[ym, 'labels'] if 'labels' in tlabels.columns else None
            if isinstance(val, list):
                rec['topics_labels'] = [str(x) for x in val]
            elif isinstance(val, str) and val:
                # separar por coma si viene serializado
                rec['topics_labels'] = [x.strip() for x in val.split(',') if x.strip()]

        # Heurística causal: pico de players y pico de reseñas positivas con parche el mismo mes
        try:
            has_players_peak = ym in players_peaks
            has_pos_peak = ym in pos_peaks
            news_patch = int(rec.get('news_patch', 0)) if rec.get('news_patch') is not None else 0
            if has_players_peak and has_pos_peak and news_patch > 0:
                rec['possible_patch_cause'] = True
                # Resumen breve usando keywords si están disponibles
                kw = rec.get('news_patch_keywords') or rec.get('news_keywords') or []
                if isinstance(kw, list) and kw:
                    rec['patch_summary'] = ", ".join([str(x) for x in kw[:3]])
        except Exception:
            pass

        out.append(rec)

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    args = ap.parse_args()
    cfg = expand_env_in_obj(yaml.safe_load(open(args.config, 'r')))
    
    parallel_mode = (cfg.get('parallelization', {}) or {}).get('mode', 'ray')
    if parallel_mode == 'ray' and RAY_AVAILABLE and not ray.is_initialized():
        address = (cfg.get('ray_cluster', {}) or {}).get('address', 'auto')
        try:
            print("[INFO] Inicializando Ray...")
            if address in (None, "", "local", "auto"):
                ray.init()
            else:
                ray.init(address=address)
        except Exception as e:
            print(f"[WARN] No se pudo inicializar Ray ('{address}'): {e}. Fallback a ejecución local.")
            # degradar el modo para el resto del script
            parallel_mode = 'multiprocessing'

    # Configure MLflow experiment and run name prefix
    ml_cfg = (cfg.get('mlflow') or {})
    if not ml_cfg.get('enabled', True):
        os.environ['MLFLOW_TRACKING_URI'] = 'file:///dev/null'
    else:
        try:
            exp_name = ml_cfg.get('experiment') or ml_cfg.get('experiment_name') or 'Default'
            mlflow.set_experiment(exp_name)
        except Exception as e:
            print(f"[WARN] No se pudo configurar el experimento de MLflow: {e}")
    run_name_prefix = ml_cfg.get('run_name_prefix', '')

    with mlflow.start_run(run_name=f"{run_name_prefix}enrich_events"):
        try:
            mlflow.log_dict(cfg, "config.yaml")
        except Exception:
            pass
        outdir = Path(cfg.get('output_dir', 'outputs/events'))
        outdir.mkdir(parents=True, exist_ok=True)
        events_path = outdir / 'events.parquet'
        
        if not events_path.exists():
            raise FileNotFoundError("Archivo de eventos no encontrado.")
        
        events_df = read_parquet_any(events_path)
        if events_df.empty:
            print("[INFO] No hay eventos para enriquecer. Abortando.")
            mlflow.log_metric("events_enriched", 0)
            return

        metadata_lookup: dict = {}
        meta_path = cfg.get('metadata_parquet')
        if meta_path:
            try:
                md = read_parquet_any(meta_path)
                if not md.empty and 'appid' in md.columns:
                    md = md.copy()
                    md['appid'] = md['appid'].astype(str)
                    if 'name' in md.columns:
                        metadata_lookup = {row['appid']: str(row['name']) for _, row in md[['appid','name']].dropna().iterrows()}
            except Exception as meta_exc:
                print(f"[WARN] No se pudo cargar metadata de {meta_path}: {meta_exc}")
        # --- Cargar noticias clasificadas (agregadas por mes) ---
        news_counts_df = pd.DataFrame()
        try:
            news_path = outdir / 'news_classified.parquet'
            if news_path.exists():
                df_news = read_parquet_any(news_path)
                if not df_news.empty:
                    tmp = df_news.copy()
                    # Normalizar columnas esperadas
                    if 'date' in tmp.columns:
                        tmp['year_month'] = pd.to_datetime(tmp['date'], errors='coerce').dt.to_period('M').dt.to_timestamp()
                    elif 'year_month' in tmp.columns:
                        tmp['year_month'] = pd.to_datetime(tmp['year_month'], errors='coerce')
                    else:
                        tmp['year_month'] = pd.NaT
                    tmp = tmp.dropna(subset=['year_month'])
                    tmp['appid'] = tmp['appid'].astype(str)
                    tmp['label'] = tmp['label'].astype(str).str.strip().str.lower()
                    # Pivot wide por categoría
                    grp = tmp.groupby(['appid','year_month','label']).size().rename('count').reset_index()
                    pivot = grp.pivot_table(index=['appid','year_month'], columns='label', values='count', fill_value=0).reset_index()
                    # Renombrar a columnas estándar
                    colmap = {
                        'patch': 'news_patch',
                        'marketing': 'news_marketing',
                        'community': 'news_community',
                        'other': 'news_other',
                    }
                    pivot = pivot.rename(columns={k: v for k, v in colmap.items() if k in pivot.columns})
                    # Asegurar columnas presentes
                    for c in colmap.values():
                        if c not in pivot.columns:
                            pivot[c] = 0
                    news_counts_df = pivot[['appid','year_month'] + list(colmap.values())]
        except Exception as e:
            print(f"[WARN] No se pudo cargar/agregar noticias clasificadas: {e}")

        # --- Cargar tópicos etiquetados ---
        topics_labels_df = pd.DataFrame()
        try:
            topics_labeled_path = outdir / 'topics_labeled.parquet'
            if topics_labeled_path.exists():
                df_tl = read_parquet_any(topics_labeled_path)
                if not df_tl.empty:
                    tmp = df_tl.copy()
                    tmp['appid'] = tmp['appid'].astype(str)
                    # Normalizar fecha de evento
                    for cand in ['event_year_month', 'anchor_year_month', 'year_month']:
                        if cand in tmp.columns:
                            tmp['year_month'] = pd.to_datetime(tmp[cand], errors='coerce')
                            break
                    if 'topics' in tmp.columns:
                        # Extraer lista de llm_label
                        def _extract_labels(items):
                            out = []
                            if isinstance(items, list):
                                for t in items:
                                    if isinstance(t, dict) and t.get('llm_label'):
                                        out.append(str(t['llm_label']))
                            return out
                        tmp['labels'] = tmp['topics'].apply(_extract_labels)
                        # Agregar por appid/mes
                        tmp = tmp[['appid','year_month','labels']]
                        agg = tmp.groupby(['appid','year_month'])['labels'].apply(lambda lists: sum(lists, [])).reset_index()
                        topics_labels_df = agg
        except Exception as e:
            print(f"[WARN] No se pudo cargar/extraer topics etiquetados: {e}")

        # --- Cargar keywords agregadas de noticias ---
        news_kw_df = pd.DataFrame()
        try:
            news_path = outdir / 'news_classified.parquet'
            if news_path.exists():
                df_nc = read_parquet_any(news_path)
                if not df_nc.empty and 'title' in df_nc.columns:
                    tmp = df_nc.copy()
                    tmp['appid'] = tmp['appid'].astype(str)
                    if 'year_month' not in tmp.columns:
                        if 'date' in tmp.columns:
                            tmp['year_month'] = pd.to_datetime(tmp['date'], errors='coerce').dt.to_period('M').dt.to_timestamp()
                        else:
                            tmp['year_month'] = pd.NaT
                    # Explode keywords si existen
                    if 'keywords' in tmp.columns:
                        # Top-k por appid/mes y por etiqueta 'patch'
                        def _topk(series, k=5):
                            vc = series.value_counts()
                            return [str(x) for x in vc.index.tolist()[:k]]
                        # Todas
                        all_kw = (tmp.explode('keywords')
                                    .dropna(subset=['keywords'])
                                    .groupby(['appid','year_month'])['keywords']
                                    .apply(lambda s: _topk(s, 5))
                                    .reset_index(name='news_keywords'))
                        # Solo patch
                        if 'label' in tmp.columns:
                            patch_kw = (tmp[tmp['label'].astype(str).str.lower()=='patch']
                                          .explode('keywords')
                                          .dropna(subset=['keywords'])
                                          .groupby(['appid','year_month'])['keywords']
                                          .apply(lambda s: _topk(s, 5))
                                          .reset_index(name='news_patch_keywords'))
                        else:
                            patch_kw = pd.DataFrame(columns=['appid','year_month','news_patch_keywords'])
                        news_kw_df = pd.merge(all_kw, patch_kw, on=['appid','year_month'], how='left')
        except Exception as e:
            print(f"[WARN] No se pudieron agregar keywords de noticias: {e}")

        event_groups = events_df.groupby('appid')
        
        print(f"Enriqueciendo eventos para {len(event_groups)} juegos de forma paralela...")
        if parallel_mode == 'ray' and RAY_AVAILABLE and ray.is_initialized():
            futures = []
            for appid, group in event_groups:
                app = str(appid)
                news_app = news_counts_df[news_counts_df['appid'].astype(str) == app] if not news_counts_df.empty else pd.DataFrame()
                tlabels_app = topics_labels_df[topics_labels_df['appid'].astype(str) == app] if not topics_labels_df.empty else pd.DataFrame()
                game_name = metadata_lookup.get(app) if metadata_lookup else None
                futures.append(_enrich_events_for_game_ray.remote(app, group, cfg, game_name, news_app, tlabels_app, news_kw_df))
            results = ray.get(futures)
            try:
                ray.shutdown()
            except Exception:
                pass
        else:
            # Fallback secuencial si Ray no está disponible
            results = []
            for appid, group in event_groups:
                app = str(appid)
                news_app = news_counts_df[news_counts_df['appid'].astype(str) == app] if not news_counts_df.empty else pd.DataFrame()
                tlabels_app = topics_labels_df[topics_labels_df['appid'].astype(str) == app] if not topics_labels_df.empty else pd.DataFrame()
                game_name = metadata_lookup.get(app) if metadata_lookup else None
                results.append(_enrich_events_for_game(app, group, cfg, game_name, news_app, tlabels_app, news_kw_df))
        
        all_explanations = [res for res in results if not res.empty]
        
        if all_explanations:
            final_df = pd.concat(all_explanations, ignore_index=True)
            out_path = outdir / 'explanations.parquet'
            write_parquet_any(final_df, out_path)
            mlflow.log_artifact(str(out_path))
            mlflow.log_metric("events_enriched", len(final_df))
            print(f"[OK] Explicaciones de eventos guardadas en -> {out_path}")
        else:
            print("[WARN] No se generaron explicaciones. Creando fichero vacío.")
            mlflow.log_metric("events_enriched", 0)

if __name__ == "__main__":
    main()
