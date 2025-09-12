#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Enriquece eventos con Twitch/YouTube/DLC/News/Topics, paralelizado con Ray.

Incluye utilidades mínimas `load_external_signals` y `enrich_group` para
combinar señales por appid/mes con los eventos detectados.
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
import mlflow
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
                            news_counts_df: pd.DataFrame | None = None,
                            topics_labels_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Enriquece los eventos de un único juego."""
    signals_cfg = cfg.get('signals', {})
    dlc_cfg = cfg.get('dlc', {})
    external_data = load_external_signals(appid, signals_cfg, dlc_cfg,
                                          news_counts_df=news_counts_df,
                                          topics_labels_df=topics_labels_df)
    explanations = enrich_group(group, external_data)
    return pd.DataFrame(explanations) if explanations else pd.DataFrame()

_enrich_events_for_game_ray = None
if RAY_AVAILABLE:
    _enrich_events_for_game_ray = ray.remote(_enrich_events_for_game)


def load_external_signals(appid: str, signals_cfg: dict, dlc_cfg: dict,
                          news_counts_df: pd.DataFrame | None = None,
                          topics_labels_df: pd.DataFrame | None = None) -> dict:
    """Carga señales externas (Twitch, YouTube) y DLCs para un appid.

    Retorna un dict con dataframes indexados por `year_month` cuando aplica.
    """
    ext: dict = {}
    # Twitch
    tw_cfg = (signals_cfg or {}).get('twitch', {})
    tw = load_twitch_monthly(appid, tw_cfg) if tw_cfg else None
    if tw is not None and not tw.empty:
        tw = tw.copy()
        tw['year_month'] = pd.to_datetime(tw['year_month'])
        ext['twitch'] = tw.set_index('year_month')

    # YouTube
    yt_cfg = (signals_cfg or {}).get('youtube', {})
    yt = load_youtube_monthly(appid, yt_cfg) if yt_cfg else None
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

    # Noticias clasificadas (conteos por categoría, por mes)
    if news_counts_df is not None and not news_counts_df.empty:
        sub = news_counts_df[news_counts_df['appid'].astype(str) == str(appid)].copy()
        if not sub.empty:
            sub['year_month'] = pd.to_datetime(sub['year_month'])
            ext['news_counts'] = sub.set_index('year_month')

    # Tópicos etiquetados (lista de labels por evento/mes)
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

        # Tópicos etiquetados (lista de etiquetas)
        tlabels = external_data.get('topics_labels')
        if tlabels is not None and ym in tlabels.index:
            val = tlabels.loc[ym, 'labels'] if 'labels' in tlabels.columns else None
            if isinstance(val, list):
                rec['topics_labels'] = [str(x) for x in val]
            elif isinstance(val, str) and val:
                # separar por coma si viene serializado
                rec['topics_labels'] = [x.strip() for x in val.split(',') if x.strip()]

        out.append(rec)

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config, 'r'))
    
    parallel_mode = (cfg.get('parallelization', {}) or {}).get('mode', 'ray')
    if parallel_mode == 'ray' and RAY_AVAILABLE and not ray.is_initialized():
        print("[INFO] Inicializando Ray...")
        ray.init(address=cfg.get('ray_cluster', {}).get('address', 'auto'))

    with mlflow.start_run(run_name=f"enrich_events"):
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

        event_groups = events_df.groupby('appid')
        
        print(f"Enriqueciendo eventos para {len(event_groups)} juegos de forma paralela...")
        if parallel_mode == 'ray' and RAY_AVAILABLE:
            futures = []
            for appid, group in event_groups:
                app = str(appid)
                news_app = news_counts_df[news_counts_df['appid'].astype(str) == app] if not news_counts_df.empty else pd.DataFrame()
                tlabels_app = topics_labels_df[topics_labels_df['appid'].astype(str) == app] if not topics_labels_df.empty else pd.DataFrame()
                futures.append(_enrich_events_for_game_ray.remote(app, group, cfg, news_app, tlabels_app))
            results = ray.get(futures)
            ray.shutdown()
        else:
            # Fallback secuencial si Ray no está disponible
            results = []
            for appid, group in event_groups:
                app = str(appid)
                news_app = news_counts_df[news_counts_df['appid'].astype(str) == app] if not news_counts_df.empty else pd.DataFrame()
                tlabels_app = topics_labels_df[topics_labels_df['appid'].astype(str) == app] if not topics_labels_df.empty else pd.DataFrame()
                results.append(_enrich_events_for_game(app, group, cfg, news_app, tlabels_app))
        
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
