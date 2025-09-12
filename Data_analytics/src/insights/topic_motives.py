#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
BERTopic sobre reseñas en ventanas relevantes, paralelizado para múltiples eventos.
"""
import argparse
import yaml
from pathlib import Path
import pandas as pd
from pymongo import MongoClient
from bertopic import BERTopic
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import CountVectorizer
from typing import Dict, Any, List
import os
from multiprocessing import Pool, cpu_count
import mlflow
from typing import Optional

# Importaciones de utilidades del proyecto
from src.utils.io import read_parquet_any, write_parquet_any, makedirs_if_local
from src.utils.mlflow_utils import start_mlflow_run, log_mlflow_params, log_mlflow_metrics, log_mlflow_artifacts

# El decorador @ray.remote debe estar en el nivel superior del módulo.
try:
    import ray
    RAY_AVAILABLE = True
    @ray.remote
    def _process_event_group_ray(appid: str, group: pd.DataFrame, cfg: Dict[str, Any]) -> List[Dict]:
        return _process_event_group(appid, group, cfg)
except ImportError:
    RAY_AVAILABLE = False

# Coherencia C_v opcional vía Gensim
try:
    from gensim.corpora import Dictionary  # type: ignore
    from gensim.models.coherencemodel import CoherenceModel  # type: ignore
    GENSIM_AVAILABLE = True
except Exception:
    GENSIM_AVAILABLE = False

def _process_event_group(appid: str, group: pd.DataFrame, cfg: Dict[str, Any]) -> List[Dict]:
    """
    Función que encapsula la lógica de modelado de tópicos para un único grupo de eventos.
    Esta función se ejecutará en paralelo.
    """
    topic_cfg = cfg.get('bertopic', {})
    all_topics_results = []
    
    embedding_model = SentenceTransformer(topic_cfg.get('embedding_model', 'all-MiniLM-L6-v2'))
    vectorizer_model = CountVectorizer(stop_words="english", min_df=topic_cfg.get('min_df', 5))
    topic_model = BERTopic(
        embedding_model=embedding_model,
        vectorizer_model=vectorizer_model,
        min_topic_size=topic_cfg.get('min_topic_size', 15),
        verbose=False
    )
    
    mongo_cfg = cfg.get('mongo_connection', {})

    for (_, year_month), event_group in group.groupby(group.index):
        event_date = pd.Timestamp(year_month)
        window = int(topic_cfg.get('window_months', 2))
        start_date = event_date - pd.DateOffset(months=window)
        end_date = event_date + pd.DateOffset(months=window)
        
        print(f"Analizando tópicos para el evento de {appid} en {year_month.strftime('%Y-%m')}...")
        
        reviews = load_reviews_for_window(str(appid), start_date, end_date, mongo_cfg, cfg)
        # Limitar muestreo por evento para MVP/escala
        max_docs = int(topic_cfg.get('max_docs_per_event', 20000))
        if len(reviews) > max_docs:
            import random
            random.seed(42)
            reviews = random.sample(reviews, max_docs)
        
        if len(reviews) < topic_model.min_topic_size:
            print(f"  -> No hay suficientes reseñas ({len(reviews)}) para modelar tópicos.")
            continue

        try:
            topics, _ = topic_model.fit_transform(reviews)
            topic_info = topic_model.get_topic_info()

            top_n = topic_cfg.get('top_n_topics', 3)
            main_topics = topic_info[topic_info.Topic != -1].head(top_n)

            # Calcular coherencia C_v si Gensim está disponible
            topic_rows = main_topics.to_dict(orient='records') if not main_topics.empty else []
            if GENSIM_AVAILABLE and topic_rows:
                try:
                    analyzer = vectorizer_model.build_analyzer()
                    tokens = [analyzer(t) for t in reviews]
                    dictionary = Dictionary(tokens)
                    # Palabras por tópico (top-k)
                    top_k_words = int(topic_cfg.get('coherence_top_k_words', 10))
                    topic_ids = [int(r['Topic']) for r in topic_rows]
                    topics_words: list[list[str]] = []
                    for tid in topic_ids:
                        words = [w for (w, _w) in (topic_model.get_topic(tid) or [])][:top_k_words]
                        topics_words.append(words)
                    cm = CoherenceModel(topics=topics_words, texts=tokens, dictionary=dictionary, coherence='c_v')
                    cv_scores = cm.get_coherence_per_topic()
                    # Anotar cada fila con su coherencia
                    for i, r in enumerate(topic_rows):
                        r['coherence_cv'] = float(cv_scores[i]) if i < len(cv_scores) else None
                except Exception as ce:
                    print(f"  -> No se pudo calcular C_v para {appid}@{year_month}: {ce}")

            if topic_rows:
                result = {
                    'appid': str(appid),
                    'event_year_month': event_date,
                    'topics': topic_rows
                }
                all_topics_results.append(result)
        except Exception as e:
            print(f"  -> Error modelando tópicos para {appid}: {e}")
            
    return all_topics_results

def _review_weight(votes_up: int | None, cfg: Dict[str, Any]) -> int:
    """Calcula el peso de una reseña según votos de utilidad.

    Método por defecto: 1 + floor(log2(1+votes_up)), con tope `cap`.
    """
    try:
        v = int(votes_up or 0)
    except Exception:
        v = 0
    wcfg = cfg.get('bertopic', {}).get('weighting', {})
    cap = int(wcfg.get('cap', 5))
    method = str(wcfg.get('method', 'log2')).lower()
    base = max(v, 0)
    if method == 'log10':
        import math
        w = 1 + int(math.log10(1 + base))
    elif method == 'log1p':
        import math
        w = 1 + int(math.log1p(base))
    else:  # log2 por defecto
        import math
        w = 1 + int(math.log2(1 + base))
    return max(1, min(w, cap))


def load_reviews_for_window(appid: str, start_date: pd.Timestamp, end_date: pd.Timestamp, mongo_cfg: dict, cfg: Dict[str, Any] | None = None) -> list[str]:
    """Carga reseñas de MongoDB para una ventana de tiempo y aplica ponderación opcional.

    Si `bertopic.weighting.enabled` es True en cfg, replica el texto según `_review_weight`.
    Evita textos muy cortos con `min_len`.
    """
    client = MongoClient(mongo_cfg['uri'])
    col = client[mongo_cfg['database']][mongo_cfg['collection']]
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    query = {
        "appid": {"$in": [appid, int(appid)]},
        "timestamp_created": {"$gte": start_ts, "$lt": end_ts},
        "language": "english"
    }
    projection = {"review": 1, "votes_up": 1, "votes_helpful": 1, "helpful": 1, "_id": 0}
    docs = list(col.find(query, projection))
    client.close()

    if not docs:
        return []

    min_len = int(((cfg or {}).get('bertopic', {}) or {}).get('weighting', {}).get('min_len', 100))
    weighting_enabled = bool(((cfg or {}).get('bertopic', {}) or {}).get('weighting', {}).get('enabled', False))

    out: list[str] = []
    for d in docs:
        text = d.get('review')
        if not text:
            continue
        if len(text) < min_len:
            continue
        if not weighting_enabled:
            out.append(text)
            continue
        votes = d.get('votes_up')
        if votes is None:
            votes = d.get('votes_helpful')
        if votes is None:
            votes = d.get('helpful')
        w = _review_weight(votes, cfg or {})
        out.extend([text] * w)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.config, 'r'))
    
    # Iniciar MLflow
    if not cfg['mlflow']['enabled']: os.environ['MLFLOW_TRACKING_URI'] = 'file:///dev/null' 
    with mlflow.start_run(run_name=f"{cfg['mlflow'].get('run_name_prefix', '')}bertopic_analysis"):
        mlflow.log_dict(cfg, "config.yaml")

        outdir = Path(cfg.get('output_dir', 'outputs/events'))
        events_path = outdir / 'events.parquet'
        
        if not events_path.exists():
            raise FileNotFoundError("Archivo de eventos no encontrado. Ejecuta detect_events.py primero.")
        
        events_df = read_parquet_any(events_path).set_index('event_id')
        
        if events_df.empty:
            print("[INFO] No hay eventos para analizar. Abortando.")
            mlflow.log_metric("total_events_analyzed", 0)
            return

        # 1. Preparar la lista de argumentos para la paralelización
        event_groups = events_df.groupby('appid')
        processing_args = [(appid, group, cfg) for appid, group in event_groups]
        
        # 2. Ejecutar las tareas en paralelo
        all_topics_results = []
        parallel_mode = (cfg.get('parallelization', {}) or {}).get('mode') or cfg.get('parallel_mode')
        if parallel_mode == 'multiprocessing':
            print("[INFO] Usando multiprocessing para paralelización.")
            num_processes = cfg.get('num_processes', cpu_count())
            with Pool(processes=num_processes) as pool:
                results = pool.starmap(_process_event_group, processing_args)
                all_topics_results = [item for sublist in results for item in sublist]
        elif parallel_mode == 'ray' and RAY_AVAILABLE:
            print("[INFO] Usando Ray para paralelización distribuida.")
            if not ray.is_initialized(): ray.init()
            futures = [_process_event_group_ray.remote(appid, group, cfg) for appid, group in processing_args]
            results = ray.get(futures)
            all_topics_results = [item for sublist in results for item in sublist]
        else: # Modo secuencial
            print("[WARN] No se ha especificado un modo de paralelización válido. Ejecutando en modo secuencial.")
            for appid, group in event_groups:
                all_topics_results.extend(_process_event_group(appid, group, cfg))
        
        # 3. Consolidar, guardar y loguear los resultados finales
        if all_topics_results:
            final_df = pd.DataFrame(all_topics_results)
            out_path = Path(outdir) / 'topics.parquet'
            makedirs_if_local(out_path.parent)
            write_parquet_any(final_df, out_path)
            
            mlflow.log_artifact(str(out_path))
            mlflow.log_metric("total_events_analyzed", len(final_df))
            # Métrica global de coherencia (promedio de C_v si existe)
            try:
                cv_vals: list[float] = []
                for rec in all_topics_results:
                    for t in (rec.get('topics') or []):
                        v = t.get('coherence_cv')
                        if v is not None:
                            cv_vals.append(float(v))
                if cv_vals:
                    mlflow.log_metric("avg_topic_coherence_cv", float(sum(cv_vals)/len(cv_vals)))
                    mlflow.log_metric("topics_with_cv", int(len(cv_vals)))
            except Exception:
                pass
            print(f"[OK] Tópicos de eventos guardados en -> {out_path}")
        else:
            print("[WARN] No se generaron tópicos para ningún evento. Guardando fichero vacío.")
            mlflow.log_metric("total_events_analyzed", 0)
            write_parquet_any(pd.DataFrame(), Path(outdir) / 'topics.parquet')

if __name__ == "__main__":
    main()
