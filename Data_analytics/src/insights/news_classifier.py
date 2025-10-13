#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Clasifica noticias y etiqueta tópicos de forma paralela con un LLM OSS,
registrando cada ejecución en MLflow.
"""
import argparse
import json
import yaml
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Dict, List, Tuple
import os
import time
import sys
import re

import mlflow
import pandas as pd
import requests
from requests.exceptions import RequestException, ConnectionError as RequestsConnectionError
from pymongo import MongoClient
import joblib
import numpy as np

# Ensure project root is importable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils.io import read_parquet_any, write_parquet_any

_SVM_CACHE: Dict[str, joblib] = {}

def _svm_predict_and_keywords(text: str, llm_cfg: Dict) -> Tuple[str | None, List[str]]:
    path = llm_cfg.get("model_path", "models/news_svm.joblib")
    pipe = _SVM_CACHE.get(path)
    if pipe is None:
        try:
            pipe = joblib.load(path)
            _SVM_CACHE[path] = pipe
        except Exception as e:
            print(f"  -> SVM model not available: {e}")
            return None, []
    try:
        label = str(pipe.predict([text])[0])
    except Exception as e:
        print(f"  -> SVM inference failed: {e}")
        return None, []
    # Extraer top-n ngrams por TF-IDF del documento
    kws: List[str] = []
    try:
        tfidf = pipe.named_steps.get('tfidf')
        if tfidf is not None:
            vec = tfidf.transform([text])
            if hasattr(tfidf, 'get_feature_names_out'):
                feats = tfidf.get_feature_names_out()
            else:
                feats = np.array([])
            arr = vec.toarray().ravel()
            topk = int(llm_cfg.get('keywords_max', 6))
            idxs = np.argsort(arr)[::-1]
            kws = [str(feats[i]) for i in idxs if i < len(feats) and arr[i] > 0][:topk]
    except Exception:
        pass
    return label, kws

def query_llm(prompt: str, llm_cfg: Dict) -> str:
    """Envía un prompt al LLM seleccionado y devuelve el texto.

    Soporta:
      - provider: "openai" con API key en env (`OPENAI_API_KEY` por defecto)
      - `server_url`: endpoint compatible con /v1/chat/completions (p.ej., local)
    """
    model = llm_cfg.get("model_id", "gpt-4.1-mini")
    max_tokens = llm_cfg.get("max_new_tokens", 128)
    temperature = llm_cfg.get("temperature", 0.1)
    messages = [{"role": "user", "content": prompt}]

    # 0) Clasificador local (SVM) opcional
    provider = str(llm_cfg.get("provider", "")).lower()
    if provider == "svm":
        model_path = llm_cfg.get("model_path", "models/news_svm.joblib")
        try:
            pipe = joblib.load(model_path)
        except Exception as e:
            print(f"  -> SVM model not available: {e}")
            return ""
        # Para SVM, el "prompt" aquí es realmente el texto a clasificar
        try:
            return str(pipe.predict([prompt])[0])
        except Exception as e:
            print(f"  -> SVM inference failed: {e}")
            return ""

    # 1) OpenAI (REST)
    api_key = llm_cfg.get("api_key") or os.environ.get(llm_cfg.get("api_key_env", "OPENAI_API_KEY"))
    base_url = llm_cfg.get("base_url") or os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1")
    if provider in {"openai", "deepseek"} or (provider == "" and api_key):
        if not api_key:
            print("  -> LLM deshabilitado: falta OPENAI_API_KEY en entorno o llm.api_key en config.")
            return ""
        url = base_url.rstrip("/") + "/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        for attempt in range(int(llm_cfg.get("retries", 3))):
            try:
                resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=30)
                resp.raise_for_status()
                return resp.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            except RequestException as e:
                if attempt < int(llm_cfg.get("retries", 3)) - 1:
                    time.sleep(2 ** attempt)
                    continue
                print(f"  -> LLM (OpenAI) fallo de red: {e}")
                return ""
            except Exception as e:
                print(f"  -> LLM (OpenAI) error: {e}")
                return ""

    # 2) Servidor local/externo compatible con /v1/chat/completions
    server_url = llm_cfg.get("server_url")
    if server_url:
        headers = {"Content-Type": "application/json"}
        payload = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        for attempt in range(int(llm_cfg.get("retries", 2))):
            try:
                response = requests.post(server_url, headers=headers, data=json.dumps(payload), timeout=10)
                response.raise_for_status()
                return response.json()["choices"][0]["message"]["content"].strip()
            except RequestsConnectionError:
                return ""
            except RequestException:
                if attempt < int(llm_cfg.get("retries", 2)) - 1:
                    time.sleep(2 ** attempt)
                    continue
                return ""
            except Exception:
                return ""

    # Si no hay proveedor configurado
    return ""

def load_news_from_mongo(appid: int, mongo_cfg: Dict) -> pd.DataFrame:
    """
    Carga noticias para un appid específico desde la colección de explotación de MongoDB.

    Args:
        appid (int): El AppID de Steam del juego.
        mongo_cfg (Dict): Configuración de la conexión a MongoDB.

    Returns:
        pd.DataFrame: Un DataFrame con las noticias encontradas.
    """
    try:
        client = MongoClient(mongo_cfg["uri"])
        db = client[mongo_cfg.get("db_name")]
        collection = db[mongo_cfg.get("collection_name")]
        
        query = {"appid": appid}
        projection = {"_id": 0, "gid": 1, "title": 1, "contents": 1, "date": 1, "appid": 1}
        
        news_cursor = collection.find(query, projection)
        news_list = list(news_cursor)
        client.close()
        
        return pd.DataFrame(news_list) if news_list else pd.DataFrame()
    except Exception as e:
        print(f"Error al conectar o consultar MongoDB: {e}")
        return pd.DataFrame()


def distinct_appids(mongo_cfg: Dict) -> List[int]:
    """Devuelve la lista de appids distintos presentes en la colección de noticias."""
    try:
        client = MongoClient(mongo_cfg["uri"])
        db = client[mongo_cfg.get("db_name")]
        collection = db[mongo_cfg.get("collection_name")]
        values = collection.distinct("appid")
        client.close()
        out = []
        for v in values:
            try:
                if v is None:
                    continue
                out.append(int(v))
            except Exception:
                continue
        return sorted(set(out))
    except Exception as e:
        print(f"No se pudieron obtener appids distintos de Mongo: {e}")
        return []

def classify_single_news(title: str, llm_cfg: Dict, contents: str | None = None) -> Tuple[str | None, List[str]]:
    """Clasifica un título (y opcionalmente contenido) y extrae keywords.

    Devuelve (label, keywords). La label pertenece a allowed o None; keywords es lista posiblemente vacía.
    """
    labels_cfg = llm_cfg.get("news_labels", []) or []
    allowed = {str(x).strip().lower() for x in labels_cfg}
    labels_str = ", ".join(labels_cfg)
    want_kw = bool(llm_cfg.get("keywords_enabled", True))
    kw_max = int(llm_cfg.get("keywords_max", 6))
    use_contents = bool(llm_cfg.get("use_contents", False))

    extra = ""
    if use_contents and contents:
        snippet = str(contents)[:500]
        extra = f"\nContenido: '{snippet}'"

    if str(llm_cfg.get("provider", "")).lower() == "svm":
        # Clasificación local: SVM + keywords por TF-IDF del documento
        text = title
        if use_contents and contents:
            text = f"{title} \n {str(contents)[:500]}"
        label_svm, kws = _svm_predict_and_keywords(text, llm_cfg)
        return (label_svm if label_svm else None, kws)

    if want_kw:
        prompt = (
            "Clasifica la noticia y extrae palabras clave. Devuelve JSON.\n"
            f"Categorias: [{labels_str}].\n"
            "Formato estricto: {\"label\": <categoria>, \"keywords\": [<kw1>, <kw2>, ...]}\n"
            f"Maximo {kw_max} keywords, concretas (frases cortas), sin repetir la categoria.\n\n"
            f"Titulo: '{title}'{extra}\n\n"
            "JSON:"
        )
    else:
        prompt = (
            f"Clasifica la siguiente noticia en una de estas categorías: [{labels_str}]. "
            f"Responde solo con una categoría exacta.\n\nNoticia: '{title}'\n\nCategoría:"
        )

    raw = query_llm(prompt, llm_cfg)
    def _norm_label(val: str | None) -> str | None:
        label = (val or "").strip().lower()
        aliases = {"patches": "patch", "marketing/ads": "marketing", "community update": "community"}
        label = aliases.get(label, label)
        return label if label in allowed else None

    label_out: str | None = None
    keywords_out: List[str] = []
    if want_kw and raw:
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                label_out = _norm_label(str(obj.get("label")))
                kws = obj.get("keywords")
                if isinstance(kws, list):
                    keywords_out = [str(k).strip() for k in kws if str(k).strip()]
        except Exception:
            pass
    if label_out is None:
        label_out = _norm_label(raw)

    if want_kw and not keywords_out:
        import re as _re
        quoted = _re.findall(r"['\"]([^'\"]{3,60})['\"]", title)
        keywords_out = [q.strip() for q in quoted][:kw_max]

    if kw_max and len(keywords_out) > kw_max:
        keywords_out = keywords_out[:kw_max]
    return label_out, keywords_out

def classify_news_parallel(news_df: pd.DataFrame, llm_cfg: Dict) -> pd.DataFrame:
    """Clasifica noticias en paralelo y añade 'keywords' (lista de strings)."""
    batch_size = int(llm_cfg.get("batch_size", 0) or 0)
    if batch_size > 0:
        return classify_news_batched(news_df, llm_cfg, batch_size)
    has_contents = 'contents' in news_df.columns
    rows = news_df[['title','contents']].to_dict(orient='records') if has_contents else news_df[['title']].assign(contents=None).to_dict(orient='records')
    max_workers = llm_cfg.get("max_workers", 8)
    def _worker(r: Dict) -> Tuple[str | None, List[str]]:
        return classify_single_news(r['title'], llm_cfg, r.get('contents'))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(_worker, rows))
    labels, keywords = zip(*results) if results else ([], [])
    news_df_copy = news_df.copy()
    news_df_copy['label'] = list(labels)
    news_df_copy['keywords'] = list(keywords)
    return news_df_copy.dropna(subset=['label']).reset_index(drop=True)

def _normalize_label(raw: str, allowed: set[str]) -> str | None:
    label = (raw or "").strip().lower()
    aliases = {"patches": "patch", "marketing/ads": "marketing", "community update": "community"}
    label = aliases.get(label, label)
    return label if label in allowed else None

def classify_news_batched(news_df: pd.DataFrame, llm_cfg: Dict, batch_size: int = 20) -> pd.DataFrame:
    """Clasifica noticias en lotes; devuelve label + keywords por fila."""
    allowed = {str(x).strip().lower() for x in (llm_cfg.get("news_labels", []) or [])}
    want_kw = bool(llm_cfg.get("keywords_enabled", True))
    kw_max = int(llm_cfg.get("keywords_max", 6))
    titles = news_df['title'].tolist()
    labels_out: list[str | None] = [None] * len(titles)
    keywords_out: list[List[str]] = [[] for _ in range(len(titles))]

    def _normalize_label(raw: str | None) -> str | None:
        label = (raw or "").strip().lower()
        aliases = {"patches": "patch", "marketing/ads": "marketing", "community update": "community"}
        label = aliases.get(label, label)
        return label if label in allowed else None

    for i in range(0, len(titles), batch_size):
        chunk = titles[i:i+batch_size]
        labels_str = ", ".join(sorted(allowed))
        if want_kw:
            prompt = (
                "Eres un clasificador. Dada una lista JSON de títulos, "
                f"devuelve un JSON array de objetos con exactamente {len(chunk)} elementos. Cada objeto: {{\"label\": <categoria>, \"keywords\": [..]}}.\n"
                f"Usa solo categorías: [{labels_str}]. Máx {kw_max} keywords por título, frases cortas específicas, sin repetir la categoría.\n\n"
                f"Títulos (JSON): {json.dumps(chunk, ensure_ascii=False)}\n\nRespuesta JSON:"
            )
        else:
            prompt = (
                "Eres un clasificador. Dada una lista JSON de títulos de noticias, "
                f"devuelve un JSON array con exactamente {len(chunk)} etiquetas, una por cada título, "
                f"usando solo estas categorías: [{labels_str}].\n\n"
                f"Títulos (JSON): {json.dumps(chunk, ensure_ascii=False)}\n\nRespuesta JSON:"
            )
        resp = query_llm(prompt, llm_cfg)
        ok = False
        if resp:
            try:
                parsed_json = json.loads(resp)
                if want_kw and isinstance(parsed_json, list):
                    ok = True
                    for j, item in enumerate(parsed_json[:len(chunk)]):
                        lbl = _normalize_label((item or {}).get('label'))
                        kws = item.get('keywords') if isinstance(item, dict) else []
                        kws = [str(k).strip() for k in (kws or []) if str(k).strip()][:kw_max]
                        labels_out[i + j] = lbl
                        keywords_out[i + j] = kws
                elif not want_kw and isinstance(parsed_json, list):
                    ok = True
                    for j, raw in enumerate(parsed_json[:len(chunk)]):
                        labels_out[i + j] = _normalize_label(str(raw))
            except Exception:
                ok = False
        if not ok:
            with ThreadPoolExecutor(max_workers=llm_cfg.get("max_workers", 8)) as ex:
                res = list(ex.map(lambda t: classify_single_news(t, llm_cfg, None), chunk))
            for j, (lbl, kws) in enumerate(res):
                labels_out[i + j] = lbl
                keywords_out[i + j] = (kws or [])[:kw_max]

    news_df_copy = news_df.copy()
    news_df_copy['label'] = labels_out
    news_df_copy['keywords'] = keywords_out
    return news_df_copy.dropna(subset=['label']).reset_index(drop=True)

def _merge_parquet_safely(df_new: pd.DataFrame, path: Path, key_cols: list[str]) -> pd.DataFrame:
    """Anexa a un único parquet consolidado deduplicando por claves.
    Si no existe, crea; si existe, concat y drop_duplicates.
    """
    try:
        if path.exists():
            df_old = read_parquet_any(path)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
            df_all = df_all.drop_duplicates(subset=key_cols)
        else:
            df_all = df_new.copy()
        write_parquet_any(df_all, path)
        return df_all
    except Exception as e:
        print(f"No se pudo consolidar parquet {path}: {e}")
        # fallback: guardar solo lo nuevo
        write_parquet_any(df_new, path)
        return df_new

def label_topics(topics_df: pd.DataFrame, llm_cfg: Dict) -> pd.DataFrame:
    """
    Etiqueta cada tópico de BERTopic en un DataFrame usando un LLM.
    Esta función también podría paralelizarse si el número de tópicos es muy grande.
    """
    topics_df_copy = topics_df.copy()
    for _, row in topics_df_copy.iterrows():
        topics_list = row.get('topics', [])
        if not isinstance(topics_list, list):
            continue
        for topic_data in topics_list:
            rep = topic_data.get('Representation', [])
            if isinstance(rep, list):
                words = rep
            else:
                words = str(rep).split(',') if rep is not None else []
            keywords = ", ".join([str(w).strip() for w in words if str(w).strip()])
            if not keywords:
                continue
            prompt = (
                "Resume los siguientes keywords en una etiqueta corta y descriptiva de 2-4 palabras "
                f"que represente el tema principal.\n\nKeywords: {keywords}\n\nEtiqueta:"
            )
            label = query_llm(prompt, llm_cfg)
            topic_data['llm_label'] = label
    return topics_df_copy

def _select_mlflow_experiment(cfg: Dict) -> str:
    """Elige el experimento de MLflow priorizando llm.mlflow_experiment,
    luego cfg.mlflow.experiment, y por último 'Steam_Events_Classification'."""
    llm = cfg.get("llm", {})
    if llm.get("mlflow_experiment"):
        return llm["mlflow_experiment"]
    mlf = cfg.get("mlflow", {})
    if mlf.get("experiment"):
        return mlf["experiment"]
    return cfg.get("mlflow_experiment_name", "Steam_Events_Classification")

def resolve_env_vars(config):
    """Resuelve ${VAR:-default} en un diccionario de configuración.

    - Usa el valor de entorno si está definido y no es cadena vacía.
    - En caso contrario, usa el default provisto (si existe) o "".
    """
    pattern = re.compile(r"\${([^}]+)}")

    def _replace_one(s: str) -> str:
        m = pattern.search(s)
        if not m:
            return s
        expr = m.group(1)
        if ':-' in expr:
            var_name, default = expr.split(':-', 1)
        else:
            var_name, default = expr, ''
        env_val = os.environ.get(var_name)
        value = env_val if env_val not in (None, '') else default
        return s[: m.start()] + value + s[m.end():]

    out = {}
    for key, value in config.items():
        if isinstance(value, str):
            prev = None
            cur = value
            # Reemplazar iterativamente por si hay varias variables en la misma cadena
            while prev != cur:
                prev = cur
                cur = _replace_one(cur)
            out[key] = cur
        elif isinstance(value, dict):
            out[key] = resolve_env_vars(value)
        else:
            out[key] = value
    return out

def main():
    """Punto de entrada principal del script."""
    ap = argparse.ArgumentParser(description="Clasifica noticias y (opcional) etiqueta tópicos con un LLM.")
    ap.add_argument("--config", required=True, help="Ruta al fichero de configuración YAML.")
    ap.add_argument("--appid", type=int, help="AppID para cargar y clasificar sus noticias desde MongoDB.")
    ap.add_argument("--label-topics", action="store_true", help="Si se pasa, además etiqueta los tópicos de BERTopic.")
    args = ap.parse_args()
    
    with open(args.config, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # Merge override para LLM si existe (configs/llm_override.yaml)
    try:
        override_path = Path('configs/llm_override.yaml')
        if override_path.exists():
            ov = yaml.safe_load(override_path.read_text(encoding='utf-8')) or {}
            if isinstance(ov, dict) and ov:
                llm_section = cfg.get('llm') or {}
                llm_section.update(ov)
                cfg['llm'] = llm_section
    except Exception:
        pass
    
    cfg = resolve_env_vars(cfg)

    outdir = Path(cfg.get('output_dir', 'outputs/events'))
    outdir.mkdir(parents=True, exist_ok=True)
    
    llm_cfg = cfg.get('llm', {})
    if not llm_cfg.get('enabled', False):
        print("El clasificador LLM está deshabilitado en la configuración.")
        return

    mlflow.set_experiment(_select_mlflow_experiment(cfg))

    with mlflow.start_run():
        print(f"MLflow Run started. Experiment: '{mlflow.get_experiment(mlflow.active_run().info.experiment_id).name}'")
        mlflow.log_param("config_file", args.config)

        mongo_cfg = cfg.get("mongodb", {})

        if args.appid:
            mlflow.log_param("appid", args.appid)
            # Evitar registrar secretos
            safe_llm_params = {k: v for k, v in llm_cfg.items() if k not in {"api_key"}}
            mlflow.log_params(safe_llm_params)
            
            news_df = load_news_from_mongo(args.appid, mongo_cfg)
            
            if not news_df.empty:
                print(f"Clasificando {len(news_df)} noticias para el appid {args.appid} en paralelo...")
                classified_news = classify_news_parallel(news_df, llm_cfg)
                
                mlflow.log_metric("total_news_found", len(news_df))
                mlflow.log_metric("news_classified_count", len(classified_news))
                
                if not classified_news.empty:
                    # Consolidar en un único parquet como define DVC
                    output_path = outdir / 'news_classified.parquet'
                    all_df = _merge_parquet_safely(classified_news, output_path, key_cols=['appid','gid','title'])
                    mlflow.log_artifact(str(output_path))
                    print(f"{len(classified_news)} noticias clasificadas; consolidado total: {len(all_df)}. Registrado en MLflow.")
                else:
                    print("Ninguna noticia pudo ser clasificada.")
            else:
                print(f"No se encontraron noticias para el appid {args.appid}.")
        else:
            # Modo batch: clasificar para todos los appids presentes en Mongo
            appids = distinct_appids(mongo_cfg)
            if not appids:
                print("No se encontraron appids en la colección de noticias. Fin.")
            total_found = 0
            total_classified = 0
            output_path = outdir / 'news_classified.parquet'
            for i, appid in enumerate(appids, start=1):
                news_df = load_news_from_mongo(appid, mongo_cfg)
                if news_df.empty:
                    continue
                total_found += len(news_df)
                print(f"[{i}/{len(appids)}] Clasificando {len(news_df)} noticias para appid {appid}...")
                classified_news = classify_news_parallel(news_df, llm_cfg)
                total_classified += len(classified_news)
                if not classified_news.empty:
                    _merge_parquet_safely(classified_news, output_path, key_cols=['appid','gid','title'])
            mlflow.log_metric("total_news_found", total_found)
            mlflow.log_metric("news_classified_count", total_classified)
            if (outdir / 'news_classified.parquet').exists():
                mlflow.log_artifact(str(outdir / 'news_classified.parquet'))

        if args.label_topics:
            topics_input_path = cfg.get("topics_input_path") or str(outdir / 'topics.parquet')
            if Path(topics_input_path).exists():
                topics_df = read_parquet_any(Path(topics_input_path))
                if not topics_df.empty:
                    print(f"\nTópicos etiquetados guardados en -> {output_path}")
            else:
                print(f"\nNo se encontró el fichero de tópicos ({topics_input_path}). Se omite el etiquetado.")

if __name__ == "__main__":
    main()
