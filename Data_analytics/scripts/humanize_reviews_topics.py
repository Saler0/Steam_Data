#!/usr/bin/env python
"""Humaniza etiquetas (labels) para tópicos de BERTopic.

Lee `outputs/topics.parquet` (por defecto), que contiene tópicos anidados,
y genera un mapping `outputs/events/humanized_topics.csv` con columnas:
  - appid (str)
  - year_month (datetime)
  - topic_id (int)
  - label (str)               → etiqueta humanizada final en inglés
  - topic_name_original (str) → nombre original (ej. 0_ships_ai_races_like)
  - coherence_cv (float)
  - c_topics (int)
  - provider (str)

Si está configurada la variable de entorno `DEEPSEEK_API_KEY` y hay red,
utiliza la API de DeepSeek; si no, usa un heurístico que limpia y titula.

Este script es RESUMABLE: si se interrumpe, continuará donde se quedó.
"""
from __future__ import annotations

import argparse
import os
import re
import csv
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm


def _read_any(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Input file not found: {p}")
    suf = p.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(p)
    if suf == ".json":
        return pd.read_json(p, lines=True)
    return pd.read_parquet(p)


def _to_title_2_4_words(s: str, min_words: int = 2, max_words: int = 4) -> str:
    s = re.sub(r"^\d+_", "", s)
    tokens = [t for t in re.split(r"_|\W+", str(s)) if t]
    if not tokens:
        return ""
    words = tokens[: max(min_words, min(max_words, len(tokens)))]
    title = " ".join(w.capitalize() for w in words)
    return title.strip()


def _call_deepseek(prompt: str) -> Optional[str]:
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        return None
    try:
        import requests  # type: ignore
    except Exception:
        return None

    base = os.getenv("DEEPSEEK_BASE_URL") or os.getenv("DEEPSEEK_API_BASE") or "https://api.deepseek.com/v1"
    url = f"{base.rstrip('/')}/chat/completions" if base.endswith("/v1") else f"{base.rstrip('/')}/v1/chat/completions"

    system_en = (
        "You label video game topics based on their keywords. "
        "Return only a short 2-4 word Title Case label in English, no quotes."
    )

    payload = {
        "model": os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        "messages": [
            {"role": "system", "content": system_en},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 64,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        text = data.get("choices", [{}])[0].get("message", {}).get("content")
        if not isinstance(text, str) or not text.strip():
            return None
        return text.strip().strip("'\"")
    except Exception:
        return None


def _build_prompt(topic_name: str, keywords: List[str], max_keywords: int = 10) -> str:
    kw_list = [str(k) for k in keywords if isinstance(k, str) and k.strip()][:max_keywords]
    joined = ", ".join(kw_list)
    return (
        f"Original topic name: {topic_name}\n"
        f"Keywords: {joined}\n"
        "Suggest a 2-4 word human-readable label for this topic."
    )


def _heuristic_label(name: str, keywords: List[str]) -> str:
    base = name or ""
    if not base and keywords:
        base = " ".join(keywords)
    return _to_title_2_4_words(base)


def humanize_labels(
    topics_path: str | Path,
    out_csv: str | Path,
    provider: str = "auto",
    max_keywords_per_topic: int = 10,
) -> Path:
    df = _read_any(topics_path)
    if df.empty:
        raise SystemExit("Dataset de tópicos vacío.")

    # --- Preparación de Datos ---
    df_exploded = df.explode('topics').reset_index(drop=True)
    df_normalized = pd.json_normalize(df_exploded['topics'])
    df_flat = pd.concat([df_exploded.drop(columns=['topics']), df_normalized], axis=1)
    df_flat['c_topics'] = df_flat['Count']

    df_flat = df_flat.rename(columns={
        'Name': 'topic_name_original',
        'Topic': 'topic_id',
        'Representation': 'keywords'
    })
    df_flat['appid'] = df_flat['appid'].astype(str)
    df_flat['topic_id'] = df_flat['topic_id'].astype(int)

    # --- Lógica de Reanudación ---
    out_path = Path(out_csv)
    processed_keys = set()
    write_header = not out_path.exists() or out_path.stat().st_size == 0

    if not write_header:
        try:
            df_processed = pd.read_csv(out_path)
            df_processed['_key'] = df_processed['appid'].astype(str) + "_" + df_processed['year_month'].astype(str) + "_" + df_processed['topic_id'].astype(str)
            processed_keys = set(df_processed['_key'])
            print(f"Se encontraron {len(processed_keys)} filas ya procesadas. Reanudando...")
        except Exception as e:
            print(f"Warning: No se pudo leer el archivo de salida existente. Se sobreescribirá. Error: {e}")
            write_header = True

    df_flat['_key'] = df_flat['appid'].astype(str) + "_" + df_flat['year_month'].astype(str) + "_" + df_flat['topic_id'].astype(str)
    df_todo = df_flat[~df_flat['_key'].isin(processed_keys)].drop(columns=['_key'])

    if df_todo.empty:
        print("No hay tópicos nuevos que procesar.")
        return out_path

    print(f"Faltan {len(df_todo)}/{len(df_flat)} filas por procesar.")

    # --- Procesamiento con Cache para el LLM ---
    use_llm = (provider == "deepseek") or (provider == "auto" and bool(os.getenv("DEEPSEEK_API_KEY")))
    label_cache = {}
    results = []

    for _, row in tqdm(df_todo.iterrows(), total=len(df_todo), desc="Humanizando tópicos"):
        name = row["topic_name_original"]
        keywords: List[str] = row["keywords"] if isinstance(row["keywords"], list) else []
        
        label = label_cache.get(name)
        used = "cache"

        if label is None:
            if use_llm:
                prompt = _build_prompt(name, keywords, max_keywords=max_keywords_per_topic)
                label = _call_deepseek(prompt)
                used = "deepseek" if label else "heuristic"
            
            if not label:
                label = _heuristic_label(name, keywords)
            
            label_cache[name] = label

        results.append({
            "appid": row["appid"],
            "year_month": row["year_month"],
            "topic_id": row["topic_id"],
            "label": label,
            "topic_name_original": name,
            "coherence_cv": row.get("coherence_cv"),
            "c_topics": row.get("c_topics"),
            "provider": used,
        })

    # --- Escritura Robusta con Pandas ---
    if results:
        df_results = pd.DataFrame(results)
        df_results.to_csv(out_path, mode='a', header=write_header, index=False, quoting=csv.QUOTE_ALL)

    print(f"[OK] Proceso finalizado. {len(results)} nuevas filas guardadas en -> {out_path}")
    return out_path


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Humaniza etiquetas de tópicos de BERTopic.")
    ap.add_argument(
        "--in",
        dest="topics_path",
        default="outputs/events/topics.parquet",
        help="Parquet/JSONL con tópicos anidados (schema: appid, year_month, topics: list)",
    )
    ap.add_argument(
        "--out",
        dest="out_csv",
        default="outputs/events/humanized_topics.csv",
        help="CSV de salida con etiquetas humanizadas para cada tópico.",
    )
    ap.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "deepseek", "heuristic"],
        help="Proveedor de etiquetas (auto → usa DeepSeek si hay API key)",
    )
    ap.add_argument("--max-keywords", type=int, default=10, help="Nº máximo de keywords por tópico para el prompt")
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    humanize_labels(
        topics_path=args.topics_path,
        out_csv=args.out_csv,
        provider=args.provider,
        max_keywords_per_topic=max(1, args.max_keywords),
    )


if __name__ == "__main__":
    main()