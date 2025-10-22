#!/usr/bin/env python
"""Humaniza y enriquece el fichero de explicaciones de eventos.

Lee `outputs/events/explanations.parquet`, y genera un CSV enriquecido con:
  - Summaries para listas de keywords usando un LLM.
  - Columnas booleanas de causa-efecto para tipos de noticias.

Este script es RESUMABLE: si se interrumpe, continuará donde se quedó.
"""
from __future__ import annotations

import argparse
import os
import re
import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import numpy as np
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


def _call_deepseek(prompt: str) -> Optional[str]:
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        return None
    try:
        import requests
    except ImportError:
        return None

    base = os.getenv("DEEPSEEK_BASE_URL") or os.getenv("DEEPSEEK_API_BASE") or "https://api.deepseek.com/v1"
    url = f"{base.rstrip('/')}/chat/completions" if base.endswith("/v1") else f"{base.rstrip('/')}/v1/chat/completions"

    system_prompt = (
        "You are an expert in summarizing keyword lists from video game news. "
        "Return only a short, descriptive 2-4 word Title Case summary in English, no quotes."
    )

    payload = {
        "model": os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        "messages": [
            {"role": "system", "content": system_prompt},
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
        return text.strip("'\"")
    except Exception:
        return None


def _build_summary_prompt(keywords: List[str]) -> str:
    kw_list = [str(k) for k in keywords if isinstance(k, str) and k.strip()]
    joined = ", ".join(kw_list)
    return f"Summarize these keywords into a 2-4 word title: {joined}"


def humanize_explanations(
    input_path: str | Path,
    output_csv: str | Path,
    provider: str = "auto",
) -> Path:
    df = _read_any(input_path)
    if df.empty:
        raise SystemExit("El archivo de explicaciones de entrada está vacío.")

    # --- 1. Añadir columnas booleanas ---
    for col in ['news_marketing', 'news_community', 'news_other']:
        if col in df.columns:
            df[f'possible_{col}'] = df[col].fillna(0) > 0

    # --- 2. Lógica de Reanudación ---
    out_path = Path(output_csv)
    processed_keys = set()
    write_header = not out_path.exists() or out_path.stat().st_size == 0

    if not write_header:
        try:
            df_processed = pd.read_csv(out_path)
            df_processed['_key'] = df_processed['appid'].astype(str) + "_" + df_processed['year_month'].astype(str)
            processed_keys = set(df_processed['_key'])
            print(f"Se encontraron {len(processed_keys)} filas ya procesadas. Reanudando...")
        except Exception as e:
            print(f"Warning: No se pudo leer el archivo de salida existente. Se sobreescribirá. Error: {e}")
            write_header = True

    df['_key'] = df['appid'].astype(str) + "_" + df['year_month'].astype(str)
    df_todo = df[~df['_key'].isin(processed_keys)].drop(columns=['_key'])

    if df_todo.empty:
        print("No hay filas nuevas que procesar.")
        return out_path

    print(f"Faltan {len(df_todo)}/{len(df)} filas por procesar.")

    # --- 3. Humanización con LLM y Cache ---
    use_llm = (provider == "deepseek") or (provider == "auto" and bool(os.getenv("DEEPSEEK_API_KEY")))
    summary_cache = {}
    results = []

    keyword_cols = ['news_keywords', 'news_patch_keywords']

    for _, row in tqdm(df_todo.iterrows(), total=len(df_todo), desc="Humanizando explicaciones"):
        row_dict = row.to_dict()
        if use_llm:
            for col in keyword_cols:
                summary_col_name = f"{col}_summary"
                keywords = row.get(col)
                if keywords is not None and len(keywords) > 0:
                    # Usar una tupla de keywords como clave de caché, ya que las listas no son hasheables
                    keywords_tuple = tuple(sorted(keywords))
                    summary = summary_cache.get(keywords_tuple)
                    
                    if summary is None:
                        prompt = _build_summary_prompt(keywords)
                        summary = _call_deepseek(prompt)
                        # Usar lista original como fallback si el LLM falla
                        summary_cache[keywords_tuple] = summary or ", ".join(keywords[:4])
                    
                    row_dict[summary_col_name] = summary_cache[keywords_tuple]
                else:
                    row_dict[summary_col_name] = None
        results.append(row_dict)

    # --- 4. Escritura Robusta con Pandas ---
    if results:
        df_results = pd.DataFrame(results)
        # Reordenar columnas para que las nuevas aparezcan al final
        existing_cols = [c for c in df.columns if c != '_key']
        new_cols = sorted([c for c in df_results.columns if c not in existing_cols])
        df_results = df_results[existing_cols + new_cols]
        df_results.to_csv(out_path, mode='a', header=write_header, index=False, quoting=csv.QUOTE_ALL)

    print(f"[OK] Proceso finalizado. {len(results)} nuevas filas guardadas en -> {out_path}")
    return out_path


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Enriquece y humaniza el fichero de explicaciones de eventos.")
    ap.add_argument(
        "--in",
        dest="input_path",
        default="outputs/events/explanations.parquet",
        help="Ruta al Parquet de entrada con las explicaciones de eventos.",
    )
    ap.add_argument(
        "--out",
        dest="output_csv",
        default="outputs/events/humanized_explanations.csv",
        help="Ruta al CSV de salida con los datos enriquecidos.",
    )
    ap.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "deepseek", "heuristic"],
        help="Proveedor para la humanización (auto → usa DeepSeek si hay API key)",
    )
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    humanize_explanations(
        input_path=args.input_path,
        output_csv=args.output_csv,
        provider=args.provider,
    )


if __name__ == '__main__':
    main()
