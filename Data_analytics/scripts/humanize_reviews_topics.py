#!/usr/bin/env python
"""Humaniza etiquetas (labels) para tópicos de BERTopic por reseña.

Lee `outputs/events/reviews_topics.parquet` (por defecto) y genera un mapping
`outputs/events/reviews_topics_labels.csv` con columnas:
  - topic_id (str)
  - label (str)               → etiqueta humanizada final
  - topic_name_original (str) → nombre original (si existía)
  - provider (str)            → deepseek|heuristic
  - lang (str)                → es|en
  - samples (int)             → nº de reseñas usadas para el prompt

Si está configurada la variable de entorno `DEEPSEEK_API_KEY` y hay red,
utiliza la API de DeepSeek; si no, usa un heurístico que limpia y titula.
"""
from __future__ import annotations

import argparse
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


def _read_any(path: str | Path) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Input file not found: {p}")
    suf = p.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(p)
    if suf == ".json":
        return pd.read_json(p)
    return pd.read_parquet(p)


def _to_title_2_4_words(s: str, min_words: int = 2, max_words: int = 4) -> str:
    tokens = [t for t in re.split(r"\W+", str(s)) if t]
    if not tokens:
        return ""
    words = tokens[: max(min_words, min(max_words, len(tokens)))]
    title = " ".join(w.capitalize() for w in words)
    return title.strip()


def _call_deepseek(prompt: str, lang: str = "es") -> Optional[str]:
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        return None
    try:
        import requests  # type: ignore
    except Exception:
        return None

    base = os.getenv("DEEPSEEK_BASE_URL") or os.getenv("DEEPSEEK_API_BASE") or "https://api.deepseek.com/v1"
    url = f"{base.rstrip('/')}/chat/completions" if base.endswith("/v1") else f"{base.rstrip('/')}/v1/chat/completions"

    system_es = (
        "Eres experto etiquetando tópicos de reseñas de videojuegos. "
        "Devuelve solo una etiqueta breve (2-4 palabras, Title Case) en español, sin comillas."
    )
    system_en = (
        "You label topics from video game reviews. "
        "Return only a short 2-4 word Title Case label in English, no quotes."
    )
    system = system_es if str(lang).lower().startswith("es") else system_en

    payload = {
        "model": os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 64,
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    try:
        # Compatibilidad con /v1/chat/completions y sin /v1
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        text = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content")
        )
        if not isinstance(text, str) or not text.strip():
            return None
        return text.strip()
    except Exception:
        return None


def _build_prompt(topic_id: str, name: str, snippets: List[str], max_snippets: int = 6) -> str:
    snips = [s for s in snippets if isinstance(s, str) and s.strip()][:max_snippets]
    joined = "\n- ".join(snips)
    return (
        f"Topic ID: {topic_id}\n"
        f"Original name: {name}\n"
        f"Representative review snippets (bulleted):\n- {joined}\n"
        "Label with 2-4 words."
    )


def _heuristic_label(name: str, snippets: List[str]) -> str:
    base = name or ""
    if not base and snippets:
        # usa tokens frecuentes de snippets como respaldo
        tokens = re.findall(r"\w+", " ".join(snippets).lower())
        common = [w for w, _ in Counter(tokens).most_common(4)]
        base = " ".join(common)
    return _to_title_2_4_words(base)


def humanize_labels(
    topics_path: str | Path,
    out_csv: str | Path,
    lang: str = "es",
    provider: str = "auto",
    max_snippets_per_topic: int = 6,
) -> Path:
    df = _read_any(topics_path)
    if df.empty:
        raise SystemExit("Dataset de tópicos por reseña vacío.")

    # columnas mínimas
    if "topic_id" not in df.columns:
        # permitir fallback cuando solo existe topic_name
        df = df.copy()
        df["topic_id"] = df.get("topic_name", pd.Series(range(len(df)))).astype(str)
    if "topic_name" not in df.columns:
        df = df.copy()
        df["topic_name"] = df["topic_id"].astype(str)

    # agrupar por topic_id y construir prompts
    grouped = (
        df.groupby("topic_id", dropna=False)
        .agg(
            topic_name_original=("topic_name", lambda s: str(s.dropna().iloc[0]) if len(s.dropna()) else ""),
            snippets=("snippet", lambda s: [x for x in s.dropna().astype(str).tolist()][:max_snippets_per_topic]),
        )
        .reset_index()
    )

    labels: List[Dict[str, Any]] = []
    use_llm = (provider == "deepseek") or (provider == "auto" and bool(os.getenv("DEEPSEEK_API_KEY")))

    for _, row in grouped.iterrows():
        tid = str(row["topic_id"])
        name = str(row["topic_name_original"]) if row["topic_name_original"] else ""
        snippets: List[str] = row["snippets"] if isinstance(row["snippets"], list) else []

        label: Optional[str] = None
        used = "heuristic"
        if use_llm:
            prompt = _build_prompt(tid, name, snippets, max_snippets=max_snippets_per_topic)
            label = _call_deepseek(prompt, lang=lang)
            used = "deepseek" if label else "heuristic"
        if not label:
            label = _heuristic_label(name, snippets)

        labels.append(
            {
                "topic_id": tid,
                "label": label,
                "topic_name_original": name,
                "provider": used,
                "lang": lang,
                "samples": len(snippets),
            }
        )

    out_path = Path(out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(labels).to_csv(out_path, index=False)
    print(f"[OK] Labels humanizadas -> {out_path} ({len(labels)})")
    return out_path


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Humaniza etiquetas de tópicos de BERTopic por reseña")
    ap.add_argument(
        "--in",
        dest="topics_path",
        default="outputs/events/reviews_topics.parquet",
        help="Parquet/CSV/JSON con columnas: review_id, topic_id, topic_name, snippet",
    )
    ap.add_argument(
        "--out",
        dest="out_csv",
        default="outputs/events/reviews_topics_labels.csv",
        help="CSV de salida con mapping de labels por topic_id",
    )
    ap.add_argument("--lang", default="es", choices=["es", "en"], help="Idioma de la etiqueta devuelta")
    ap.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "deepseek", "heuristic"],
        help="Proveedor de etiquetas (auto → usa DeepSeek si hay API key)",
    )
    ap.add_argument("--max-snippets", type=int, default=6, help="Nº máximo de snippets por tópico para el prompt")
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    humanize_labels(
        topics_path=args.topics_path,
        out_csv=args.out_csv,
        lang=args.lang,
        provider=args.provider,
        max_snippets_per_topic=max(1, args.max_snippets),
    )


if __name__ == "__main__":
    main()

