#!/usr/bin/env python
"""Summarize a 'topics' column into a concise, readable string.

Supports:
- Heuristic summarization (default, offline): parses Python/JSON-like topic structures.
- Optional DeepSeek LLM summarization if env vars are set and network available.

Input is a CSV/Parquet with at least a 'topics' column. Optionally includes
'appid' and a date column to help identify rows.
"""
from __future__ import annotations

import argparse
import ast
import re
import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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


def _safe_parse_topics(val: Any) -> List[Dict[str, Any]]:
    """Parse topics from str/list safely. Accepts:
    - Python-like list of dicts (using ast.literal_eval)
    - JSON string
    - Already a list of dicts
    Returns [] on failure.
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return [x for x in val if isinstance(x, dict)]
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        # Pre-clean numpy-style arrays: array([...], dtype=object) -> [...]
        def _preclean_numpy_arrays(txt: str) -> str:
            # Replace any numpy-like array(...) with just its list payload
            # Examples handled:
            #   array(['a','b'], dtype=object)
            #   array(["a", "b"])  (no dtype)
            #   'Representation': array([...], dtype=object)
            pattern = re.compile(r"array\(\s*(\[[\s\S]*?\])\s*(?:,\s*dtype=object)?\s*\)")
            return pattern.sub(r"\1", txt)

        # Optionally drop very large doc blobs to ease parsing (Representative_Docs, top_docs)
        def _drop_heavy_doc_blobs(txt: str) -> str:
            # Remove key: Representative_Docs: <anything balanced up to next '],'> heuristically
            txt = re.sub(r"'Representative_Docs'\s*:\s*\[[\s\S]*?\](\s*,)?", "", txt)
            txt = re.sub(r'"Representative_Docs"\s*:\s*\[[\s\S]*?\](\s*,)?', "", txt)
            txt = re.sub(r"'top_docs'\s*:\s*\[[\s\S]*?\](\s*,)?", "", txt)
            txt = re.sub(r'"top_docs"\s*:\s*\[[\s\S]*?\](\s*,)?', "", txt)
            return txt

        s = _drop_heavy_doc_blobs(_preclean_numpy_arrays(s))
        # Try JSON first
        try:
            obj = json.loads(s)
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
        except Exception:
            pass
        # Fallback to Python repr
        try:
            obj = ast.literal_eval(s)
            if isinstance(obj, list):
                return [x for x in obj if isinstance(x, dict)]
        except Exception:
            # Last resort: best-effort extraction of Name/Representation via regex
            try:
                items: List[Dict[str, Any]] = []
                # Extract {'Name': '...', 'Representation': [...]} pairs
                for m in re.finditer(r"[\{,]\s*'Name'\s*:\s*([^,\}]+)\s*,[\s\S]*?'Representation'\s*:\s*(\[[\s\S]*?\])", s):
                    name_raw = m.group(1).strip()
                    if name_raw.startswith("'") or name_raw.startswith('"'):
                        name = json.loads(name_raw.replace("'", '"')) if '"' in name_raw or '"' in name_raw else name_raw.strip("'\"")
                    else:
                        name = str(name_raw)
                    rep_txt = _preclean_numpy_arrays(m.group(2))
                    try:
                        rep_list = ast.literal_eval(rep_txt)
                        if isinstance(rep_list, list):
                            items.append({'Name': name, 'Representation': rep_list, 'Count': None})
                    except Exception:
                        items.append({'Name': name, 'Count': None})
                return items
            except Exception:
                return []
    return []


def _clean_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    # Drop leading index like '0_' and join tokens
    parts = name.split('_')
    if parts and parts[0].isdigit():
        parts = parts[1:]
    return " ".join([p for p in parts if p])


def _heuristic_summary(topics: List[Dict[str, Any]], max_items: int = 3, max_keywords: int = 5) -> str:
    if not topics:
        return ""
    # Order by Count desc if available
    def _count(d: Dict[str, Any]) -> int:
        try:
            return int(d.get('Count') or d.get('count') or 0)
        except Exception:
            return 0
    sorted_topics = sorted([t for t in topics if isinstance(t, dict)], key=_count, reverse=True)[:max_items]
    chunks: List[str] = []
    for t in sorted_topics:
        name = _clean_name(str(t.get('Name') or t.get('name') or ''))
        cnt = _count(t)
        rep = t.get('Representation')
        # Accept numpy arrays as well
        try:
            if hasattr(rep, 'tolist'):
                rep = rep.tolist()
        except Exception:
            pass
        if isinstance(rep, (list, tuple)):
            kws = [str(x) for x in rep if str(x).strip()][:max_keywords]
        else:
            kws = []
        part = name or ", ".join(kws)
        if cnt:
            chunks.append(f"{part} (n={cnt}; kw: {', '.join(kws)})")
        else:
            chunks.append(f"{part} (kw: {', '.join(kws)})")
    return "; ".join([c for c in chunks if c])


def _call_deepseek(prompt: str, api_key: Optional[str], api_base: Optional[str] = None, model: Optional[str] = None) -> Optional[str]:
    """Optional DeepSeek call. Requires requests installed and network.
    Resiliently falls back to None on any error.
    """
    api_key = api_key or os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        return None
    try:
        import requests  # type: ignore
    except Exception:
        return None
    api_base = api_base or os.getenv('DEEPSEEK_API_BASE', 'https://api.deepseek.com')
    model = model or os.getenv('DEEPSEEK_MODEL', 'deepseek-chat')
    url = api_base.rstrip('/') + '/v1/chat/completions'
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are an expert in summarizing topics from video game reviews. Produce exactly three words in English, Title Case, no punctuation."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 128,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=20)
        if resp.status_code != 200:
            return None
        data = resp.json()
        text = data.get('choices', [{}])[0].get('message', {}).get('content')
        return (text or '').strip() if isinstance(text, str) else None
    except Exception:
        return None


def _to_three_word_title(s: str) -> str:
    # Keep letters/numbers, split on whitespace, take first 3 tokens
    tokens = [t for t in str(s).replace('\n', ' ').split(' ') if t.strip()]
    if not tokens:
        return ''
    words = tokens[:3]
    title = ' '.join(w.capitalize() for w in words)
    # Remove stray punctuation and trim
    return ''.join(ch for ch in title if ch.isalnum() or ch.isspace()).strip()


def summarize_row(topics_raw: Any, provider: str = 'heuristic', max_items: int = 3) -> str:
    topics = _safe_parse_topics(topics_raw)
    if provider == 'deepseek':
        # Enforce a strict three-word, Title Case output for deepseek provider
        prompt = (
            "Create a three-word Title Case topic name summarizing these topics. "
            "Return only the three words.\n" + json.dumps(topics)[:3000]
        )
        out = _call_deepseek(prompt, api_key=os.getenv('DEEPSEEK_API_KEY'))
        if out:
            return _to_three_word_title(out)
        # Fallback to heuristic if LLM not available, but still enforce 3-word Title Case
        return _to_three_word_title(_heuristic_summary(topics, max_items=max_items))
    # Heuristic provider: keep original format
    return _heuristic_summary(topics, max_items=max_items)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Summarize a 'topics' column into a readable text column")
    ap.add_argument('--in', dest='input_path', required=True, help='Input CSV/Parquet with a topics column')
    ap.add_argument('--out', dest='output_path', required=True, help='Output CSV/Parquet path')
    ap.add_argument('--topics-col', default='topics', help='Column name containing topics')
    ap.add_argument('--summary-col', default='topics_summary', help='Output column name')
    ap.add_argument('--provider', default='heuristic', choices=['heuristic','deepseek'], help='Summarization provider')
    ap.add_argument('--max-items', type=int, default=3, help='Max topics to include')
    return ap.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    df = _read_any(args.input_path)
    if args.topics_col not in df.columns:
        raise SystemExit(f"Column '{args.topics_col}' not found in input")
    out = df.copy()
    out[args.summary_col] = out[args.topics_col].apply(lambda x: summarize_row(x, provider=args.provider, max_items=max(1, args.max_items)))
    p = Path(args.output_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.suffix.lower() == '.csv':
        out.to_csv(p, index=False)
    elif p.suffix.lower() == '.json':
        out.to_json(p, orient='records', lines=False)
    else:
        out.to_parquet(p, index=False)
    print(f"[OK] Wrote summarized topics -> {p}")


if __name__ == '__main__':
    main()

