#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Genera JSONs de cliente a partir de los PoCs definidos en
Data_analytics/scripts/poc_assign_single_game.py (SAMPLE_GAMES y PROTOTYPE_GAMES).

Uso:
  docker compose exec -w /app/Data_analytics analytics \
    python scripts/generate_clients_from_poc.py --out-dir configs/clients

Por defecto crea ficheros JSON con id 'poc-<slug>' a partir del nombre/clave,
rellenando campos básicos (name, description/about_game, tags, price=None).
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any, Dict, List


def _slug(text: str) -> str:
    base = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower()).strip("-")
    return base or "unnamed"


def _to_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(x).strip() for x in value if str(x).strip()]
    text = str(value)
    if not text.strip():
        return []
    for sep in (";", "|"):
        text = text.replace(sep, ",")
    return [p.strip() for p in text.split(",") if p.strip()]


def _build_client_from_proto(entry: Dict[str, Any]) -> Dict[str, Any]:
    name = str(entry.get("name") or "PoC Game").strip()
    short = entry.get("short_description")
    detailed = entry.get("detailed_description")
    genres = _to_list(entry.get("genres"))
    categories = _to_list(entry.get("categories"))
    tags = genres + categories
    return {
        "name": name,
        "description": short or detailed or "",
        "about_game": detailed or short or "",
        "tags": tags or None,
        "price": None,
        "release_date": None,
        "languages": None,
    }


def _build_client_from_sample(key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    name = str(entry.get("name") or key).strip()
    short = entry.get("short_description")
    detailed = entry.get("detailed_description")
    genres = _to_list(entry.get("genres"))
    categories = _to_list(entry.get("categories"))
    tags = genres + categories
    return {
        "name": name,
        "description": short or detailed or "",
        "about_game": detailed or short or "",
        "tags": tags or None,
        "price": None,
        "release_date": None,
        "languages": None,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="configs/clients", help="Directorio de destino para los JSONs")
    args = ap.parse_args()

    # Importar los PoCs
    import importlib.util
    poc_path = Path(__file__).resolve().parent / "poc_assign_single_game.py"
    if not poc_path.exists():
        raise SystemExit(f"No se encontró {poc_path}")
    spec = importlib.util.spec_from_file_location("poc_assign_single_game", str(poc_path))
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    created: List[str] = []

    # PROTOTYPE_GAMES (lista de dicts)
    proto_list = getattr(mod, "PROTOTYPE_GAMES", []) or []
    for entry in proto_list:
        client = _build_client_from_proto(entry)
        cid = f"poc-{_slug(client['name'])}"
        path = out_dir / f"{cid}.json"
        path.write_text(json.dumps(client, ensure_ascii=False, indent=2), encoding="utf-8")
        created.append(str(path))

    # SAMPLE_GAMES (dict key -> dict)
    samples = getattr(mod, "SAMPLE_GAMES", {}) or {}
    for key, entry in samples.items():
        client = _build_client_from_sample(key, entry)
        cid = f"poc-{_slug(key)}"
        path = out_dir / f"{cid}.json"
        path.write_text(json.dumps(client, ensure_ascii=False, indent=2), encoding="utf-8")
        created.append(str(path))

    print("[OK] JSONs de cliente generados:")
    for p in created:
        print(" -", p)


if __name__ == "__main__":
    main()

