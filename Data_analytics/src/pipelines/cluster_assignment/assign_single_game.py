#!/usr/bin/env python
from __future__ import annotations

"""CLI to embed a single game and assign it to the best existing cluster."""

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import numpy as np
import pandas as pd
import yaml
from sentence_transformers import SentenceTransformer

from src.pipelines.generate_embeddings import _build_doc
from src.pipelines.cluster_assignment.assign_new_games import _assign_cluster
from src.utils.io import write_parquet_any


def _load_config(config_path: Path) -> Dict[str, Any]:
    if not config_path.exists():
        raise SystemExit(f"Config file not found: {config_path}")
    return yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}


def _normalize_tokens(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        raw_items = [raw]
    else:
        raw_items = list(raw)
    values: list[str] = []
    for item in raw_items:
        if not isinstance(item, str):
            continue
        for part in item.split(','):
            part = part.strip()
            if part:
                values.append(part)
    return values


def _prompt(label: str) -> str:
    try:
        return input(f"{label}: ").strip()
    except EOFError:
        return ""


def _collect_game_fields(args: argparse.Namespace, doc_fields: Dict[str, Any], interactive: bool) -> Dict[str, Any]:
    collected: Dict[str, Any] = {}
    text_fields = doc_fields.get("text_fields") or []
    tag_fields = doc_fields.get("tag_fields") or []

    for field in text_fields:
        value = getattr(args, field, None)
        if (value is None or value == "") and interactive:
            prompt_label = field.replace('_', ' ').title()
            value = _prompt(prompt_label)
        if (value is None or value == "") and not interactive:
            raise SystemExit(f"Missing value for text field '{field}'.")
        collected[field] = value or ""

    for field in tag_fields:
        raw_value = getattr(args, field, None)
        if raw_value in (None, []) and interactive:
            prompt_label = field.replace('_', ' ').title()
            raw_value = _prompt(f"{prompt_label} (comma separated, optional)")
        collected[field] = _normalize_tokens(raw_value)

    return collected


def _build_parser(doc_fields: Dict[str, Any]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Embed a single game description and assign it to an existing cluster",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="Data_analytics/configs/embeddings.yaml",
        help="Path to the embeddings configuration file.",
    )
    parser.add_argument(
        "--medoids",
        default="Data_analytics/models/cluster_medoids.json",
        help="Path to the JSON file with cluster medoids/centroids.",
    )
    parser.add_argument(
        "--appid",
        help="Custom identifier for the game (if omitted, prompts or falls back to a generated value).",
    )
    parser.add_argument(
        "--out",
        help="Optional Parquet output path to store the assignment.",
    )
    parser.add_argument(
        "--cluster-version",
        dest="cluster_version",
        help="Cluster version label to store when writing the assignment.",
    )
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Do not prompt for missing values (fails instead).",
    )

    for field in doc_fields.get("text_fields", []) or []:
        parser.add_argument(
            f"--{field.replace('_', '-')}",
            dest=field,
            help=f"Value for text field '{field}'.",
        )
    for field in doc_fields.get("tag_fields", []) or []:
        parser.add_argument(
            f"--{field.replace('_', '-')}",
            dest=field,
            nargs="*",
            help=f"Values for tag field '{field}' (space or comma separated).",
        )
    return parser


def main() -> None:
    import sys

    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--config", default="Data_analytics/configs/embeddings.yaml")
    pre_args, _ = pre_parser.parse_known_args()
    initial_config = _load_config(Path(pre_args.config))
    doc_fields = initial_config.get("document_fields") or {}

    parser = _build_parser(doc_fields)
    args = parser.parse_args()

    config = _load_config(Path(args.config))
    doc_fields = config.get("document_fields") or {}
    interactive = not args.non_interactive

    game_fields = _collect_game_fields(args, doc_fields, interactive)
    appid = args.appid
    if (appid is None or appid == "") and interactive:
        appid = _prompt("AppID") or f"manual-{int(datetime.now().timestamp())}"
    if appid is None or appid == "":
        appid = f"manual-{int(datetime.now().timestamp())}"

    model_name = config.get("embedding_model")
    if not model_name:
        raise SystemExit("Embedding model not defined in config.")
    normalize = bool(config.get("normalize_embeddings", False))

    document = _build_doc({**game_fields, "appid": appid}, doc_fields, config.get("assemble"))
    model = SentenceTransformer(model_name)
    embedding = model.encode([document], normalize_embeddings=normalize, show_progress_bar=False)[0]

    medoids_path = Path(args.medoids)
    if not medoids_path.exists():
        raise SystemExit(f"Medoids file not found: {medoids_path}")
    medoids = json.loads(medoids_path.read_text(encoding="utf-8"))

    cluster_id = _assign_cluster(np.asarray(embedding, dtype=np.float32), medoids)
    if cluster_id is None:
        raise SystemExit("No cluster could be assigned (medoids may be empty).")

    print("================ Result ================")
    print(f"AppID           : {appid}")
    print(f"Cluster ID      : {cluster_id}")
    print(f"Embedding model : {model_name}")

    if args.out:
        cluster_version = args.cluster_version or datetime.now().strftime("%Y%m")
        assigned_date = pd.Timestamp.now().normalize()
        df = pd.DataFrame(
            [
                {
                    "appid": str(appid),
                    "cluster_id": int(cluster_id),
                    "cluster_version": str(cluster_version),
                    "assigned_date": assigned_date,
                }
            ]
        )
        write_parquet_any(df, args.out)
        print(f"Assignment saved to {args.out}")


if __name__ == "__main__":
    main()
