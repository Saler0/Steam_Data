#!/usr/bin/env python
"""PoC para asignar un juego manual a clústeres existentes usando SentenceTransformer."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

import numpy as np
import yaml
from sentence_transformers import SentenceTransformer

# Permite reutilizar utilidades internas sin modificar PYTHONPATH global
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipelines.generate_embeddings import _build_doc

# Prototipos de medoids de respaldo para que el PoC funcione incluso sin haber corrido el pipeline completo
PROTOTYPE_GAMES: List[Dict[str, Any]] = [
    {
        "cluster_id": 101,
        "name": "Nightfall Survivors",
        "short_description": "Acción roguelike con oleadas infinitas de vampiros y mejoras evolutivas.",
        "detailed_description": (
            "Enfréntate a hordas interminables de criaturas nocturnas en partidas de 20 minutos, "
            "colecciona reliquias y crea combinaciones rotas para sobrevivir hasta el amanecer."
        ),
        "genres": ["Action", "Roguelike", "Survival"],
        "categories": ["Single-player", "Controller Support"],
    },
    {
        "cluster_id": 202,
        "name": "Valley Bloom",
        "short_description": "Simulador agrícola relajado centrado en relaciones y decoración.",
        "detailed_description": (
            "Cultiva la granja de tus sueños, cría animales, participa en festivales de temporada "
            "y forja amistades profundas con los habitantes del valle."
        ),
        "genres": ["Simulation", "Casual", "Farming"],
        "categories": ["Single-player"],
    },
    {
        "cluster_id": 303,
        "name": "Neon Nexus Tactics",
        "short_description": "Constructor de mazos cyberpunk con combates tácticos por turnos.",
        "detailed_description": (
            "Dirige a un equipo de hackers mercenarios, combina cartas de habilidades y mejora chips "
            "augmentados para infiltrarte en megacorporaciones rivales."
        ),
        "genres": ["Strategy", "Card Game", "RPG"],
        "categories": ["Single-player", "Steam Deck Verified"],
    },
]

SAMPLE_GAMES: Dict[str, Dict[str, Any]] = {
    "vampire": {
        "name": "Crimson Tide",
        "short_description": "Shooter roguelike de hordas con mejoras acumulativas cada minuto.",
        "detailed_description": (
            "Desata armas absurdas mientras sobrevives a oleadas crecientes, desbloquea sinergias "
            "y recoge sangre para evolucionar habilidades en pleno combate.") ,
        "genres": ["Action", "Roguelike", "Bullet Hell"],
        "categories": ["Single-player"],
    },
    "farm": {
        "name": "Sunrise Ranch",
        "short_description": "Gestiona una granja costera y enamórate del pueblo vecino.",
        "detailed_description": (
            "Planta, pesca y cuida animales mientras restauras el pueblo y construyes relaciones "
            "con personajes únicos en un ambiente acogedor."),
        "genres": ["Simulation", "Farming", "Casual"],
        "categories": ["Single-player", "Relaxing"],
    },
    "deck": {
        "name": "Gridbreak Protocol",
        "short_description": "Deckbuilder táctico ambientado en una distopía tecnológica.",
        "detailed_description": (
            "Combina cartas hacking, drones y tácticas de sigilo para superar misiones "
            "procedurales contra IA hostil."),
        "genres": ["Strategy", "Card Game", "Roguelike"],
        "categories": ["Single-player"],
    },
}


def _load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"No se encontró el archivo de configuración: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _ensure_unit(vec: np.ndarray) -> np.ndarray:
    norm = np.linalg.norm(vec)
    if norm == 0:
        return vec
    return vec / norm


def _embed_documents(model: SentenceTransformer, docs: Iterable[str], normalize: bool) -> List[np.ndarray]:
    embeddings = model.encode(list(docs), normalize_embeddings=normalize, show_progress_bar=False)
    vectors = [np.asarray(emb, dtype=np.float32) for emb in embeddings]
    if not normalize:
        vectors = [_ensure_unit(vec) for vec in vectors]
    return vectors


def _load_or_build_medoids(
    medoids_path: Path,
    model: SentenceTransformer,
    doc_fields: Dict[str, Any],
    assemble_cfg: Dict[str, Any] | None,
    normalize: bool,
) -> Dict[str, np.ndarray]:
    if medoids_path.exists():
        data = json.loads(medoids_path.read_text(encoding="utf-8"))
        medoids = {str(cid): _ensure_unit(np.asarray(vec, dtype=np.float32)) for cid, vec in data.items()}
        return medoids

    print("[INFO] No se encontraron medoids reales; se usarán prototipos de respaldo.")
    docs = [_build_doc(proto, doc_fields, assemble_cfg) for proto in PROTOTYPE_GAMES]
    vectors = _embed_documents(model, docs, normalize)
    return {str(proto["cluster_id"]): vec for proto, vec in zip(PROTOTYPE_GAMES, vectors)}


def _prepare_sample(args: argparse.Namespace) -> Dict[str, Any]:
    if args.name or args.short_description or args.detailed_description:
        sample = {
            "name": args.name or "",
            "short_description": args.short_description or "",
            "detailed_description": args.detailed_description or "",
            "genres": args.genres or [],
            "categories": args.categories or [],
        }
        return sample
    return SAMPLE_GAMES[args.scenario]


def _format_similarity_table(scores: List[tuple[str, float]]) -> str:
    header = "cluster_id | similitud"
    lines = [header, "-" * len(header)]
    for cid, score in scores:
        lines.append(f"{cid:>10} | {score: .4f}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="PoC: Embebe un juego y muestra el clúster más cercano.")
    parser.add_argument("--config", default="configs/embeddings.yaml", help="Config de embeddings a reutilizar.")
    parser.add_argument(
        "--medoids",
        default="models/cluster_medoids.json",
        help="Ruta al JSON de medoids. Si no existe, se crean medoids prototipo.",
    )
    parser.add_argument(
        "--scenario",
        choices=sorted(SAMPLE_GAMES.keys()),
        default="vampire",
        help="Escenario de ejemplo a evaluar cuando no se pasan textos personalizados.",
    )
    parser.add_argument("--name", help="Nombre del juego personalizado.")
    parser.add_argument("--short-description", dest="short_description", help="Descripción corta personalizada.")
    parser.add_argument("--detailed-description", dest="detailed_description", help="Descripción larga personalizada.")
    parser.add_argument("--genres", nargs="*", help="Lista de géneros para un juego personalizado.")
    parser.add_argument("--categories", nargs="*", help="Lista de categorías para un juego personalizado.")
    parser.add_argument(
        "--show-doc",
        action="store_true",
        help="Imprime el documento ensamblado antes de generar el embedding.",
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.0,
        help="Umbral mínimo de similitud coseno para mostrar vecinos.",
    )
    parser.add_argument(
        "--max-neighbors",
        type=int,
        default=None,
        help="Máximo de vecinos a listar (None = todos los que superen el umbral).",
    )
    args = parser.parse_args()

    config = _load_config(Path(args.config))
    doc_fields = config.get("document_fields") or {}
    assemble_cfg = config.get("assemble")
    normalize = bool(config.get("normalize_embeddings", False))

    model_name = config.get("embedding_model")
    if not model_name:
        raise SystemExit("La config no define 'embedding_model'.")
    print(f"[INFO] Cargando modelo de embeddings: {model_name}")
    model = SentenceTransformer(model_name)

    medoids = _load_or_build_medoids(Path(args.medoids), model, doc_fields, assemble_cfg, normalize)

    sample = _prepare_sample(args)
    doc = _build_doc(sample, doc_fields, assemble_cfg)
    if args.show_doc:
        print("\n===== Documento ensamblado =====\n")
        print(doc)
        print("\n================================\n")

    sample_vec = _embed_documents(model, [doc], normalize)[0]

    scores = []
    for cid, vec in medoids.items():
        score = float(np.dot(sample_vec, vec))
        scores.append((cid, score))
    scores.sort(key=lambda x: x[1], reverse=True)

    if not scores:
        raise SystemExit("No se pudieron calcular similitudes contra los medoids.")

    best_cluster, best_score = scores[0]
    print("================= RESULTADO =================")
    print(f"Mejor clúster : {best_cluster}")
    print(f"Similitud     : {best_score:.4f}")

    filtered = [item for item in scores if item[1] >= args.min_similarity]
    if args.max_neighbors is not None and args.max_neighbors > 0:
        filtered = filtered[: args.max_neighbors]

    if not filtered:
        print("\nNo hay vecinos que superen el umbral de similitud solicitado.")
    else:
        print(f"\nRanking de vecinos (>= {args.min_similarity:.4f}) - mostrando {len(filtered)} de {len(scores)} medoids")
        print(_format_similarity_table(filtered))



if __name__ == "__main__":
    main()

