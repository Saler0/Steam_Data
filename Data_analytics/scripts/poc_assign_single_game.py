#!/usr/bin/env python
"""PoC para asignar un juego manual a clústeres existentes usando SentenceTransformer."""
from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List
from copy import deepcopy

import numpy as np
import pandas as pd
import yaml
from sentence_transformers import SentenceTransformer

# Permite reutilizar utilidades internas sin modificar PYTHONPATH global
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.pipelines.generate_embeddings import _build_doc
from src.utils.io import read_parquet_any
from src.insights.neighbor_strategy import DEFAULT_CONFIG, EmbeddingIndex, select_competitor_neighbors
try:
    import mlflow
    from src.utils.mlflow_utils import (
        start_mlflow_run,
        log_mlflow_params,
        log_mlflow_metrics,
        log_mlflow_artifacts,
    )
except Exception:  # pragma: no cover
    mlflow = None
    start_mlflow_run = None
    log_mlflow_params = None
    log_mlflow_metrics = None
    log_mlflow_artifacts = None


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
        "short_description": "Roguelike horde shooter with cumulative upgrades every minute.",
        "detailed_description": (
            "Unleash absurd weapons as you survive escalating waves, unlock synergies, "
            "and collect blood to evolve abilities mid-battle."
        ),
        "genres": ["Action", "Roguelike", "Bullet Hell"],
        "categories": ["Single-player"],
    },
    "farm": {
        "name": "Sunrise Ranch",
        "short_description": "Manage a seaside farm and fall in love with the neighbouring town.",
        "detailed_description": (
            "Plant, fish, and care for animals while restoring the village and building relationships "
            "with unique characters in a cozy atmosphere."
        ),
        "genres": ["Simulation", "Farming", "Casual"],
        "categories": ["Single-player", "Relaxing"],
    },
    "deck": {
        "name": "Gridbreak Protocol",
        "short_description": "A tactical deckbuilder set in a high-tech dystopia.",
        "detailed_description": (
            "Combine hacking cards, drones, and stealth tactics to complete procedural missions "
            "against hostile AI networks."
        ),
        "genres": ["Strategy", "Card Game", "Roguelike"],
        "categories": ["Single-player"],
    },
    "echoes": {
        "name": "Echoes of the Abyss",
        "short_description": (
            "A survival exploration game set in a bioluminescent cave system, "
            "where sound is both a weapon and a guide."
        ),
        "detailed_description": (
            "In Echoes of the Abyss, players step into the role of an explorer trapped in an "
            "ever-shifting underground world. The caves are alive with dangers: creatures that "
            "hunt through sound, fungi that glow in response to your movement, and shifting "
            "tunnels that force constant adaptation. "
            "Instead of light, your survival depends on sound. Use echoes to navigate, distract "
            "predators with thrown objects, and play instruments to interact with mysterious species. "
            "Along the way, uncover the lost history of a civilization that once harnessed acoustics "
            "as a source of power. "
            "The game emphasizes atmosphere, immersion, and replayability through procedurally "
            "generated cave networks and branching narrative paths. Every run is a new descent into the unknown."
        ),
        "genres": ["Action", "Adventure", "Indie", "Simulation"],
        "categories": [
            "Single-player",
            "Steam Achievements",
            "Steam Cloud",
            "Full controller support",
            "Captions available",
            "Adjustable Difficulty",
        ],
    },
    "stellar": {
        "name": "Stellar Dominion",
        "short_description": (
            "A grand strategy game of interstellar conquest where every decision "
            "shapes the fate of galaxies."
        ),
        "detailed_description": (
            "In Stellar Dominion, you command a rising civilization in a distant future where "
            "countless factions struggle for control over the galaxy’s dwindling resources. "
            "The game combines large-scale strategic planning with deep political and technological systems. "
            "Players must expand their empire by colonizing new planets, researching advanced technologies, "
            "and forging fragile alliances—or breaking them in decisive wars. "
            "Each choice carries long-term consequences: do you pursue diplomacy to unite the stars, "
            "or domination to rule them with force? "
            "Dynamic AI factions react to your playstyle, creating unpredictable rivalries and alliances. "
            "Procedurally generated star systems ensure no two campaigns are the same, "
            "while branching story events push players to balance survival, ambition, and morality in a harsh universe."
        ),
        "genres": ["Strategy", "Simulation", "Indie"],
        "categories": [
            "Single-player",
            "Steam Achievements",
            "Steam Cloud",
            "Full controller support",
            "Adjustable Difficulty",
            "Stats",
        ],
    },
    "harvest": {
        "name": "Harvest Haven",
        "short_description": (
            "A relaxing farming simulation where you build your dream farm, "
            "raise animals, and cultivate thriving crops."
        ),
        "detailed_description": (
            "In Harvest Haven, players inherit an abandoned farm on the outskirts of a peaceful valley. "
            "Your goal is to restore it to life by planting crops, raising animals, and building a thriving homestead. "
            "You’ll manage seasonal cycles, care for livestock, and experiment with crop rotations to maximize harvests. "
            "Beyond farming, the valley holds a lively community of characters to befriend, festivals to join, "
            "and hidden secrets to uncover. "
            "The game emphasizes creativity and relaxation: customize your farm layout, breed unique animals, "
            "and craft tools and recipes to expand your possibilities. "
            "Whether you prefer carefully planning your fields or simply enjoying the calm pace of rural life, "
            "Harvest Haven offers an immersive farming experience."
        ),
        "genres": ["Simulation", "Casual", "Indie"],
        "categories": [
            "Single-player",
            "Steam Achievements",
            "Steam Cloud",
            "Family Sharing",
            "Adjustable Difficulty",
            "Full controller support",
        ],
    },
    "zombie": {
        "name": "Outbreak Protocol",
        "short_description": "Co-op survival shooter with relentless waves of mutants and visceral combat.",
        "detailed_description": (
            "In Outbreak Protocol, a catastrophic biohazard event has plunged humanity into chaos. "
            "Form elite squads and hold the line against escalating waves of infected across ruined cities, "
            "abandoned labs, and quarantined zones. "
            "Each round intensifies the challenge with new mutant types, unique bosses, "
            "and dynamic environmental conditions that reshape strategy. "
            "Wield a diverse arsenal — from improvised weapons to advanced military gear — "
            "and customize your loadout between waves. "
            "Progression is cooperative: unlock specialized classes, passive perks, and team synergies "
            "to survive longer. "
            "Inspired by genre classics like Killing Floor 2, Left 4 Dead, and World War Z, "
            "Outbreak Protocol delivers an intense, replayable experience focused on teamwork and tension."
        ),
        "genres": ["Action", "Shooter", "Co-op", "Survival"],
        "categories": [
            "Single-player",
            "Online Co-op",
            "Steam Achievements",
            "Steam Cloud",
            "Partial Controller Support",
            "Stats",
            "Co-op Campaign",
        ],
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


def _load_optional_df(path_str: str | None) -> pd.DataFrame:
    if not path_str:
        return pd.DataFrame()
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()
    try:
        suffix = path.suffix.lower()
        if suffix in {'.parquet', '.pq'}:
            return read_parquet_any(path)
        if suffix == '.csv':
            return pd.read_csv(path)
        if suffix == '.tsv':
            return pd.read_csv(path, sep='	')
        if suffix == '.json':
            return pd.read_json(path)
        return read_parquet_any(path)
    except Exception:
        return pd.DataFrame()


def _vector_from_value(value: Any) -> np.ndarray | None:
    if isinstance(value, np.ndarray):
        return value.astype(np.float32)
    if isinstance(value, list):
        return np.asarray(value, dtype=np.float32)
    if isinstance(value, str):
        try:
            return np.asarray(json.loads(value), dtype=np.float32)
        except Exception:
            return None
    return None


def _prepare_lookup(df: pd.DataFrame, key: str, value: str) -> dict:
    if df.empty or key not in df.columns or value not in df.columns:
        return {}
    series = df[[key, value]].dropna()
    try:
        series[key] = series[key].astype(str)
    except Exception:
        series[key] = series[key].apply(lambda x: str(x) if x is not None else None)
    return {row[key]: row[value] for _, row in series.iterrows()}


def _prepare_query_metadata(sample: Dict[str, Any]) -> Dict[str, Any]:
    def _clean(values: Any) -> List[str]:
        if not values:
            return []
        if isinstance(values, (list, tuple, set)):
            items = values
        else:
            items = str(values).split(',')
        out: List[str] = []
        for item in items:
            token = str(item).strip()
            if token:
                out.append(token)
        return out

    genres = _clean(sample.get('genres'))
    tags = _clean(sample.get('tags')) or genres
    categories = _clean(sample.get('categories'))
    price = sample.get('price')
    try:
        price_val = float(price) if price is not None else None
    except Exception:
        price_val = None
    is_free = None
    if price_val is not None:
        is_free = price_val == 0
    elif isinstance(sample.get('is_free'), bool):
        is_free = sample['is_free']

    modes = []
    for token in categories:
        low = token.lower()
        if 'pvp' in low:
            modes.append('pvp')
        if 'pve' in low:
            modes.append('pve')
        if 'coop' in low or 'co-op' in low:
            modes.append('coop')
        if 'single' in low:
            modes.append('singleplayer')
    modes = sorted(set(modes))

    return {
        'genres': genres,
        'tags': tags,
        'categories': categories,
        'modes': modes,
        'price': price_val,
        'is_free': is_free,
        'name': sample.get('name'),
    }



def _neighbors_with_strategy(
    sample_vec: np.ndarray,
    sample_metadata: Dict[str, Any],
    query_cluster_id: int | None,
    emb_df: pd.DataFrame,
    clusters_df: pd.DataFrame,
    metadata_df: pd.DataFrame,
    medoids: Dict[str, np.ndarray],
    strategy_cfg: Dict[str, Any],
    faiss_index_path: str | None = None,
    faiss_ids_path: str | None = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if emb_df.empty or 'embedding' not in emb_df.columns:
        return [], {}
    df = emb_df.copy()
    if 'appid' not in df.columns and 'app_id' in df.columns:
        df = df.rename(columns={'app_id': 'appid'})
    if 'appid' not in df.columns:
        return [], {}
    df = df[['appid', 'embedding']].dropna()
    df['appid'] = df['appid'].astype(str)
    index = EmbeddingIndex.from_dataframe(
        df,
        faiss_index_path=faiss_index_path,
        faiss_ids_path=faiss_ids_path,
    )
    if not index.ids:
        return [], {}

    cluster_df = clusters_df.copy()
    if not cluster_df.empty:
        if 'app_id' in cluster_df.columns and 'appid' not in cluster_df.columns:
            cluster_df = cluster_df.rename(columns={'app_id': 'appid'})
        if 'appid' in cluster_df.columns:
            cluster_df['appid'] = cluster_df['appid'].astype(str)

    meta_df = metadata_df.copy()
    if not meta_df.empty and 'appid' in meta_df.columns:
        meta_df['appid'] = meta_df['appid'].astype(str)

    user_cfg = strategy_cfg.copy() if strategy_cfg else {}

    neighbors, diagnostics = select_competitor_neighbors(
        query_vec=sample_vec,
        query_metadata=sample_metadata,
        query_appid=None,
        query_cluster_id=query_cluster_id,
        embeddings=index,
        clusters_df=cluster_df,
        metadata_df=meta_df,
        medoids=medoids,
        user_cfg=user_cfg,
    )
    return neighbors, diagnostics




def _find_neighbors(sample_vec: np.ndarray,
                    emb_df: pd.DataFrame,
                    clusters_df: pd.DataFrame,
                    metadata_df: pd.DataFrame,
                    top_k: Optional[int],
                    min_similarity: float) -> List[Dict[str, Any]]:
    if emb_df.empty or 'embedding' not in emb_df.columns:
        return []
    df = emb_df.copy()
    if 'appid' not in df.columns and 'app_id' in df.columns:
        df = df.rename(columns={'app_id': 'appid'})
    if 'appid' not in df.columns:
        return []
    df['appid'] = df['appid'].astype(str)
    vectors: List[np.ndarray] = []
    ids: List[str] = []
    for _, row in df.iterrows():
        vec = _vector_from_value(row.get('embedding'))
        if vec is None:
            continue
        vectors.append(_ensure_unit(vec))
        ids.append(row.get('appid'))
    if not vectors:
        return []
    matrix = np.vstack(vectors)
    query = _ensure_unit(sample_vec.astype(np.float32))
    sims = matrix @ query
    order = np.argsort(-sims)
    clusters_lookup = _prepare_lookup(clusters_df.rename(columns={'app_id': 'appid'}) if 'app_id' in clusters_df.columns else clusters_df, 'appid', 'cluster_id')
    names_lookup = _prepare_lookup(metadata_df.rename(columns={'app_id': 'appid'}) if 'app_id' in metadata_df.columns else metadata_df, 'appid', 'name')
    neighbors: List[Dict[str, Any]] = []
    for idx in order:
        app = ids[idx]
        similarity = float(sims[idx])
        if similarity < min_similarity:
            continue
        neighbors.append({
            'appid': app,
            'similarity': similarity,
            'cluster_id': clusters_lookup.get(app),
            'name': names_lookup.get(app),
        })
        if top_k is not None and len(neighbors) >= top_k:
            break
    return neighbors
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
        default=None,
        help="Umbral mínimo de similitud coseno para mostrar vecinos. Si no se indica, se infiere de params.yaml (neighbor_strategy.min_score o min_similarity_in).",
    )
    parser.add_argument("--params-config", default="configs/params.yaml", help="YAML con neighbor_strategy para defaults y pesos del re-ranking.")
    parser.add_argument("--embeddings", default="data/processed/embeddings/embeddings.parquet", help="Ruta a embeddings.parquet con columnas appid y embedding.")
    parser.add_argument("--clusters", default="data/processed/clusters.parquet", help="Ruta a clusters.parquet para mapear appid -> cluster_id.")
    parser.add_argument("--metadata", default="data/processed/game_metadata.parquet", help="Ruta opcional a metadata con nombres de juego.")
    parser.add_argument("--faiss-index", default=None, help="Ruta opcional a un indice FAISS persistido.")
    parser.add_argument("--faiss-ids", default=None, help="Ruta opcional al JSON con el orden de appids del indice FAISS.")
    parser.add_argument("--neighbors", type=int, default=None, help="Numero de vecinos mas cercanos a mostrar (override).")
    parser.add_argument("--max-neighbors", type=int, dest="neighbors", help=argparse.SUPPRESS)
    parser.add_argument("--allow-cross", dest="allow_cross", action="store_true", help="Permite candidatos cross-cluster en el re-ranking.")
    parser.add_argument("--no-allow-cross", dest="allow_cross", action="store_false", help="Desactiva candidatos cross-cluster.")
    parser.set_defaults(allow_cross=None)
    parser.add_argument("--k-in", type=int, default=None, help="Numero de vecinos intra-cluster a considerar en la primera banda.")
    parser.add_argument("--k-out", type=int, default=None, help="Numero de candidatos cross-cluster iniciales.")
    parser.add_argument("--min-sim-in", type=float, default=None, help="Similitud minima para vecinos intra-cluster.")
    parser.add_argument("--min-sim-out", type=float, default=None, help="Similitud minima para candidatos cross-cluster.")
    parser.add_argument("--max-out-ratio", type=float, default=None, help="Proporcion maxima de cross-cluster en la lista final.")
    parser.add_argument("--show-diagnostics", action="store_true", help="Muestra diagnosticos de la estrategia de vecinos.")
    parser.add_argument("--mlflow-run-name", default=None, help="Nombre personalizado para la corrida de MLflow.")
    args = parser.parse_args()

    params_cfg: Dict[str, Any] = {}
    if args.params_config:
        cfg_path = Path(args.params_config)
        if cfg_path.exists():
            try:
                loaded = yaml.safe_load(cfg_path.read_text(encoding='utf-8'))
                if isinstance(loaded, dict):
                    params_cfg = loaded
                else:
                    print(f"[WARN] params_config {cfg_path} no contiene un diccionario, ignorando.")
            except Exception as exc:
                print(f"[WARN] No se pudo leer params_config {cfg_path}: {exc}")
        else:
            print(f"[WARN] params_config no encontrado: {cfg_path}")

    strategy_cfg = deepcopy(DEFAULT_CONFIG)

    def _merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> None:
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                _merge_dict(base[key], value)
            else:
                base[key] = value

    file_cfg = params_cfg.get('neighbor_strategy') or {}
    if isinstance(file_cfg, dict):
        _merge_dict(strategy_cfg, file_cfg)
    legacy_cfg = (params_cfg.get('client_report') or {}).get('neighbors_config') or {}
    if isinstance(legacy_cfg, dict):
        _merge_dict(strategy_cfg, legacy_cfg)
    faiss_index_path = args.faiss_index or strategy_cfg.pop("faiss_index_path", None)
    faiss_ids_path = args.faiss_ids or strategy_cfg.pop("faiss_ids_path", None)
    if not faiss_index_path:
        faiss_index_path = params_cfg.get("faiss_index_path")
    if not faiss_ids_path:
        faiss_ids_path = params_cfg.get("faiss_ids_path")

    if args.neighbors is None:
        args.neighbors = int(strategy_cfg.get('target_total', DEFAULT_CONFIG.get('target_total', 20)))
    strategy_cfg['target_total'] = int(args.neighbors)

    if args.allow_cross is None:
        args.allow_cross = bool(strategy_cfg.get('allow_cross_cluster', True))
    strategy_cfg['allow_cross_cluster'] = bool(args.allow_cross)

    if args.k_in is not None:
        strategy_cfg['k_in'] = int(args.k_in)
    args.k_in = int(strategy_cfg.get('k_in', DEFAULT_CONFIG.get('k_in', 25)))

    if args.k_out is not None:
        strategy_cfg['k_out'] = int(args.k_out)
    args.k_out = int(strategy_cfg.get('k_out', DEFAULT_CONFIG.get('k_out', 15)))

    if args.min_sim_in is not None:
        strategy_cfg['min_similarity_in'] = float(args.min_sim_in)
    args.min_sim_in = float(strategy_cfg.get('min_similarity_in', DEFAULT_CONFIG.get('min_similarity_in', 0.0)))

    if args.min_sim_out is not None:
        strategy_cfg['min_similarity_out'] = float(args.min_sim_out)
    args.min_sim_out = float(strategy_cfg.get('min_similarity_out', DEFAULT_CONFIG.get('min_similarity_out', 0.78)))

    # Inferir umbral de similitud desde YAML si no se pasa por flag
    if args.min_similarity is None:
        cfg_min_score = strategy_cfg.get('min_score')
        try:
            args.min_similarity = float(cfg_min_score) if cfg_min_score is not None else float(strategy_cfg.get('min_similarity_in', DEFAULT_CONFIG.get('min_similarity_in', 0.0)))
        except Exception:
            args.min_similarity = float(strategy_cfg.get('min_similarity_in', DEFAULT_CONFIG.get('min_similarity_in', 0.0)))

    if args.max_out_ratio is not None:
        strategy_cfg['max_out_ratio'] = float(args.max_out_ratio)
    args.max_out_ratio = float(strategy_cfg.get('max_out_ratio', DEFAULT_CONFIG.get('max_out_ratio', 0.3)))
    mlflow_cfg = params_cfg.get("mlflow") or {}
    mlflow_run_name = args.mlflow_run_name or f"single-game-poc-{args.scenario}"
    mlflow_run_active = False
    artifact_path: str | None = None
    if mlflow and start_mlflow_run and mlflow_cfg:
        try:
            start_mlflow_run(
                mlflow_cfg.get("experiment_name", "single-game-poc"),
                mlflow_run_name,
                tracking_uri=mlflow_cfg.get("tracking_uri"),
            )
            if log_mlflow_params:
                params_to_log = {
                    "scenario": args.scenario,
                    "neighbors_target": args.neighbors,
                    "allow_cross_cluster": strategy_cfg.get("allow_cross_cluster"),
                    "k_in": args.k_in,
                    "k_out": args.k_out,
                    "min_similarity_in": args.min_sim_in,
                    "min_similarity_out": args.min_sim_out,
                    "max_out_ratio": args.max_out_ratio,
                    "faiss_index_path": faiss_index_path or "",
                    "faiss_ids_path": faiss_ids_path or "",
                }
                log_mlflow_params({k: ("" if v is None else str(v)) for k, v in params_to_log.items()})
            mlflow_run_active = True
        except Exception as exc:
            print(f"[WARN] No se pudo iniciar MLflow: {exc}")
            mlflow_run_active = False
    else:
        mlflow_run_active = False

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
    if mlflow_run_active and log_mlflow_params:
        try:
            log_mlflow_params({"best_cluster": str(best_cluster)})
        except Exception as exc:
            print(f"[WARN] No se pudo registrar parámetro en MLflow: {exc}")
    print("================= RESULTADO =================")
    print(f"Mejor clúster : {best_cluster}")
    print(f"Similitud     : {best_score:.4f}")

    filtered = [item for item in scores if item[1] >= args.min_similarity]
    if args.neighbors is not None and args.neighbors > 0:
        filtered = filtered[: args.neighbors]

    if not filtered:
        print("\nNo hay vecinos que superen el umbral de similitud solicitado.")
    else:
        print(f"\nRanking de vecinos (>= {args.min_similarity:.4f}) - mostrando {len(filtered)} de {len(scores)} medoids")
        print(_format_similarity_table(filtered))

    emb_df = _load_optional_df(args.embeddings)
    clusters_df = _load_optional_df(args.clusters)
    metadata_df = _load_optional_df(args.metadata)

    sample_metadata = _prepare_query_metadata(sample)
    try:
        query_cluster_id = int(best_cluster)
    except Exception:
        query_cluster_id = None

    strategy_neighbors, diagnostics = _neighbors_with_strategy(
        sample_vec,
        sample_metadata,
        query_cluster_id,
        emb_df,
        clusters_df,
        metadata_df,
        medoids,
        strategy_cfg,
        faiss_index_path=faiss_index_path,
        faiss_ids_path=faiss_ids_path,
    )

    if not strategy_neighbors:
        strategy_neighbors = _find_neighbors(sample_vec, emb_df, clusters_df, metadata_df, args.neighbors, args.min_similarity)
        diagnostics = {}

    if mlflow_run_active and log_mlflow_metrics:
        metrics = {
            "best_cluster_similarity": float(best_score),
            "neighbors_selected": len(strategy_neighbors or []),
        }
        if diagnostics:
            for key, value in diagnostics.items():
                if isinstance(value, (int, float, np.floating)):
                    metrics[f"diag_{key}"] = float(value)
            faiss_used = diagnostics.get("faiss_used")
            if isinstance(faiss_used, bool):
                metrics["diag_faiss_used"] = 1.0 if faiss_used else 0.0
        try:
            log_mlflow_metrics(metrics)
        except Exception as exc:
            print(f"[WARN] No se pudieron registrar métricas en MLflow: {exc}")
        if log_mlflow_artifacts and strategy_neighbors:
            try:
                tmp_file = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, suffix=".json")
                with tmp_file:
                    json.dump({"neighbors": strategy_neighbors, "diagnostics": diagnostics}, tmp_file, ensure_ascii=False, indent=2)
                artifact_path = tmp_file.name
                log_mlflow_artifacts(artifact_path, artifact_path="single_game_poc")
            except Exception as exc:
                print(f"[WARN] No se pudo registrar artefactos en MLflow: {exc}")
            finally:
                if artifact_path:
                    Path(artifact_path).unlink(missing_ok=True)
                    artifact_path = None
    if strategy_neighbors:
        print(f"\nVecinos mas cercanos (top {len(strategy_neighbors)}):")
        header = f"{'appid':<12}{'cluster':<10}{'sim':<8}{'score':<8}{'src':<8}name"
        print(header)
        print('-' * len(header))
        for row in strategy_neighbors:
            cluster_val = row.get('cluster_id')
            cluster_txt = str(cluster_val) if cluster_val is not None else '-'
            score_val = row.get('score')
            score_txt = f"{score_val:.4f}" if isinstance(score_val, (float, int)) else '-'
            source_txt = row.get('source') or '-'
            name_txt = row.get('name') or ''
            sim_val = row.get('similarity', 0.0)
            print(f"{row['appid']:<12}{cluster_txt:<10}{sim_val:.4f}{score_txt:>8}{source_txt:>8} {name_txt}")
    else:
        print('\nNo se pudieron calcular vecinos desde embeddings (archivo ausente o sin datos).')

    if args.show_diagnostics and diagnostics:
        print('\n[Diag] estrategia de vecinos:')
        for key, value in diagnostics.items():
            print(f" - {key}: {value}")

    if mlflow_run_active and mlflow:
        try:
            mlflow.end_run()
        except Exception:
            pass

if __name__ == "__main__":
    main()

