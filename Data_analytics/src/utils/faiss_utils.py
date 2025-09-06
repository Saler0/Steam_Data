#!/usr/bin/env python
# -*- coding: utf-8 -*-

import faiss
import numpy as np
from pathlib import Path
from typing import Tuple


def build_faiss_index(vectors: np.ndarray, index_type: str = "FlatL2", use_gpu: bool = False) -> faiss.Index:
    """
    Construye un índice FAISS a partir de un arreglo de vectores.

    Args:
        vectors (np.ndarray): Arreglo NumPy 2D de embeddings (float32).
        index_type (str): Tipo de índice FAISS ("FlatL2" | "FlatIP").
        use_gpu (bool): Intentar usar GPU si está disponible.

    Returns:
        faiss.Index: Índice FAISS construido y poblado.
    """
    if not isinstance(vectors, np.ndarray) or vectors.ndim != 2:
        raise ValueError("Los vectores deben ser un arreglo 2D de NumPy.")

    vectors = vectors.astype(np.float32)
    dim = vectors.shape[1]

    if index_type in ("FlatL2", "FlatIP"):
        index = faiss.IndexFlatL2(dim) if index_type == "FlatL2" else faiss.IndexFlatIP(dim)
    elif index_type.upper().startswith("IVF"):
        # Ejemplo: IVF4096,Flat  (usa métrica L2/IP según el sufijo FlatL2/FlatIP implícito)
        try:
            index = faiss.index_factory(dim, index_type)
        except Exception as e:
            raise ValueError(f"No se pudo construir índice '{index_type}' con index_factory: {e}")
    else:
        raise ValueError(f"Tipo de índice FAISS no soportado: {index_type}")

    # Opcional: mover a GPU si procede
    if use_gpu:
        try:
            res = faiss.StandardGpuResources()
            index = faiss.index_cpu_to_gpu(res, 0, index)
        except Exception as e:
            print(f"[WARN] No se pudo inicializar índice FAISS en GPU: {e}. Usando CPU.")

    index.add(vectors)
    print(f"[INFO] Índice FAISS tipo '{index_type}' construido con {index.ntotal} vectores.")
    return index


def search_faiss_index(index: faiss.Index, query_vectors: np.ndarray, top_k: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Busca los vecinos más cercanos en un índice FAISS.

    Args:
        index (faiss.Index): El índice FAISS.
        query_vectors (np.ndarray): Arreglo 2D de los vectores a buscar.
        top_k (int): Número de vecinos más cercanos a retornar.

    Returns:
        tuple: (distancias, índices).
    """
    query_vectors = query_vectors.astype(np.float32)
    distances, indices = index.search(query_vectors, top_k)
    return distances, indices


def save_faiss_index(index: faiss.Index, path: str):
    """Guarda un índice FAISS en un archivo."""
    faiss.write_index(index, str(Path(path)))
    print(f"[INFO] Índice FAISS guardado en {path}.")


def load_faiss_index(path: str) -> faiss.Index:
    """Carga un índice FAISS desde un archivo."""
    index = faiss.read_index(str(Path(path)))
    print(f"[INFO] Índice FAISS cargado desde {path} con {index.ntotal} vectores.")
    return index
