#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utilidades mínimas para cargar documentos desde MongoDB.

Provee la clase `MongoLoader` usada por el pipeline de embeddings.
"""
from __future__ import annotations
from typing import Dict, Iterator, Any

from pymongo import MongoClient


class MongoLoader:
    """Cargador simple que itera documentos de una colección de MongoDB.

    Args:
        uri: URI de conexión a MongoDB.
        database: Nombre de la base de datos.
        collection: Nombre de la colección.
    """

    def __init__(self, uri: str, database: str, collection: str) -> None:
        self._uri = uri
        self._db = database
        self._coll = collection

    def load_data(self, query: Dict[str, Any] | None = None, projection: Dict[str, int] | None = None) -> Iterator[Dict[str, Any]]:
        """Generador que emite documentos según `query` y `projection`.

        Usa un cursor no bloqueante y cierra la conexión al finalizar.
        """
        client = MongoClient(self._uri)
        try:
            col = client[self._db][self._coll]
            cur = col.find(query or {}, projection or {})
            for doc in cur:
                # Normaliza appid a str si existe
                if 'appid' in doc and doc['appid'] is not None:
                    try:
                        doc['appid'] = str(doc['appid'])
                    except Exception:
                        pass
                yield doc
        finally:
            try:
                client.close()
            except Exception:
                pass

