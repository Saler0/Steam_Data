from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Tuple

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import NotFound

from db.mongodb import MongoDBClient
from pymongo.errors import PyMongoError


ALLOWED_PLATFORMS = {"windows", "mac", "linux"}


def create_app() -> Flask:
    app = Flask(__name__)
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db_name = os.environ.get("MONGO_DB", "exploitation_zone")
    mongo_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_name)
    app.config["MONGO_CLIENT"] = mongo_client

    CORS(app, resources={r"/api/*": {"origins": "*"}})

    register_routes(app, mongo_client)

    return app


def register_routes(app: Flask, mongo_client: MongoDBClient) -> None:
    @app.errorhandler(NotFound)
    def handle_not_found(error: NotFound):
        return jsonify({"message": error.description or "Recurso no encontrado"}), 404

    @app.get("/api/health")
    def health() -> Tuple[Dict[str, str], int]:
        return {"status": "ok"}, 200

    @app.get("/api/mongo-health")
    def mongo_health() -> Tuple[Dict[str, str], int]:
        try:
            mongo_client.ping()
        except Exception as exc:
            return {"status": "error", "message": str(exc)}, 503
        return {"status": "ok"}, 200

    @app.post("/api/games")
    def create_game():
        payload = request.get_json(silent=True) or {}
        errors = validate_game_payload(payload)
        if errors:
            return jsonify({"message": "Datos invalidos", "errors": errors}), 400

        platforms = payload.get("platforms", [])
        if isinstance(platforms, str):
            platforms = [platforms]
        platforms = list(dict.fromkeys(platforms))

        install_size = float(payload["install_size_gb"])
        ram_required = float(payload["ram_gb"])
        short_description = payload.get("short_description", "").strip()
        detailed_description = payload.get("detailed_description", "").strip()
        genres = payload.get("genres", [])
        categories = payload.get("categories", [])

        document = {
            "nombre": payload["nombre"].strip(),
            "descripcion_corta": short_description,
            "descripcion_detallada": detailed_description,
            "genres": genres,
            "categories": categories,
            "precio": float(payload["precio"]),
            "platforms": platforms,
            "install_size_gb": install_size,
            "ram_gb": ram_required,
            "created_at": datetime.utcnow(),
        }

        try:
            collection = mongo_client.get_collection("juegos_clientes")
            insert_result = collection.insert_one(document)
        except PyMongoError as exc:
            return (
                jsonify(
                    {
                        "message": "No se pudo guardar el juego en MongoDB",
                        "error": str(exc),
                    }
                ),
                502,
            )

        response_payload = {
            "mongo_id": str(insert_result.inserted_id),
            "nombre": document["nombre"],
            "descripcion_corta": document["descripcion_corta"],
            "descripcion_detallada": document["descripcion_detallada"],
            "genres": document["genres"],
            "categories": document["categories"],
            "precio": document["precio"],
            "platforms": document["platforms"],
            "install_size_gb": document["install_size_gb"],
            "ram_gb": document["ram_gb"],
            "created_at": document["created_at"].isoformat() + "Z",
        }
        return jsonify(response_payload), 201


def validate_game_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    errors: Dict[str, str] = {}

    text_fields = ["nombre", "short_description", "detailed_description"]
    for field in text_fields:
        value = payload.get(field)
        if value is None:
            errors[field] = "Este campo es obligatorio"
            continue
        text_value = str(value).strip()
        if not text_value:
            errors[field] = "Este campo es obligatorio"
            continue
        payload[field] = text_value

    numeric_fields = ["precio", "install_size_gb", "ram_gb"]
    for field in numeric_fields:
        value = payload.get(field)
        if value in (None, ""):
            errors[field] = "Este campo es obligatorio"
            continue
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            errors[field] = "Debe ser un numero"
            continue
        if numeric_value < 0:
            errors[field] = "Debe ser no negativo"
            continue
        payload[field] = numeric_value

    platforms = payload.get("platforms")
    if not platforms:
        errors["platforms"] = "Debes seleccionar al menos una plataforma"
    else:
        if isinstance(platforms, str):
            platforms = [platforms]
        try:
            normalized = []
            for platform in platforms:
                if platform is None:
                    continue
                normalized_value = str(platform).strip().lower()
                if normalized_value:
                    normalized.append(normalized_value)
        except TypeError:
            errors["platforms"] = "Formato de plataformas no valido"
        else:
            normalized = list(dict.fromkeys(normalized))
            if not normalized:
                errors["platforms"] = "Debes seleccionar al menos una plataforma"
            else:
                invalid = [plat for plat in normalized if plat not in ALLOWED_PLATFORMS]
                if invalid:
                    errors["platforms"] = "Plataformas no validas: " + ", ".join(sorted(set(invalid)))
                else:
                    payload["platforms"] = normalized

    def _normalize_list(field: str) -> None:
        raw = payload.get(field)
        if raw in (None, ""):
            payload[field] = []
            return
        if isinstance(raw, (list, tuple, set)):
            candidates = list(raw)
        else:
            candidates = [raw]
        cleaned = []
        for item in candidates:
            if item is None:
                continue
            text_item = str(item).strip()
            if text_item:
                cleaned.append(text_item)
        payload[field] = list(dict.fromkeys(cleaned))

    _normalize_list("genres")
    _normalize_list("categories")

    return errors


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("BACKEND_PORT", 5001))
    app.run(debug=True, port=port)
