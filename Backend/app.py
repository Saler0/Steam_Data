from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Tuple

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import NotFound

from db.mongodb import MongoDBClient
from pymongo.errors import PyMongoError


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

        document = {
            "nombre": payload["nombre"].strip(),
            "categoria": payload["categoria"].strip(),
            "descripcion": payload["descripcion"].strip(),
            "precio": float(payload["precio"]),
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
            "categoria": document["categoria"],
            "descripcion": document["descripcion"],
            "precio": document["precio"],
            "created_at": document["created_at"].isoformat() + "Z",
        }
        return jsonify(response_payload), 201


def validate_game_payload(payload: Dict[str, Any]) -> Dict[str, str]:
    errors: Dict[str, str] = {}
    required_fields = ["nombre", "categoria", "descripcion", "precio"]

    for field in required_fields:
        value = payload.get(field)
        if value in (None, ""):
            errors[field] = "Este campo es obligatorio"
            continue

        if field == "precio":
            try:
                float(value)
            except (TypeError, ValueError):
                errors[field] = "Debe ser un numero"

    return errors


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("BACKEND_PORT", 5001))
    app.run(debug=True, port=port)
