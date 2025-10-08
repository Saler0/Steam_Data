from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.exceptions import NotFound

from db.mongodb import MongoDBClient
from pymongo.errors import PyMongoError

from services.decision_rules import DecisionRulesService
from services.single_game_poc import (
    PoCConfigurationError,
    PoCExecutionError,
    SingleGamePoCService,
)


ALLOWED_PLATFORMS = {"windows", "mac", "linux"}



def create_app() -> Flask:
    app = Flask(__name__)
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db_name = os.environ.get("MONGO_DB", "exploitation_zone")
    mongo_client = MongoDBClient(uri=mongo_uri, db_name=mongo_db_name)
    decision_rules_service = DecisionRulesService(mongo_client)
    app.config["MONGO_CLIENT"] = mongo_client
    app.config["DECISION_RULES_SERVICE"] = decision_rules_service

    try:
        poc_service = SingleGamePoCService()
    except PoCConfigurationError as exc:
        poc_service = None
        app.logger.error("SingleGamePoCService initialization failed: %s", exc)
    except Exception as exc:  # pragma: no cover - defensive
        poc_service = None
        app.logger.exception("Unexpected error initializing SingleGamePoCService: %s", exc)
    app.config["POC_SERVICE"] = poc_service

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
        full_content_included = payload.get("full_content_included", False)

        document = {
            "nombre": payload["nombre"].strip(),
            "short_description": short_description,
            "detailed_description": detailed_description,
            "genres": genres,
            "categories": categories,
            "precio": float(payload["precio"]),
            "platforms": platforms,
            "install_size_gb": install_size,
            "ram_gb": ram_required,
            "full_content_included": full_content_included,
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

        poc_service: Optional[SingleGamePoCService] = app.config.get("POC_SERVICE")
        if poc_service is None:
            collection.delete_one({"_id": insert_result.inserted_id})
            return (
                jsonify(
                    {
                        "message": "Servicio de asignacion no disponible",
                        "error": "No se pudo inicializar el servicio de PoC",
                    }
                ),
                503,
            )

        try:
            poc_result = poc_service.run(
                {
                    "name": document["nombre"],
                    "short_description": document["short_description"],
                    "detailed_description": document["detailed_description"],
                    "genres": document["genres"],
                    "categories": document["categories"],
                    "price": document["precio"],
                },
                neighbors=20,
                min_similarity=0.0,
            )
        except PoCExecutionError as exc:
            collection.delete_one({"_id": insert_result.inserted_id})
            app.logger.exception("PoC execution failed for game '%s': %s", document["nombre"], exc)
            return (
                jsonify(
                    {
                        "message": "No se pudo ejecutar la asignacion de competidores",
                        "error": str(exc),
                    }
                ),
                502,
            )
        except Exception as exc:  # pragma: no cover - defensive
            collection.delete_one({"_id": insert_result.inserted_id})
            app.logger.exception("Unexpected error running PoC for game '%s': %s", document["nombre"], exc)
            return (
                jsonify(
                    {
                        "message": "Error inesperado al ejecutar la asignacion de competidores",
                        "error": str(exc),
                    }
                ),
                502,
            )

        raw_neighbors = poc_result.get("neighbors", []) if isinstance(poc_result, dict) else []
        diagnostics = poc_result.get("diagnostics", {}) if isinstance(poc_result, dict) else {}
        normalized_neighbors = []
        for item in raw_neighbors:
            if not isinstance(item, dict):
                continue
            neighbor = {
                "appid": item.get("appid"),
                "cluster_id": item.get("cluster_id"),
                "name": item.get("name"),
            }
            similarity = item.get("similarity")
            try:
                neighbor["similarity"] = float(similarity) if similarity is not None else None
            except (TypeError, ValueError):
                neighbor["similarity"] = None
            score = item.get("score")
            if score is not None:
                try:
                    neighbor["score"] = float(score)
                except (TypeError, ValueError):
                    pass
            source = item.get("source")
            if source:
                neighbor["source"] = source
            normalized_neighbors.append(neighbor)
            if len(normalized_neighbors) >= 20:
                break
        decision_rules_service = app.config.get("DECISION_RULES_SERVICE")
        if isinstance(decision_rules_service, DecisionRulesService):
            try:
                price_rule = decision_rules_service.evaluate_price_rule(document.get("precio"), raw_neighbors)
            except Exception as exc:
                app.logger.exception("Failed to evaluate price rule: %s", exc)
                price_rule = {"label": "error", "details": str(exc)}
            try:
                platform_rule = decision_rules_service.evaluate_platform_rule(document.get("platforms"), raw_neighbors)
            except Exception as exc:
                app.logger.exception("Failed to evaluate platform rule: %s", exc)
                platform_rule = "error"
        else:
            price_rule = {"label": "sin_servicio"}
            platform_rule = "sin_servicio"

        best_similarity = poc_result.get("best_cluster_similarity") if isinstance(poc_result, dict) else None
        try:
            best_similarity_val = float(best_similarity) if best_similarity is not None else None
        except (TypeError, ValueError):
            best_similarity_val = None

        poc_record = {
            "best_cluster_id": poc_result.get("best_cluster_id") if isinstance(poc_result, dict) else None,
            "best_cluster_similarity": best_similarity_val,
            "neighbors": normalized_neighbors,
            "diagnostics": diagnostics,
            "generated_at": datetime.utcnow(),
            "platforms_rule": platform_rule,
        }

        try:
            collection.update_one(
                {"_id": insert_result.inserted_id},
                {"$set": {"poc_assignment": poc_record, "price_rule": price_rule, "platforms_rule": platform_rule}},
            )
        except PyMongoError as exc:
            collection.delete_one({"_id": insert_result.inserted_id})
            app.logger.exception("Failed to persist PoC assignment for game '%s': %s", document["nombre"], exc)
            return (
                jsonify(
                    {
                        "message": "No se pudo guardar el resultado del PoC",
                        "error": str(exc),
                    }
                ),
                502,
            )

        response_payload = {
            "mongo_id": str(insert_result.inserted_id),
            "name": document["nombre"],
            "short_description": document["short_description"],
            "detailed_description": document["detailed_description"],
            "genres": document["genres"],
            "categories": document["categories"],
            "precio": document["precio"],
            "platforms": document["platforms"],
            "install_size_gb": document["install_size_gb"],
            "ram_gb": document["ram_gb"],
            "full_content_included": document["full_content_included"],
            "created_at": document["created_at"].isoformat() + "Z",
            "platforms_rule": platform_rule,
            "poc_assignment": {
                "best_cluster_id": poc_record["best_cluster_id"],
                "best_cluster_similarity": poc_record["best_cluster_similarity"],
                "neighbors": normalized_neighbors,
                "diagnostics": poc_record["diagnostics"],
                "generated_at": poc_record["generated_at"].isoformat() + "Z",
                "platforms_rule": platform_rule,
            },
            "price_rule": price_rule
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

    raw_full_content = payload.get("full_content_included")
    if isinstance(raw_full_content, (list, tuple, set)):
        raw_values = list(raw_full_content)
        raw_full_content = raw_values[0] if raw_values else None

    truthy_values = {"yes", "true", "1", "on", "si"}
    falsy_values = {"no", "false", "0", "off"}

    if raw_full_content in (None, ""):
        errors["full_content_included"] = "Debes indicar si el contenido completo esta incluido"
    elif isinstance(raw_full_content, bool):
        payload["full_content_included"] = raw_full_content
    elif isinstance(raw_full_content, (int, float)) and not isinstance(raw_full_content, bool):
        payload["full_content_included"] = bool(raw_full_content)
    elif isinstance(raw_full_content, str):
        normalized_full_content = raw_full_content.strip().lower()
        if normalized_full_content in truthy_values:
            payload["full_content_included"] = True
        elif normalized_full_content in falsy_values:
            payload["full_content_included"] = False
        else:
            errors["full_content_included"] = "Valor de contenido completo no valido"
    else:
        errors["full_content_included"] = "Valor de contenido completo no valido"

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

