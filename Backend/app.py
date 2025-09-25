from __future__ import annotations

import os
from typing import Any, Dict, Tuple

from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
from werkzeug.exceptions import NotFound


db = SQLAlchemy()


class Game(db.Model):
    __tablename__ = "games"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(100), nullable=False)
    categoria = db.Column(db.String(50), nullable=False)
    descripcion = db.Column(db.Text, nullable=False)
    precio = db.Column(db.Float, nullable=False)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "nombre": self.nombre,
            "categoria": self.categoria,
            "descripcion": self.descripcion,
            "precio": self.precio,
        }


def create_app() -> Flask:
    app = Flask(__name__)
    database_url = os.environ.get("DATABASE_URL", "sqlite:///mi_db.db")
    app.config.update(
        SQLALCHEMY_DATABASE_URI=database_url,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
    )

    db.init_app(app)
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    register_routes(app)

    with app.app_context():
        db.create_all()

    return app


def register_routes(app: Flask) -> None:
    @app.errorhandler(NotFound)
    def handle_not_found(error: NotFound):
        return jsonify({"message": error.description or "Recurso no encontrado"}), 404

    @app.get("/api/health")
    def health() -> Tuple[Dict[str, str], int]:
        return {"status": "ok"}, 200

    @app.get("/api/games")
    def list_games():
        games = Game.query.order_by(Game.id).all()
        return jsonify({"games": [game.to_dict() for game in games]})

    @app.post("/api/games")
    def create_game():
        payload = request.get_json(silent=True) or {}
        errors = validate_game_payload(payload)
        if errors:
            return jsonify({"message": "Datos invalidos", "errors": errors}), 400

        game = Game(
            nombre=payload["nombre"].strip(),
            categoria=payload["categoria"].strip(),
            descripcion=payload["descripcion"].strip(),
            precio=float(payload["precio"]),
        )
        db.session.add(game)
        db.session.commit()
        return jsonify(game.to_dict()), 201

    @app.get("/api/games/<int:game_id>")
    def get_game(game_id: int):
        game = Game.query.get_or_404(game_id, description="Juego no encontrado")
        return jsonify(game.to_dict())

    @app.put("/api/games/<int:game_id>")
    def update_game(game_id: int):
        payload = request.get_json(silent=True) or {}
        errors = validate_game_payload(payload)
        if errors:
            return jsonify({"message": "Datos invalidos", "errors": errors}), 400

        game = Game.query.get_or_404(game_id, description="Juego no encontrado")
        game.nombre = payload["nombre"].strip()
        game.categoria = payload["categoria"].strip()
        game.descripcion = payload["descripcion"].strip()
        game.precio = float(payload["precio"])
        db.session.commit()
        return jsonify(game.to_dict())

    @app.delete("/api/games/<int:game_id>")
    def delete_game(game_id: int):
        game = Game.query.get_or_404(game_id, description="Juego no encontrado")
        db.session.delete(game)
        db.session.commit()
        return "", 204


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
