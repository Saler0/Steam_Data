from __future__ import annotations

import os
from typing import Any, Dict, List

import requests
from flask import Flask, abort, flash, redirect, render_template, request, url_for


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["BACKEND_URL"] = os.environ.get("BACKEND_URL", "http://localhost:5001").rstrip("/")
    app.secret_key = os.environ.get("FRONTEND_SECRET_KEY", "front-secret-key")

    def backend_url(path: str) -> str:
        return f"{app.config['BACKEND_URL']}{path}" if path.startswith("/") else f"{app.config['BACKEND_URL']}/{path}"

    @app.context_processor
    def inject_settings() -> Dict[str, Any]:
        return {"backend_url": app.config["BACKEND_URL"]}

    @app.route("/")
    def home():
        juegos: List[Dict[str, Any]] = []
        try:
            response = requests.get(backend_url("/api/games"), timeout=5)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                juegos = payload.get("games", [])
        except requests.HTTPError:
            flash("El backend devolvio un error al obtener los juegos.", "error")
        except (requests.RequestException, ValueError):
            flash("No se pudo cargar la lista de juegos en este momento.", "error")
        return render_template("index.html", juegos=juegos)

    @app.route("/about")
    def about():
        return render_template("about.html")

    @app.route("/agregar_juego", methods=["GET", "POST"])
    def agregar_juego():
        if request.method == "POST":
            payload = {
                "nombre": request.form.get("nombre", "").strip(),
                "categoria": request.form.get("categoria", "").strip(),
                "descripcion": request.form.get("descripcion", "").strip(),
                "precio": request.form.get("precio", "").strip(),
            }

            if not all(payload.values()):
                flash("Todos los campos son obligatorios.", "error")
                return redirect(url_for("agregar_juego"))

            try:
                payload["precio"] = float(payload["precio"])
            except ValueError:
                flash("El precio debe ser numerico.", "error")
                return redirect(url_for("agregar_juego"))

            try:
                response = requests.post(backend_url("/api/games"), json=payload, timeout=5)
            except requests.RequestException:
                flash("No se pudo comunicar con el backend.", "error")
                return redirect(url_for("agregar_juego"))

            if response.status_code >= 400:
                try:
                    error_payload = response.json()
                except ValueError:
                    error_payload = {}
                message = error_payload.get("message") or "No se pudo guardar el juego."
                flash(message, "error")
                return redirect(url_for("agregar_juego"))

            flash("Juego agregado correctamente.", "success")
            return redirect(url_for("home"))

        return render_template("user.html")

    @app.route("/juego/<int:juego_id>")
    def juego(juego_id: int):
        try:
            response = requests.get(backend_url(f"/api/games/{juego_id}"), timeout=5)
        except requests.RequestException:
            abort(502, description="No fue posible contactar el backend.")

        if response.status_code == 404:
            abort(404)

        if response.status_code >= 400:
            abort(response.status_code)

        try:
            juego_data = response.json()
        except ValueError:
            abort(502, description="Respuesta invalida del backend.")

        return render_template("juego.html", juego=juego_data)

    @app.errorhandler(404)
    def handle_not_found(_error):
        return render_template("error.html", titulo="404", mensaje="No encontramos lo que buscas."), 404

    @app.errorhandler(502)
    def handle_backend_error(error):
        mensaje = getattr(error, "description", None) or "Error al contactar el backend."
        return render_template("error.html", titulo="502", mensaje=mensaje), 502

    return app


app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("FRONTEND_PORT", 5100))
    app.run(debug=True, port=port)
