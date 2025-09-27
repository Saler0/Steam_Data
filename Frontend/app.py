from __future__ import annotations

import os
from typing import Any, Dict

import requests
from flask import Flask, flash, redirect, render_template, request, url_for


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
        return render_template("index.html")

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
            return redirect(url_for("agregar_juego"))

        return render_template("user.html")

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
