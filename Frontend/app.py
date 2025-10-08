# from __future__ import annotations

# import os
# from typing import Any, Dict

# import requests
# from flask import Flask, flash, redirect, render_template, request, url_for


# def create_app() -> Flask:
#     app = Flask(__name__, template_folder="templates", static_folder="static")
#     app.config["BACKEND_URL"] = os.environ.get("BACKEND_URL", "http://localhost:5001").rstrip("/")
#     backend_timeout = float(os.environ.get("BACKEND_TIMEOUT", "120"))
#     app.secret_key = os.environ.get("FRONTEND_SECRET_KEY", "front-secret-key")

#     def backend_url(path: str) -> str:
#         return f"{app.config['BACKEND_URL']}{path}" if path.startswith("/") else f"{app.config['BACKEND_URL']}/{path}"

#     @app.context_processor
#     def inject_settings() -> Dict[str, Any]:
#         return {"backend_url": app.config["BACKEND_URL"]}

#     @app.route("/")
#     def home():
#         return render_template("index.html")

#     @app.route("/about")
#     def about():
#         return render_template("about.html")

#     # @app.route("/agregar_juego", methods=["GET", "POST"])
#     # def agregar_juego():
#     #     if request.method == "POST":
#     #         payload = {
#     #             "nombre": request.form.get("nombre", "").strip(),
#     #             "categoria": request.form.get("categoria", "").strip(),
#     #             "descripcion": request.form.get("descripcion", "").strip(),
#     #             "precio": request.form.get("precio", "").strip(),
#     #         }

#     #         if not all(payload.values()):
#     #             flash("Todos los campos son obligatorios.", "error")
#     #             return redirect(url_for("agregar_juego"))

#     #         try:
#     #             payload["precio"] = float(payload["precio"])
#     #         except ValueError:
#     #             flash("El precio debe ser numerico.", "error")
#     #             return redirect(url_for("agregar_juego"))

#     #         try:
#     #             response = requests.post(backend_url("/api/games"), json=payload, timeout=backend_timeout)
#     #         except requests.RequestException:
#     #             flash("No se pudo comunicar con el backend.", "error")
#     #             return redirect(url_for("agregar_juego"))

#     #         if response.status_code >= 400:
#     #             try:
#     #                 error_payload = response.json()
#     #             except ValueError:
#     #                 error_payload = {}
#     #             message = error_payload.get("message") or "No se pudo guardar el juego."
#     #             flash(message, "error")
#     #             return redirect(url_for("agregar_juego"))

#     #         flash("Juego agregado correctamente.", "success")
#     #         return redirect(url_for("agregar_juego"))

#     #     return render_template("user.html")
#     @app.route("/agregar_juego", methods=["GET", "POST"])
#     def agregar_juego():
#         if request.method == "POST":
#             nombre = request.form.get("nombre", "").strip()
#             short_description = request.form.get("short_description", "").strip()
#             detailed_description = request.form.get("detailed_description", "").strip()


#             payload = {
#                 "nombre": nombre,

#                 "precio": request.form.get("precio", "").strip(),
#                 "short_description": short_description,
#                 "detailed_description": detailed_description,
#                 "genres": request.form.getlist("genres"),
#                 "categories": request.form.getlist("categories"),
#             }

#             platforms = [p for p in request.form.getlist("platforms") if p]
#             raw_install_size = request.form.get("install_size_gb", "").strip()
#             raw_ram = request.form.get("ram_gb", "").strip()

#             if not all([nombre, payload["precio"], short_description, payload["detailed_description"], raw_install_size, raw_ram]):
#                 flash("Todos los campos basicos son obligatorios.", "error")
#                 return redirect(url_for("agregar_juego"))

#             if not platforms:
#                 flash("Selecciona al menos una plataforma.", "error")
#                 return redirect(url_for("agregar_juego"))

#             try:
#                 payload["precio"] = float(payload["precio"])
#                 install_size = float(raw_install_size)
#                 ram_gb = float(raw_ram)
#             except ValueError:
#                 flash("Los campos numericos deben ser validos.", "error")
#                 return redirect(url_for("agregar_juego"))

#             if install_size < 0 or ram_gb < 0:
#                 flash("Los valores numericos deben ser no negativos.", "error")
#                 return redirect(url_for("agregar_juego"))

#             payload.update(
#                 {
#                     "platforms": platforms,
#                     "install_size_gb": install_size,
#                     "ram_gb": ram_gb,
#                 }
#             )


#             try:
#                 response = requests.post(backend_url("/api/games"), json=payload, timeout=backend_timeout)
#             except requests.RequestException:
#                 flash("No se pudo comunicar con el backend.", "error")
#                 return redirect(url_for("agregar_juego"))

#             if response.status_code >= 400:
#                 try:
#                     error_payload = response.json()
#                 except ValueError:
#                     error_payload = {}
#                 message = error_payload.get("message") or "No se pudo guardar el juego."
#                 flash(message, "error")
#                 return redirect(url_for("agregar_juego"))

#             # If everything succeeded show loading screen
#             # return render_template("loading.html")
#             return redirect(url_for("loading"))


#         # GET request -> show form
#         return render_template("form.html")

#     @app.errorhandler(404)
#     def handle_not_found(_error):
#         return render_template("error.html", titulo="404", mensaje="No encontramos lo que buscas."), 404

#     @app.errorhandler(502)
#     def handle_backend_error(error):
#         mensaje = getattr(error, "description", None) or "Error al contactar el backend."
#         return render_template("error.html", titulo="502", mensaje=mensaje), 502

#     @app.route("/loading")
#     def loading():
#         return render_template("loading.html")

#     @app.route("/dashboard")
#     def dashboard():
#         return render_template("dashboard.html")

#     return app


# app = create_app()

# if __name__ == "__main__":
#     port = int(os.environ.get("FRONTEND_PORT", 5100))
#     app.run(debug=True, port=port)


from __future__ import annotations

import os
from typing import Any, Dict

import requests
from flask import Flask, flash, redirect, render_template, request, url_for, session, jsonify

def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    app.config["BACKEND_URL"] = os.environ.get("BACKEND_URL", "http://localhost:5001").rstrip("/")
    backend_timeout = float(os.environ.get("BACKEND_TIMEOUT", "120"))
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
            # Guardamos los datos del formulario en la sesión
            session["form_data"] = request.form.to_dict(flat=False)
            return redirect(url_for("loading"))
        return render_template("form.html")

    @app.route("/process_game", methods=["POST"])
    def process_game():
        form_data = session.get("form_data")
        if not form_data:
            return jsonify({"status": "error", "message": "No hay datos de formulario"}), 400

        try:
            nombre = form_data.get("nombre", [""])[0].strip()
            short_description = form_data.get("short_description", [""])[0].strip()
            detailed_description = form_data.get("detailed_description", [""])[0].strip()
            precio = float(form_data.get("precio", ["0"])[0])
            install_size = float(form_data.get("install_size_gb", ["0"])[0])
            ram_gb = float(form_data.get("ram_gb", ["0"])[0])
        except (ValueError, IndexError):
            return jsonify({"status": "error", "message": "Campos numericos invalidos"}), 400

        genres = form_data.get("genres", [])
        categories = form_data.get("categories", [])
        platforms = form_data.get("platforms", [])
        full_content_values = form_data.get("full_content_included", [])

        if not nombre or not short_description or not detailed_description:
            return jsonify({"status": "error", "message": "Faltan campos obligatorios"}), 400

        if not platforms:
            return jsonify({"status": "error", "message": "Debes seleccionar al menos una plataforma"}), 400

        if not full_content_values:
            return jsonify({"status": "error", "message": "Debes indicar si el contenido completo esta incluido"}), 400

        raw_full_content = str(full_content_values[0]).strip().lower()
        truthy_values = {"yes", "true", "1", "on", "si"}
        falsy_values = {"no", "false", "0", "off"}
        if raw_full_content in truthy_values:
            full_content_included = True
        elif raw_full_content in falsy_values:
            full_content_included = False
        else:
            return jsonify({"status": "error", "message": "Valor de contenido completo no valido"}), 400

        if precio < 0:
            return jsonify({"status": "error", "message": "El precio no puede ser negativo"}), 400

        if install_size <= 0:
            return jsonify({"status": "error", "message": "El tamano de instalacion debe ser mayor que 0"}), 400

        if ram_gb <= 0:
            return jsonify({"status": "error", "message": "La RAM recomendada debe ser mayor que 0"}), 400

        payload = {
            "nombre": nombre,
            "precio": precio,
            "short_description": short_description,
            "detailed_description": detailed_description,
            "genres": genres,
            "categories": categories,
            "platforms": platforms,
            "install_size_gb": install_size,
            "ram_gb": ram_gb,
            "full_content_included": full_content_included,
        }

        try:
            response = requests.post(backend_url("/api/games"), json=payload, timeout=backend_timeout)
            if response.status_code >= 400:
                try:
                    error_payload = response.json()
                except ValueError:
                    error_payload = {}
                message = error_payload.get("message") or "No se pudo guardar el juego."
                return jsonify({"status": "error", "message": message}), 400
        except requests.RequestException:
            return jsonify({"status": "error", "message": "No se pudo comunicar con el backend"}), 500

        session.pop("form_data", None)
        return jsonify({"status": "success"}), 200


    @app.route("/loading")
    def loading():
        return render_template("loading.html")

    @app.route("/dashboard")
    def dashboard():
        return render_template("dashboard.html")

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

