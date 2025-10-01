# API Backend de Steam Data

Esta aplicacion Flask expone el catalogo de juegos mediante una API REST. Gestiona la persistencia con SQLAlchemy y permite configurar la URL de la base de datos.

## Requisitos

- Python 3.11 o superior
- Entorno virtual por proyecto (recomendado)

## Configuracion inicial

1. Crea y activa un entorno virtual antes de instalar dependencias.
   - PowerShell (Windows):
     ```powershell
     python -m venv venv
     .\venv\Scripts\Activate.ps1
     ```
   - macOS/Linux bash:
     ```bash
     python -m venv venv
     source venv/bin/activate
     ```
2. Instala las dependencias dentro del entorno activo:
   ```bash
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```

## Ejecucion del backend

1. Verifica que el entorno virtual este activado (prefijo `venv` en la consola).
2. Variables de entorno opcionales:
   - `DATABASE_URL` (por defecto `sqlite:///mi_db.db`).
   - `BACKEND_PORT` (por defecto `5001`).
3. Inicia el servidor:
   ```bash
   python app.py
   ```
4. Rutas disponibles:
   - `GET /api/health`
   - `GET /api/games`
   - `POST /api/games`
   - `GET /api/games/<id>`
   - `PUT /api/games/<id>`
   - `DELETE /api/games/<id>`

La aplicacion habilita CORS para `/api/*` de modo que la consuma el servicio frontend.


