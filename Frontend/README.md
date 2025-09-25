# Frontend de Steam Data

Aplicacion Flask que renderiza el sitio Gamebooster y consume la API REST del backend.

## Requisitos

- Python 3.11 o superior
- API backend ejecut�ndose (por defecto `http://localhost:5001`)
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
2. Instala dependencias dentro del entorno activo:
   ```bash
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```
a
## Configuracion

- `BACKEND_URL`: URL del backend. Por defecto `http://localhost:5001`.
- `FRONTEND_SECRET_KEY`: clave para sesiones y mensajes flash. Por defecto `front-secret-key`.

Ejemplo en PowerShell:
```powershell
$env:BACKEND_URL = "http://localhost:8000"
$env:FRONTEND_SECRET_KEY = "cambia-esta-clave"
```

## Ejecucion del frontend

1. Asegura que el backend sea alcanzable y el entorno virtual este activo.
2. Inicia la aplicacion:
   ```bash
   python app.py
   ```
3. Abre `http://localhost:5000` (o el puerto configurado) en el navegador.
