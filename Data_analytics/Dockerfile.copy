# Usa una imagen base de Python delgada (slim). Es una excelente elección.
FROM python:3.11-slim

# Define el directorio de trabajo desde el principio.
WORKDIR /app

# --- INSTALACIÓN DE DEPENDENCIAS ---

# 1. Combina los comandos de apt-get en una sola capa (RUN) para reducir el tamaño de la imagen.
#    Se instalan las dependencias del sistema necesarias para compilar algunas librerías de Python.
#    'libgomp1' es necesaria para Faiss y otras librerías de computación científica.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    make \
    cmake \
    git \
    curl \
    ca-certificates \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# 2. Copia SOLO el fichero de requisitos e instálalos en su propia capa.
#    Esto aprovecha el caché de Docker. Si no cambias el requirements.txt,
#    Docker no volverá a ejecutar este paso, haciendo las reconstrucciones mucho más rápidas.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 3. Copia el resto del código de la aplicación.
#    Este es el último paso para que los cambios en el código no invaliden el caché de las dependencias.
COPY . /app

# --- CONFIGURACIÓN DEL ENTORNO ---

# Define variables de entorno que estarán disponibles dentro del contenedor.
ENV PYTHONUNBUFFERED=1
ENV MLFLOW_TRACKING_URI=http://mlflow:5000
# Apunta a la ruta donde se montarán las credenciales de GCP desde docker-compose.
ENV GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcp.json

# El comando por defecto ha sido movido a docker-compose.yml,
# lo que es más flexible. Dejar el Dockerfile sin CMD es una práctica común
# para imágenes que se usarán en diferentes contextos.

