# Usar una imagen base de Python
FROM python:3.11-slim

# Instalar MLflow y dependencias para S3 (aunque no se use, es buena práctica tenerlo)
RUN pip install --no-cache-dir mlflow boto3

# Puerto por defecto de la UI de MLflow
EXPOSE 5000

# Crear el directorio que usaremos para el backend y los artefactos
RUN mkdir -p /mlruns

# Comando para iniciar el servidor de MLflow de forma robusta
# - El backend-store-uri apunta a donde se guardan los metadatos (experimentos, runs)
# - El default-artifact-root es donde se guardan los ficheros (modelos, etc.)
# Apuntamos ambos a la misma carpeta, que será montada con un volumen.
CMD ["mlflow", "server", \
     "--host", "0.0.0.0", \
     "--port", "5000", \
     "--backend-store-uri", "file:///mlruns", \
     "--default-artifact-root", "file:///mlruns"]