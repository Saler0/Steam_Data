# Usar una imagen base de Python
FROM python:3.11-slim

# Instalar MLflow (y dependencias si las hubiera, como para conectar a S3 o BBDD)
RUN pip install --no-cache-dir mlflow

# Puerto por defecto de la UI de MLflow
EXPOSE 5000

# Comando para iniciar el servidor de MLflow
CMD ["mlflow", "ui", "--host", "0.0.0.0", "--port", "5000"]