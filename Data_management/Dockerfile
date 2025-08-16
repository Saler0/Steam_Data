# 1) Imagen base estable con Java17 disponible
FROM python:3.11-slim-bookworm

# 2) Variables de entorno
ENV PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    PYTHONIOENCODING=utf-8 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:$PATH" \
    SPARK_JARS_PACKAGES=org.mongodb.spark:mongo-spark-connector_2.12:3.0.1

# 3) Instalar Java, curl y otras utilidades
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl procps ca-certificates gnupg \
    && rm -rf /var/lib/apt/lists/* \
    && test -x "$JAVA_HOME/bin/java" && "$JAVA_HOME/bin/java" -version

# 4) Instalar Spark 3.5.1
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark.tgz \
    && tar -xzf spark.tgz -C /opt \
    && mv /opt/spark-3.5.1-bin-hadoop3 /opt/spark \
    && rm spark.tgz



# 5) Crear directorio de la app
WORKDIR /app

# 6) Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

# 7) Copiar el resto del código
COPY . .

# 8) Expone puertos si tu app los usa (p.ej. HTTP)
# EXPOSE 8000

# 9) Punto de entrada: ajusta según tu script principal
CMD ["python", "main.py"]