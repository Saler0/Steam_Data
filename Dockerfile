# General pipeline image combining Data_management and Data_analytics
FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        g++ \
        make \
        cmake \
        git \
        curl \
        ca-certificates \
        libgomp1 \
        openjdk-17-jre-headless \
        procps \
        dos2unix \
        gnupg \
    && rm -rf /var/lib/apt/lists/*

ARG SPARK_VERSION=3.5.1
ARG HADOOP_PACKAGE=hadoop3
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-${HADOOP_PACKAGE}.tgz -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-${HADOOP_PACKAGE} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Instala dependencias de ambos subproyectos aprovechando caché
COPY Data_management/requirements.txt /tmp/requirements_dm.txt
COPY Data_analytics/requirements.txt /tmp/requirements_da.txt
RUN pip install --upgrade pip \
    && pip install -r /tmp/requirements_dm.txt \
    && pip install -r /tmp/requirements_da.txt

# Copia el repositorio completo
COPY . .

# Asegura que los módulos estén en PYTHONPATH
ENV PYTHONPATH="/app/Data_management:/app/Data_analytics:${PYTHONPATH}"

# Prepara scripts
RUN chmod +x /app/docker/pipeline-entrypoint.sh

ENTRYPOINT ["/app/docker/pipeline-entrypoint.sh"]
