
# 🎮 Proyecto de Clustering y Recomendación de Videojuegos en Steam

Este repositorio implementa un pipeline completo de Big Data y MLOps para analizar, agrupar y recomendar videojuegos basados en descripciones y características extraídas de Steam. Utiliza tecnologías escalables como **Apache Spark**, **FAISS**, **DVC** y **MLflow**.

---

## 🧱 Estructura del Proyecto

```
modelos_steam_data/
├── features.py                      # Extracción de embeddings y limpieza
├── train_clustering_faiss_spark.py # Clustering KMeans con Spark + FAISS + MLflow
├── predict_faiss_mlflow.py         # Similitud de juegos cliente con FAISS
├── params.yaml                     # Hiperparámetros versionados
├── dvc.yaml                        # Definición del pipeline DVC
├── data/processed/                 # Embeddings, clusters y salidas procesadas
├── models/clustering/              # Modelos KMeans y FAISS
├── reports/similarity/             # Juegos similares recomendados
```

---

## ⚙️ Tecnologías Principales

- **Apache Spark** para clustering escalable
- **FAISS** para búsquedas por similitud rápida
- **DVC** para versionado de datos y pipeline reproducible
- **MLflow** para tracking de parámetros, métricas y artefactos

---

## 🚀 Ejecución del Pipeline

1. **Instala las dependencias**:

```bash
pip install -r requirements.txt
```

2. **Ejecuta el pipeline completo con DVC**:

```bash
dvc repro
```

3. **Lanza la interfaz de MLflow para ver experimentos**:

```bash
mlflow ui
```

Abre: [http://localhost:5000](http://localhost:5000)

---

## 🧪 Parámetros configurables

Edita el archivo `params.yaml` para modificar:

```yaml
clustering:
  min_k: 5
  max_k: 30
  random_state: 42
  n_init: 10

similitud:
  umbral: 0.90
```

---

## 💾 Versionado de datos

Para guardar los artefactos de DVC:

```bash
dvc push
```

Y para recuperarlos en otra máquina:

```bash
dvc pull
```

---

## 📬 ¿Futuro?

Este pipeline está listo para integrarse con una **API FastAPI** que permita al cliente enviar la descripción de su videojuego y recibir los más similares. También puede conectarse a dashboards o sistemas de analítica en tiempo real.

---

## ✍️ Autor

Desarrollado como parte de un proyecto de TFM en Big Data sobre Steam. 🧠🔥
