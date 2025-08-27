# Documentación del Proyecto de Análisis de Datos de Steam

## 1. Visión General

Este proyecto es una plataforma completa de ingeniería y ciencia de datos diseñada para recopilar, procesar, analizar y modelar datos de la plataforma Steam. El sistema está completamente contenedorizado con Docker, lo que garantiza su portabilidad y facilidad de despliegue.

El objetivo principal es extraer datos de diversas fuentes (APIs, Web Scraping), procesarlos a través de un pipeline de ETL escalable con Spark, y prepararlos para análisis y modelos de Machine Learning, como sistemas de recomendación por clustering.

---

## 2. Arquitectura y Flujo de Datos

La arquitectura del proyecto sigue un modelo moderno y desacoplado. Para una visualización interactiva y detallada de todos los componentes y cómo fluyen los datos entre ellos, abre el siguiente archivo en tu navegador:

- **`architecture_flow.html`**

Este diagrama muestra el viaje de los datos desde las fuentes originales hasta las bases de datos de explotación y los modelos de ML.

---

## 3. Stack Tecnológico

El proyecto integra un conjunto de tecnologías robustas para cada etapa del proceso:

- **Orquestación de Contenedores**: Docker & Docker Compose
- **Lenguaje Principal**: Python 3.9
- **Procesamiento de Big Data**: Apache Spark
- **Base de Datos NoSQL**: MongoDB (para los datos principales del pipeline)
- **Base de Datos Relacional**: SQLite (para datos históricos del scraper)
- **Web Scraping**: BeautifulSoup, Requests
- **MLOps**: 
    - **MLflow**: Seguimiento de experimentos y gestión de modelos.
    - **DVC**: Versionado de datos para reproducibilidad.
- **Búsqueda de Similitud**: FAISS (de Facebook AI)

---

## 4. Estructura del Proyecto

El repositorio está organizado en los siguientes directorios clave:

- **`/Data_management`**: Contiene todo el código del pipeline de datos (ETL).
    - **`/data_ingestion`**: Scripts para la recolección de datos (APIs, scrapers).
    - **`/db`**: Scripts para la inicialización de esquemas de bases de datos.
    - **`/landing_zone`**: Directorio donde se depositan los datos crudos (punto de partida del pipeline).
    - **`/data`**: Directorio donde se almacenan bases de datos locales, como el archivo SQLite.
    - `Dockerfile`, `Dockerfile.scraping`: Archivos para construir las imágenes de Docker de los pipelines.
    - `main.py`, `main_mensual.py`: Puntos de entrada para los diferentes flujos de trabajo.

- **`/modelos_steam_data`**: Contiene todo lo relacionado con el modelado de Machine Learning y análisis.

- **`docker-compose.yml`**: El archivo principal que orquesta todos los servicios, volúmenes y redes del proyecto.

---

## 5. Guía de Uso Paso a Paso

Sigue estos pasos para configurar y ejecutar el proyecto.

### Prerrequisitos

- Tener instalado **Docker** y **Docker Compose** en tu sistema.

### Workflow 1: Ejecutar el Scraper Mensual

Este proceso recopila datos históricos de jugadores desde Steamcharts y los guarda en la base de datos local SQLite.

1.  **Prepara el archivo de entrada**: Asegúrate de que en la carpeta `Data_management/landing_zone/` exista un archivo `.ndjson` con los juegos que quieres procesar. El sistema está configurado para usar `unprocessed_games.ndjson` por defecto.

2.  **Ejecuta el servicio**: Abre una terminal en la raíz del proyecto y ejecuta el siguiente comando:

    ```sh
    docker-compose --profile scraping up --build
    ```

    - `--profile scraping`: Activa el perfil específico para este servicio.
    - `--build`: Reconstruye la imagen de Docker si has hecho cambios en el código.

3.  **Resultado**: El scraper se ejecutará y se detendrá al finalizar. La base de datos se creará o se actualizará en `Data_management/data/steam_data.db`.

### Workflow 2: Ejecutar el Pipeline ETL Principal con Spark

Este proceso toma los datos de la `landing_zone`, los procesa con Spark y los carga en la base de datos MongoDB en las zonas "trusted" y "explotation".

1.  **Asegúrate de que MongoDB esté corriendo**: Puedes iniciar solo MongoDB si lo necesitas con `docker-compose up -d mongo`.

2.  **Ejecuta el pipeline**:

    ```sh
    docker-compose --profile seed up --build
    ```

3.  **Resultado**: El job de Spark se ejecutará y se detendrá al finalizar, procesando y moviendo los datos a MongoDB.

### Workflow 3: Levantar los Servicios Base

Si quieres levantar los servicios de fondo como MongoDB y MLflow para desarrollo o análisis:

```sh
docker-compose up -d mongo mlflow
```

Esto iniciará los contenedores en modo "detached" (-d).

### Workflow 4: Acceder al Entorno de Análisis

Para experimentar, ejecutar notebooks o scripts de análisis:

1.  **Inicia el contenedor de analítica** (asegúrate de que los servicios base como mongo estén corriendo):
    ```sh
    docker-compose up -d analytics
    ```

2.  **Accede a una terminal dentro del contenedor**:
    ```sh
    docker-compose exec analytics bash
    ```

Ahora estás dentro del contenedor y tienes acceso a todos los scripts y herramientas de Python para realizar análisis.
