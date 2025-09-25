# Proyecto Steam Data
El proyecto tiene las siguiente estructura:

- Data_management: Contiene lo relacionado al pipeline desde la ingesta de datos hasta la exploitation zone.
- Data_analytics: Contiene lo relacionado con los modelos y clustering.
- Frontend: Aplicacion web.
- Backend: API que conecta la aplicacion web con la base de datos.

## Para Desarrolladores

Como clonar el repositorio:
`ash
git clone https://github.com/Saler0/Steam_Data.git
`

Como crear una branch en base a la branch development:
`ash
git checkout -b <nombre_de_la_branch> development
`

Como subir cambios al repositorio:
`ash
git add -A
git commit -m "Comentario descriptivo sobre el commit"
git push origin <nombre_de_la_branch>
`

Como traer los ultimos cambios sobre una branch:
`ash
git pull origin <nombre_de_la_branch>
`

Cuando la branch se encuentra lista para la funcionalidad que fue creada se puede hacer un pull request para poder fusionarla con la rama development:
`ash
gh pr create --base development --head <nombre_de_la_branch> --title "titulo" --body "body"
`

Como aceptar un pull request:
`ash
gh pr merge <nombre_de_la_branch>
`

Finalmente, para actualizar los cambios del merge en development se hace:
`ash
git checkout development
git pull origin development
`

## Docker pipeline unificado

El nuevo Dockerfile en la raiz construye una imagen con todo el stack de Data_management + Data_analytics. El contenedor puede ejecutar la ingesta completa o solo las piezas de analitica segun el modo configurado.

### Preparacion inicial

1. Copia los archivos de entorno: cp Data_management/.env.example Data_management/.env y cp Data_analytics/.env.example Data_analytics/.env.
2. Ajusta credenciales/URIs en ambos .env segun tu entorno (por ejemplo MONGO_URI, claves de APIs, etc.).
3. Construye la imagen: docker compose --profile pipeline build pipeline.

### Ejecutar el pipeline completo

- Levanta dependencias y corre todo el flujo (Data_management -> Data_analytics):
  `ash
  docker compose --profile pipeline up pipeline
  `
  El contenedor sale al finalizar; revisa Data_management/logs y Data_analytics/outputs para los artefactos.

### Modos disponibles (PIPELINE_MODE)

- ull (default): ingesta + analytics con los pasos por defecto (generate_embeddings, un_clustering, ccf, events, 	opics, 
ews, enrich).
- nalytics: solo los pasos de analitica; omite Data_management.
- clustering: ejecuta embeddings (salvo SKIP_EMBEDDINGS=1) y clustering.
- embeddings: genera unicamente los embeddings.
- shell: abre una shell interactiva dentro del contenedor para ejecucion manual.

Puedes personalizar los pasos analiticos con la variable ANALYTICS_STEPS, por ejemplo:
`ash
docker compose --profile pipeline run --rm -e ANALYTICS_STEPS="generate_embeddings run_clustering" pipeline
`

### Caso: solo clustering con datos ya cargados en Mongo

Si tu amigo ya tiene la zona de explotacion en Mongo, basta con:
`ash
docker compose --profile pipeline run --rm \
  -e PIPELINE_MODE=clustering \
  -e MONGO_URI="mongodb://tu.mongo:27017" \
  pipeline
`

- El contenedor genera embeddings automaticamente antes de ejecutar el clustering (a menos que definas SKIP_EMBEDDINGS=1).
- Ajusta CLUSTERING_CONFIG o EMBEDDINGS_CONFIG si necesitas apuntar a configuraciones personalizadas.

### Tips adicionales

- El contenedor hereda Data_management y Data_analytics como volumenes, por lo que los resultados quedan disponibles en la carpeta del repositorio.
- Para ejecutar pasos manuales desde dentro del contenedor: docker compose --profile pipeline run --rm -e PIPELINE_MODE=shell pipeline y luego lanza los scripts a mano.
- Si no necesitas MLflow, establece MLFLOW_TRACKING_URI="" antes de lanzar el contenedor.
