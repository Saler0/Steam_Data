# Data Management

El pipeline de Managemente pasa por las siguiente etapas:

1. Ingesta: Captura los datos que aún no han sido guardados en la trusted zone. Captura de APIs de Steam y Youtube.

2. Lading Zone: Es la zona de almacenamiento donde se colocan los datos recién ingeridos, en su formato original (raw), antes de ser procesados o validados.

3. Trusted Zone: Es la zona donde los datos han sido validados, limpiados y transformados a un formato coherente y confiable. En esta etapa, se asegura la calidad y consistencia de la información. Los datos se almacenan en MongoDB.

4. Exploitation Zone (zona de explotación): es la zona final donde los datos están listos para ser consumidos por herramientas de análisis, visualización o machine learning. También se almacenan en MongoDB y pueden estar optimizados para consultas rápidas y específicas.

Como ver los logs que entrego la ultima ejecucion del pipeline:
docker logs -f steam_data-app > steam_data-app.log 2>&1
