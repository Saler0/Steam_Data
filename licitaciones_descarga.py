import requests
from bs4 import BeautifulSoup
import zipfile
import os

# URL de la página
url = "https://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas/LicitacionesAgregacion.aspx"

# Directorio donde se guardarán los archivos
base_dir = "licitaciones_datos"
os.makedirs(base_dir, exist_ok=True)

# Hacer una solicitud a la página
response = requests.get(url)
response.raise_for_status()  # Asegura que la solicitud fue exitosa

# Parsear el HTML con BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Encontrar todos los enlaces que terminan en .zip
# Inspeccionando la página, los enlaces están en etiquetas <a> con href que terminan en .zip
zip_links = soup.find_all('a', href=lambda href: href and href.endswith('.zip'))
print(zip_links)

# Descargar cada archivo ZIP
for link in zip_links:
    zip_url = link['href']
    # Si el enlace no es absoluto, lo hacemos absoluto
    if not zip_url.startswith('http'):
        zip_url = "https://www.hacienda.gob.es" + zip_url

    # Nombre del archivo (por ejemplo, "2025-Enero.zip")
    file_name = zip_url.split('/')[-1]
    # Extraer el año y mes del nombre del archivo (para organizar carpetas)
    year_month = file_name.replace('.zip', '')  # Ejemplo: "2025-Enero"
    year = year_month.split('-')[0]  # Ejemplo: "2025"

    # Crear directorio para el año
    year_dir = os.path.join(base_dir, year)
    os.makedirs(year_dir, exist_ok=True)

    # Ruta donde se guardará el archivo ZIP
    zip_path = os.path.join(year_dir, file_name)

    # Descargar el archivo
    print(f"Descargando {file_name}...")
    try:
        zip_response = requests.get(zip_url, timeout=10)
        zip_response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error al descargar {zip_url}: {e}")
    
    with open(zip_path, 'wb') as f:
        f.write(zip_response.content)

    # Descomprimir el archivo ZIP
    extract_dir = os.path.join(year_dir, year_month)
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print(f"Descomprimido {file_name} en {extract_dir}")

print("Descarga y descompresión completadas.")
