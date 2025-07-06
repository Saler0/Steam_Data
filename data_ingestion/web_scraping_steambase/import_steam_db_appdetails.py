import requests
import json
import os

# --- Configuración ---
# Necesitarás un App ID de Steam para cada juego.
# Puedes encontrarlo en la URL del juego en la tienda de Steam (ej: https://store.steampowered.com/app/570/Dota_2/ -> App ID es 570)
APP_ID_EXAMPLE = 570 # Ejemplo: Dota 2

# Para obtener la cantidad de jugadores, necesitas una Steam Web API Key.
# Consíguela aquí: https://steamcommunity.com/dev/apikey
# Por seguridad, es mejor NO incrustar la API Key directamente en el código fuente para proyectos grandes.
# Para este ejemplo, la pediremos por consola o la leeremos de una variable de entorno.
# Si la tienes como variable de entorno, descomenta la línea de abajo y asegúrate de configurarla.
# STEAM_WEB_API_KEY = os.environ.get("STEAM_API_KEY")
STEAM_WEB_API_KEY = None # Se pedirá al usuario más tarde

# --- Funciones para obtener datos ---

def get_game_price(app_id, country_code='us', language='english'):
    """
    Obtiene información de precios y detalles del juego desde la Steam Storefront API.
    :param app_id: ID de la aplicación del juego.
    :param country_code: Código de país (ej: 'es', 'us', 'de') para localización de precios.
    :param language: Idioma de la respuesta (ej: 'spanish', 'english').
    :return: Diccionario con detalles del precio o None si falla.
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}&cc={country_code}&l={language}"
    print(f"Consultando precios para App ID {app_id}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lanza una excepción para códigos de estado HTTP erróneos (4xx o 5xx)
        data = response.json()

        if str(app_id) in data and data[str(app_id)]['success']:
            game_data = data[str(app_id)]['data']
            price_info = game_data.get('price_overview')
            if price_info:
                print(f"  Nombre: {game_data.get('name', 'N/A')}")
                print(f"  Precio Inicial: {price_info.get('initial_formatted', 'N/A')}")
                print(f"  Precio Final: {price_info.get('final_formatted', 'N/A')}")
                print(f"  Porcentaje de Descuento: {price_info.get('discount_percent', 'N/A')}%")
                return price_info
            else:
                print(f"  No hay información de precios disponible para App ID {app_id}. Puede ser un juego gratuito o no listado.")
                return None
        else:
            print(f"  Error al obtener datos para App ID {app_id}: {data.get(str(app_id), {}).get('data', {}).get('message', 'Juego no encontrado o error en la API.')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  Error de conexión al intentar obtener precios para App ID {app_id}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"  Error al decodificar la respuesta JSON para App ID {app_id}.")
        return None

def get_player_count(app_id, api_key):
    """
    Obtiene la cantidad actual de jugadores para un juego desde la Steam Web API.
    Requiere una Steam Web API Key.
    :param app_id: ID de la aplicación del juego.
    :param api_key: Tu Steam Web API Key.
    :return: Cantidad de jugadores como int, o None si falla.
    """
    if not api_key:
        print("Error: No se proporcionó una Steam Web API Key. No se puede obtener el número de jugadores.")
        return None

    url = f"https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={app_id}&key={api_key}"
    print(f"Consultando cantidad de jugadores para App ID {app_id}...")
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if 'response' in data and data['response']['result'] == 1:
            player_count = data['response']['player_count']
            print(f"  Jugadores actuales: {player_count}")
            return player_count
        else:
            print(f"  Error al obtener cantidad de jugadores para App ID {app_id}: {data.get('response', {}).get('message', 'Error en la API o juego no encontrado.')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  Error de conexión al intentar obtener jugadores para App ID {app_id}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"  Error al decodificar la respuesta JSON para App ID {app_id}.")
        return None

# --- Funciones principales y ejecución ---

def main():
    global STEAM_WEB_API_KEY # Necesitamos modificar la variable global

    print("--- Obtención de Datos de Juegos de Steam en Python ---")
    print("Para este script, necesitarás:")
    print("1. El 'App ID' de Steam del juego (lo encuentras en la URL de la tienda de Steam).")
    print("2. Tu 'Steam Web API Key' (la obtienes en https://steamcommunity.com/dev/apikey).")
    print("-" * 50)

    # Pedir el App ID al usuario
    game_app_id = input(f"Introduce el App ID del juego (ej: {APP_ID_EXAMPLE}): ").strip()
    if not game_app_id.isdigit():
        print("El App ID debe ser un número. Saliendo.")
        return
    game_app_id = int(game_app_id)

    # Pedir la API Key si no está configurada como variable de entorno
    if not STEAM_WEB_API_KEY:
        STEAM_WEB_API_KEY = input("Introduce tu Steam Web API Key: ").strip()
        if not STEAM_WEB_API_KEY:
            print("API Key no proporcionada. No se podrá obtener la cantidad de jugadores. Saliendo.")
            return

    print("\n--- Buscando información de precios ---")
    price_info = get_game_price(game_app_id, country_code='es', language='spanish')

    print("\n--- Buscando cantidad de jugadores ---")
    player_count = get_player_count(game_app_id, STEAM_WEB_API_KEY)

    print("\n--- Resumen de la información obtenida ---")
    if price_info:
        print(f"Juego: {price_info.get('name', 'N/A')}")
        print(f"Precio en EUR: {price_info.get('final_formatted', 'N/A')}")
        print(f"Descuento: {price_info.get('discount_percent', 'N/A')}%")
    else:
        print("No se pudo obtener información de precios.")

    if player_count is not None:
        print(f"Jugadores actuales: {player_count}")
    else:
        print("No se pudo obtener la cantidad de jugadores.")

    print("\n--- Información sobre Streaming y Redes Sociales ---")
    print("La API de Steam no proporciona directamente información de streaming en plataformas como Twitch o YouTube,")
    print("ni métricas detalladas de redes sociales (como número de seguidores, actividad).")
    print("Para obtener esos datos, necesitarías usar las APIs específicas de esas plataformas (ej: Twitch API, YouTube Data API, X API, Facebook Graph API, etc.).")
    print("Esto implicaría un esfuerzo de integración adicional para cada plataforma.")

if __name__ == "__main__":
    main()