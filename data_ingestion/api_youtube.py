from youtube_transcript_api import YouTubeTranscriptApi
from googleapiclient.discovery import build
import json  

def obtener_transcripcion(video_id, como_lista=True):
    """
    Devuelve la transcripción de un vídeo de YouTube dado su ID.
    Si como_lista=True, devuelve una lista de fragmentos. Por defecto, devuelve un string.
    """
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=['es', 'en'])
        if como_lista:
            return [x['text'] for x in transcript]
        else:
            return ' '.join([x['text'] for x in transcript])
    except Exception as e:
        return False


def guardar_comentarios_individuales(video_ids, api_key, archivo_salida="comentarios.json"):
    """
    Extrae comentarios de varios videos y los guarda como documentos individuales en un único archivo JSON.
    Ahora incluye el título del video en cada comentario.
    """
    youtube = build('youtube', 'v3', developerKey=api_key)
    todos_los_comentarios = []

    for video_id in video_ids:
        print(f"Procesando video: {video_id}")

        # Obtener título del video
        try:
            video_request = youtube.videos().list(
                part="snippet",
                id=video_id
            )
            video_response = video_request.execute()
            titulo_video = video_response['items'][0]['snippet']['title']
        except Exception as e:
            print(f"Error al obtener título del video {video_id}: {e}")
            titulo_video = ""

        next_page_token = None

        while True:
            try:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,
                    textFormat="plainText",
                    pageToken=next_page_token
                )
                response = request.execute()

                for item in response['items']:
                    snippet = item['snippet']['topLevelComment']['snippet']
                    comentario = {
                        "video_id": video_id,
                        "titulo_video": titulo_video,   # <-- Aquí agregamos el título
                        "comentario_id": item['id'],
                        "texto": snippet.get('textDisplay', ''),
                        "likes": snippet.get('likeCount', 0),
                        "autor": snippet.get('authorDisplayName', ''),
                        "fecha": snippet.get('publishedAt', '')
                    }
                    todos_los_comentarios.append(comentario)

                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break

            except Exception as e:
                print(f"Error al obtener comentarios de {video_id}: {e}")
                break

    with open(archivo_salida, "w", encoding="utf-8") as f:
        json.dump(todos_los_comentarios, f, ensure_ascii=False, indent=2)

    print(f"Guardados {len(todos_los_comentarios)} comentarios en {archivo_salida}")


def buscar_videos_youtube(query, api_key, max_results=5):
    """
    Busca vídeos en YouTube por un término dado y devuelve una lista de diccionarios
    con el ID y título de los vídeos encontrados.
    """
    youtube = build('youtube', 'v3', developerKey=api_key)

    request = youtube.search().list(
        part="snippet",
        q=query,
        maxResults=max_results,
        type="video"
    )
    response = request.execute()

    videos = []
    for item in response['items']:
        video_id = item['id']['videoId']
        titulo = item['snippet']['title']
        videos.append({'video_id': video_id, 'titulo': titulo})

    return videos


def guardar_transcripcion_json(video_id, titulo, ruta_archivo):
    """
    Guarda la transcripción de un vídeo en un archivo JSON.
    """
    transcripcion = obtener_transcripcion(video_id, como_lista=True)

    if isinstance(transcripcion, str) and transcripcion.startswith("Error"):
        print(transcripcion)
        return
    if transcripcion == False:
        error = True
    else:
        error = False
    with open(ruta_archivo, 'w', encoding='utf-8') as f:
        json.dump({
            "video_id": video_id,
            "título":  titulo,
            "error": error,
            "transcripcion": transcripcion
        }, f, ensure_ascii=False, indent=2)
