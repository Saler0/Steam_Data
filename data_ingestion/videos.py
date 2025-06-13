import os
from funciones import obtener_transcripcion, buscar_videos_youtube, guardar_comentarios_individuales, guardar_transcripcion_json

API_KEY = ""

videojuegos_name_list = ["EXPEDITION 33", "SEKIRO", "The First Berserker Khazan", "Lords of the Fallen", "DARK SOULS II"]
videojuegos_diccionario = dict()

# Crear carpeta para guardar las transcripciones si no existe
os.makedirs("transcripciones", exist_ok=True)

for nombre_juego in videojuegos_name_list:
    print(f"\nBuscando vídeos para el juego: {nombre_juego}")
    videos = buscar_videos_youtube(query=nombre_juego, api_key=API_KEY, max_results=20)
    video_ids = [video['video_id'] for video in videos]
    videojuegos_diccionario[nombre_juego] = videos

    archivo_salida_comentarios = f"comentarios_{nombre_juego.replace(' ', '_').lower()}.json"
    print(f"Guardando comentarios de los vídeos de '{nombre_juego}' en '{archivo_salida_comentarios}'...")
    guardar_comentarios_individuales(video_ids, API_KEY, archivo_salida_comentarios)

    for video in videos:
        video_id = video['video_id']
        titulo = video['titulo']
        archivo_transcripcion = f"transcripciones/{video_id}.json"
        print(f"Guardando transcripción del vídeo '{titulo}' en '{archivo_transcripcion}'...")
        guardar_transcripcion_json(video_id, titulo, archivo_transcripcion)
