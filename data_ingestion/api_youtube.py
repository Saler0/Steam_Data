import os
import json
import logging
from datetime import datetime
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from google.auth.credentials import AnonymousCredentials
import re

class ApiYoutube:
    def __init__(self, nombre_juegos, api_key):
        self.game_names = nombre_juegos
        self.api_key = api_key
        self.max_videos = 20

        # Cliente de YouTube usando credenciales
        self.yt = build(
            'youtube',
            'v3',
            developerKey=self.api_key,
            credentials=AnonymousCredentials(),
            cache_discovery=False
        )


        # Landing zone
        self.lz_dir = os.path.join("landing_zone", "api_youtube")
        os.makedirs(self.lz_dir, exist_ok=True)


    def slugify(selft, juego: str) -> str:
        # 1) reemplaza espacios, quita ™
        s = juego.replace(" ", "_").replace("™", "")
        # 2) elimina TODOS los caracteres reservados de Windows:
        #    <>:"/\|?*
        s = re.sub(r'[<>:"/\\|?*]', "", s)
        # 3) opcional: para que quede más limpio, deja sólo letras, números, guiones y guión bajo
        s = re.sub(r'[^0-9A-Za-z_-]', "", s)
        return s

    def buscar_videos(self, query):
        """Busca vídeos en YouTube para un término dado."""
        try:
            resp = (
                self.yt.search()
                       .list(
                           part="snippet",
                           q=query,
                           maxResults=self.max_videos,
                           type="video"
                       )
                       .execute()
            )
            return resp.get("items", [])
        except Exception as e:
            logging.error(f"Error en search('{query}'): {e}")
            return []


    def obtener_transcripcion(self, video_id):
        """Devuelve la transcripción completa como lista de segmentos."""
        try:
            return YouTubeTranscriptApi.get_transcript(
                video_id,
                languages=['en']
            )
        except Exception as e:
            logging.warning(f"No transcript para {video_id}: {e}")
            return []

    def guardar_search_ndjson(self, juego, items):
        """Graba cada item de search en NDJSON."""
        safe = self.slugify(juego)
        path = os.path.join(self.lz_dir, f"search_{safe}.ndjson")
        with open(path, "w", encoding="utf-8") as f:
            for itm in items:
                f.write(json.dumps(itm, ensure_ascii=False) + "\n")
        logging.info(f"✔ Search NDJSON: {path}")        

    def guardar_transcript_ndjson(self, video_id, segmentos):
        """Graba cada segmento de transcript en NDJSON."""
        path = os.path.join(self.lz_dir, f"transcript_{video_id}.ndjson")
        with open(path, "w", encoding="utf-8") as f:
            for seg in segmentos:
                f.write(json.dumps(seg, ensure_ascii=False) + "\n")
        logging.info(f"✔ Transcript NDJSON: {path}")

    def guardar_comments_ndjson(self, video_id, items):
        """Graba cada comentario (item) en NDJSON."""
        path = os.path.join(self.lz_dir, f"comments_{video_id}.ndjson")
        with open(path, "w", encoding="utf-8") as f:
            for itm in items:
                f.write(json.dumps(itm, ensure_ascii=False) + "\n")
        logging.info(f"✔ Comments NDJSON: {path}")




    def run(self):

        for juego in self.game_names:
            safe = self.slugify(juego)

            # 1) Search
            path_search = os.path.join(self.lz_dir, f"search_{safe}.ndjson")
            if os.path.exists(path_search) and os.path.getsize(path_search) > 0:
                logging.info(f"≡ Saltando search para «{juego}»: ya existe {path_search}")
                # opcional: cargar items desde el fichero
                with open(path_search, encoding="utf-8") as f:
                    items = [json.loads(l) for l in f]
            else:
                logging.info(f"🔍 Buscando vídeos para: {juego}")
                items = self.buscar_videos(juego)
                self.guardar_search_ndjson(juego, items)

            video_ids = [
                itm["id"]["videoId"]
                for itm in items
                if itm.get("id", {}).get("videoId")
            ]

            # 2) Transcripciones
            for vid in video_ids:
                path_trans = os.path.join(self.lz_dir, f"transcript_{vid}.ndjson")
                if os.path.exists(path_trans) and os.path.getsize(path_trans) > 0:
                    logging.info(f"≡ Saltando transcript para {vid}: ya existe {path_trans}")
                    continue
                logging.info(f"💬 Obteniendo transcript para: {vid}")
                segmentos = self.obtener_transcripcion(vid)
                self.guardar_transcript_ndjson(vid, segmentos)

            # 3) Comentarios
            for vid in video_ids:
                path_comm = os.path.join(self.lz_dir, f"comments_{vid}.ndjson")
                if os.path.exists(path_comm) and os.path.getsize(path_comm) > 0:
                    logging.info(f"≡ Saltando comments para {vid}: ya existe {path_comm}")
                    continue

                logging.info(f"💬 Obteniendo comentarios para: {vid}")
                all_comments = []
                token = None
                while True:
                    try:
                        resp = (
                            self.yt.commentThreads()
                                .list(
                                    part="snippet",
                                    videoId=vid,
                                    maxResults=100,
                                    textFormat="plainText",
                                    pageToken=token
                                )
                                .execute()
                        )
                    except Exception as e:
                        logging.warning(f"Error comments {vid}: {e}")
                        break

                    batch = resp.get("items", [])
                    all_comments.extend(batch)
                    token = resp.get("nextPageToken")
                    if not token:
                        break

                self.guardar_comments_ndjson(vid, all_comments)

        logging.info("✅ ApiYoutube completado.")