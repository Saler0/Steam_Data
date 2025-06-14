import requests
import json
import time
import logging
from datetime import datetime
import os



class ApiSteam:

    def __init__(self, appids):

        # Parámetros
        self.appids = appids
        self.reviews_per_game =  100
        self.country_code     =  "us"

        # Paths de landing zone
        self.lz_games_dir   = os.path.join("landing_zone", "api_steam")
        os.makedirs(self.lz_games_dir, exist_ok=True)

    def get_game_details(self, appid):
        try:
            url = (
                f"https://store.steampowered.com/api/appdetails"
                f"?appids={appid}&cc={self.country_code}&l=english"
            )
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            raw = res.json()
            section = raw.get(str(appid), {})
            if not section.get("success", False):
                return None, False
            return section["data"], False
        except Exception as e:
            logging.error(f"Error al obtener detalles para {appid}: {e}")
            return None, True
        
    def get_reviews_since_ts(self, appid, since_ts=0):
        try:
            cursor = "*"
            page = 1
            all_reviews = []

            while True:
                logging.info(f"🔍 Página {page} de reseñas (cursor={cursor[:10]}…) para {appid}")
                params = {
                    "json": 1,
                    "filter": "recent",
                    "language": "english",
                    "num_per_page": self.reviews_per_game,
                    "cursor": cursor
                }
                res = requests.get(
                    f"https://store.steampowered.com/appreviews/{appid}",
                    params=params, timeout=10
                )
                res.raise_for_status()
                data = res.json()
                reviews = data.get("reviews", [])
                if not reviews:
                    break

                for r in reviews:
                    if r.get("timestamp_created",0) <= since_ts:
                        logging.info("⏹ Reseña antigua detectada; paro descarga.")
                        return all_reviews, False
                    all_reviews.append(r)

                cursor = data.get("cursor")
                page += 1
                time.sleep(0.8)  # respetar rate-limit

            return all_reviews, False
        
        except Exception as e:
            logging.error(f"❌ Error al obtener detalles para {appid}: {e}")
            return [], True


    def run(self):
        games_ndjson_path = os.path.join(self.lz_games_dir, "steam_games.ndjson")

        # 0) cargar appids ya procesados
        processed = set()
        if os.path.exists(games_ndjson_path):
            with open(games_ndjson_path, "r", encoding="utf-8") as fg:
                for line in fg:
                    try:
                        rec = json.loads(line)
                        processed.add(rec.get("appid"))
                    except json.JSONDecodeError:
                        continue

        # 1)  abrir en modo append (solo añade los que falten)
        with open(games_ndjson_path, "a", encoding="utf-8") as fg:
            for appid in self.appids:
                if appid in processed:
                    logging.info(f"≡ Saltando {appid}: ya está en {games_ndjson_path}")
                    continue

                logging.info(f"📦 Fetch details {appid}")
                details, err = self.get_game_details(appid)
                record = {
                    "appid":   appid,
                    "details": details,
                    "error":   err
                }
                fg.write(json.dumps(record, ensure_ascii=False) + "\n")

        logging.info(f"✔ Creado NDJSON de juegos en {games_ndjson_path}")

        # 2) reviews: un NDJSON por cada appid
        for appid in self.appids:
            logging.info(f"⭐ Fetch reviews {appid}")
            reviews_path = os.path.join(self.lz_games_dir, f"reviews_{appid}.ndjson")
            last_ts = 0
            if os.path.exists(reviews_path):
                with open(reviews_path, encoding="utf-8") as fr_old:
                    for line in fr_old:
                        try:
                            r = json.loads(line)
                            ts = r.get("timestamp_created", 0)
                            if ts > last_ts:
                                last_ts = ts
                        except json.JSONDecodeError:
                            continue

            logging.info(f"⭐ Fetch reviews {appid} desde ts={last_ts}")
            reviews, err = self.get_reviews_since_ts(appid, last_ts)
            if err:
                logging.warning(f"❌ No se pudieron bajar reseñas para {appid}")
                continue

            # Grabo sólo las reseñas nuevas
            mode = "a" if os.path.exists(reviews_path) else "w"
            with open(reviews_path, mode, encoding="utf-8") as fr:
                for r in reviews:
                    fr.write(json.dumps(r, ensure_ascii=False) + "\n")
            logging.info(f"✔ Creado NDJSON de reseñas en {reviews_path}")


        # 3) extraigo nombres para devolverlos al pipeline
        game_names = []
        for line in open(games_ndjson_path, encoding="utf-8"):
            obj = json.loads(line)
            det = obj.get("details") or {}
            name = det.get("name")
            if name:
                game_names.append(name)

        logging.info("🎮 Nombres de juegos procesados:")
        for n in game_names:
            logging.info(f"   • {n}")

        return game_names

