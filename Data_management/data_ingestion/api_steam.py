import requests
import json
import time
import logging
from datetime import date
import os



class ApiSteam:

    def __init__(self, trusted_client,appids_to_process_reviews):

        self.trusted_client = trusted_client

        # Parámetros
        self.reviews_per_game =  100
        self.country_code     =  "us"

        # Paths de landing zone
        self.lz_games_dir   = os.path.join("landing_zone", "api_steam")
        os.makedirs(self.lz_games_dir, exist_ok=True)

        # appdi a los que se les buscara su review
        self.appids_to_process_reviews = appids_to_process_reviews

    def get_all_games(self):
        url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
        res = requests.get(url)
        res.raise_for_status()
        return res.json()["applist"]["apps"]

    def get_game_details(self, appid):
        try:
            time.sleep(1.3)
            url = (
                f"https://store.steampowered.com/api/appdetails?appids={appid}&cc={self.country_code}&l=english"
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

        # 0) Lista completa de appids de Steam
        all_games   = self.get_all_games()
        all_appids  = sorted({g["appid"] for g in all_games})

        # 1) Detectar cuáles ya están en la BD de trusted_zone
        existing_appids = {
            doc["appid"]
            for doc in self.trusted_client.juegos.find({}, {"appid": 1})
        }
        appids_nuevos = sorted(set(all_appids) - existing_appids)
        logging.info(f"🔎 {len(appids_nuevos)} appids nuevos; {len(existing_appids)} appids existentes")

         # Crear el NDJSON con la fecha en el nombre
        fecha_actual = date.today()
        fecha_formateada = fecha_actual.strftime("%Y_%m_%d")
        games_ndjson_path = os.path.join(self.lz_games_dir, f"steam_games_{fecha_formateada}.ndjson")

       # — guardar appids nuevos en el ndjson games_ndjson_path:

        # 0) cargo los appids que ya estaban en este NDJSON (si existe)
        processed = set()
        if os.path.exists(games_ndjson_path):
            with open(games_ndjson_path, "r", encoding="utf-8") as fg:
                for line in fg:
                    try:
                        rec = json.loads(line)
                        processed.add(rec.get("appid"))
                    except json.JSONDecodeError:
                        continue

        # 1) abro en modo append y sólo añado los que falten
        with open(games_ndjson_path, "a", encoding="utf-8") as fg:
            for appid in appids_nuevos:
                if appid in processed:
                    logging.info(f"≡ Saltando {appid}: ya está en {games_ndjson_path}")
                    continue

                logging.info(f"📦 Fetch details {appid}")
                details, err = self.get_game_details(appid)
                # Si detail es None y err=False, es que no hay data (beta, etc.); igualmente lo anoto
                record = {
                    "appid":   appid,
                    "details": details
                }
                fg.write(json.dumps(record, ensure_ascii=False) + "\n")

        logging.info(f"✔ Creado/actualizado NDJSON de juegos en {games_ndjson_path}")

        
        # 3) Reseñas: para *todos* los appids (incluyendo los que esta en o no estan la base de datos) a menos que este en modo MVP

        if self.appids_to_process_reviews is not None:
            all_appids=self.appids_to_process_reviews
            

        for appid in all_appids:
            # 3.1 determinar desde qué timestamp arrancar
            if appid in existing_appids:
                # saco el max timestamp ya grabado en trusted_zone.steam_reviews
                rec = self.trusted_client.reviews.find_one(
                    {"appid": appid},
                    sort=[("timestamp_created", -1)],
                    projection={"timestamp_created": 1}
                )
                since_ts = rec["timestamp_created"] if rec else 0
            else:
                # appid nuevo → bajar todo el histórico
                since_ts = 0

            logging.info(f"⭐ Fetch reviews {appid} desde ts={since_ts}")

            reviews, err = self.get_reviews_since_ts(appid, since_ts)
            if err:
                logging.warning(f"❌ Falló descarga reseñas {appid}")
                continue

            # 3.2 grabo en landing zone
            reviews_path = os.path.join(self.lz_games_dir, f"reviews_{appid}.ndjson")
            mode = "a" if os.path.exists(reviews_path) else "w"
            with open(reviews_path, mode, encoding="utf-8") as fr:
                for r in reviews:
                    # si quisieras, podrías añadir "appid" en cada r antes de grabar
                    fr.write(json.dumps(r, ensure_ascii=False) + "\n")

            logging.info(f"✔ Reseñas de {appid} en {reviews_path} ({len(reviews)} nuevas)")

        # 4) Extraer nombres y devolverlos al pipeline
        game_names = []
        with open(games_ndjson_path, encoding="utf-8") as fg:
            for line in fg:
                det = json.loads(line).get("details") or {}
                if det.get("name"):
                    game_names.append(det["name"])

        logging.info("🎮 Nombres de juegos procesados:")
        for n in game_names:
            logging.info(f"   • {n}")

        return game_names
