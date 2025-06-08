import requests
import json
import time
import logging
from datetime import datetime
import pymongo
from pymongo import MongoClient


class ApiSteam:

    def __init__(self, appids_to_process):
        self.appids = appids_to_process
        self.reviews_per_game =  100
        self.country_code     =  "us"

        # Conexión a MongoDB
        client = MongoClient("mongodb://localhost:27017")
        db = client["juegos_steam"]
        self.collection_juegos        = db["juegos_steam"]
        self.reviews_collection = db["steam_reviews"]
        self.log_collection    = db["import_log"]

        self.collection_juegos.create_index("appid", unique=True)
        self.reviews_collection.create_index(
            [("appid", 1), ("review.recommendationid", 1)],
            unique=True,
            sparse=True
        )

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
        
    def get_reviews_since_ts(self, appid, since_ts):
        try:
            cursor = "*"
            page = 1
            new_reviews = []

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

                for rev in reviews:
                    ts = rev.get("timestamp_created", 0)
                    # si esta reseña ya era vieja, detenemos TODO
                    if ts <= since_ts:
                        logging.info("⏹ Reseña antigua detectada; paro descarga.")
                        return new_reviews, False
                    new_reviews.append(rev)

                cursor = data.get("cursor")
                page += 1
                time.sleep(0.8)  # respetar rate-limit

            return new_reviews, False
        
        except Exception as e:
            logging.error(f"❌ Error al obtener detalles para {appid}: {e}")
            return [], True


    def run(self):

        for appid in self.appids:
            logging.info(f"Procesando appid {appid}…")
            
            # ——————————————————————————————————————
            # 1) Detalles: solo fetch/insert cuando no haya en Mongo
            # ——————————————————————————————————————
            existing_game = self.collection_juegos.find_one({"appid": appid})
            if not existing_game:
                # no había, vamos a la API
                details, err_d = self.get_game_details(appid)

                # guardamos JSON local
                filename = f"steam_game_{appid}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump({
                        "appid": appid,
                        "details": details,
                        "error_details": err_d
                    }, f, indent=2, ensure_ascii=False)
                logging.info(f"💾 Guardado fichero {filename}")

                # insertamos en Mongo
                self.collection_juegos.insert_one({
                    "appid":         appid,
                    "details":       details,
                    "error_details": err_d,
                    "fetched_at":    datetime.utcnow()
                })
                logging.info(f"✔ Insertado appid {appid} en steam_data")
            else:
                # ya existían
                err_d = existing_game.get("error_details", False)
                logging.info(f"ℹ Detalles de appid {appid} ya en cache; omito API.")

            # ——————————————————————————————————————
            # 2) Reseñas: fetch incremental aunque el juego ya estuviera
            # ——————————————————————————————————————
            # obtenemos el timestamp máximo que ya tenemos
            last = self.reviews_collection.find_one(
                {"appid": appid},
                sort=[("review.timestamp_created", pymongo.DESCENDING)],
                projection={"review.timestamp_created": 1}
            )
            since_ts = last["review"]["timestamp_created"] if last else 0

            # llamamos al método incremental
            new_revs, err_r = self.get_reviews_since_ts(appid, since_ts)

            # insertamos en bloque (ordered=False salta duplicados)
            inserted_rev = 0
            if new_revs:
                docs = [{"appid": appid, "review": rev} for rev in new_revs]
                try:
                    result = self.reviews_collection.insert_many(docs, ordered=False)
                    inserted_rev = len(result.inserted_ids)
                except pymongo.errors.BulkWriteError as bwe:
                    # contamos cuántos entraron de verdad (descartar 11000)
                    errors = bwe.details.get("writeErrors", [])
                    inserted_rev = len(docs) - sum(1 for e in errors if e["code"] == 11000)
            logging.info(f"✔ Insertadas {inserted_rev} reseñas nuevas de {appid}")

            # ——————————————————————————————————————
            # 3) Log
            # ——————————————————————————————————————
            self.log_collection.insert_one({
                "appid":            appid,
                "details_cached":   bool(existing_game),
                "new_reviews":      inserted_rev,
                "error_details":    err_d,
                "error_reviews":    err_r,
                "timestamp":        datetime.utcnow()
            })

            # rate-limit
            time.sleep(0.8)

        logging.info("✅ Todos los juegos procesados.")

