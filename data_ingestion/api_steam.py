import requests
import json
import time
import logging
from datetime import datetime
from pymongo import MongoClient

class ApiSteam:

    def __init__(self, appids_to_process):
        self.appids = appids_to_process
        self.reviews_per_game =  100
        self.country_code     =  "us"

        # Conexión a MongoDB
        client = MongoClient("mongodb://localhost:27017")
        db = client["steam_data"]
        self.collection_juegos        = db["steam_data"]
        self.reviews_collection = db["steam_reviews"]
        self.log_collection    = db["import_log"]

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
        
    def get_all_game_reviews(self, appid):
        try:
            all_reviews = []
            cursor = "*"   # primera llamada
            while True:
                params = {
                    "json": 1,
                    "num_per_page": self.reviews_per_game,  # hasta 100
                    "filter": "recent",                     # o "updated"
                    "language": "english",
                    "cursor": cursor
                }
                res = requests.get(f"https://store.steampowered.com/appreviews/{appid}", 
                                params=params, timeout=10)
                res.raise_for_status()
                page = res.json()
                reviews = page.get("reviews", [])
                if not reviews:
                    break
                all_reviews.extend(reviews)
                cursor = page.get("cursor")  # siguiente página
                time.sleep(0.8)              # para respetar rate limits

            return all_reviews, False
        
        except Exception as e:
            logging.error(f"❌ Error al obtener detalles para {appid}: {e}")
            return [], True


    def run(self):

        for appid in self.appids:
            logging.info(f"Procesando appid {appid}…")
            
            # 1) Obtener detalles
            details, err_d = self.get_game_details(appid)

    


            # 2) Guardar JSON individual de detalles
            filename = f"steam_game_{appid}.json"
            juego_info = { "appid": appid,
                           "details": details, 
                           "error_details": err_d }
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(juego_info, f, indent=2, ensure_ascii=False)
            logging.info(f"💾 Guardado fichero {filename}")


            # 3) Insertar en colección steam_data
            exists_main = self.collection_juegos.find_one({"appid": appid}) is not None
            if not exists_main:
                self.collection_juegos.insert_one(juego_info)
                inserted_main = True
                logging.info(f"✔ Insertado appid {appid} en steam_data")
            else:
                inserted_main = False
                logging.info(f"⚠ appid {appid} ya existía en steam_data; se omite")

            # 4) Obtener todas las reseñas
            reviews, err_r = self.get_all_game_reviews(appid)

            # 5) Insertar cada reseña como documento independiente
            inserted_rev = 0
            for rev in reviews:
                rev_doc = {
                    "appid":       appid,
                    "review":      rev,
                }

                rec_id = rev.get("recommendationid")
                if rec_id and not self.reviews_collection.find_one({
                    "appid": appid, "review.recommendationid": rec_id
                }):
                    self.reviews_collection.insert_one(rev_doc)
                    inserted_rev += 1

            logging.info(f"✔ Insertadas {inserted_rev}/{len(reviews)} reseñas en steam_reviews")

            # 6) Registrar en import_log
            log_entry = {
                "appid":            appid,
                "inserted_main":    inserted_main,
                "inserted_reviews": inserted_rev,
                "total_reviews":    len(reviews),
                "error_details":    err_d,
                "error_reviews":    err_r,
                "timestamp":        datetime.utcnow(),
                "file":             filename
            }
            self.log_collection.insert_one(log_entry)


            # 7) Pausa para rate-limit
            time.sleep(0.8)

        logging.info("✅ Todos los juegos procesados.")

