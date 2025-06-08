import requests
import json
import time
import os
import glob

class ApiSteam:
    MAX_PER_PART = 5200

    def __init__(self):
        self.juegos_procesados = set()
        self.juegos_extraidos = []
        self.last_part_num = 1

    def get_all_games(self):
        url = "https://api.steampowered.com/ISteamApps/GetAppList/v2/"
        res = requests.get(url)
        return res.json()["applist"]["apps"]

    def get_game_details(self, appid):
        try:
            url = f"https://store.steampowered.com/api/appdetails?appids={appid}&cc=us&l=english"
            res = requests.get(url, timeout=10)
            raw_text = res.content.decode("utf-8-sig")
            raw = json.loads(raw_text)
            app_section = raw.get(str(appid))
            if not isinstance(app_section, dict) or app_section.get("success") is not True:
                return None, False
            return app_section.get("data"), False
        except Exception as e:
            print(f"❌ Error al obtener detalles para {appid}: {e}")
            return None, True

    def get_game_reviews(self, appid, num=100):
        try:
            url = f"https://store.steampowered.com/appreviews/{appid}"
            params = {
                "json": 1,
                "num_per_page": num,
                "filter": "recent",
                "language": "english"
            }
            res = requests.get(url, params=params, timeout=10)
            raw_text = res.content.decode("utf-8-sig")
            data = json.loads(raw_text)
            return data.get("reviews", []), False
        except Exception as e:
            print(f"❌ Error al obtener reseñas para {appid}: {e}")
            return [], True

    def find_existing_parts(self):
        parts = glob.glob("steam_games_data_part*.json")
        result = []
        for p in parts:
            name = os.path.basename(p)
            try:
                n = int(name.replace("steam_games_data_part", "").replace(".json", ""))
                result.append((n, p))
            except:
                pass
        return sorted(result, key=lambda x: x[0])

    def load_part(self, file):
        with open(file, "r", encoding="utf-8") as f:
            return json.load(f)

    def save_part(self):
        fname = f"steam_games_data_part{self.last_part_num}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(self.juegos_extraidos, f, indent=2, ensure_ascii=False)
        print(f"💾 Guardado parte {self.last_part_num}: {len(self.juegos_extraidos)} juegos → {fname}")

    def resume_or_start(self):
        parts = self.find_existing_parts()
        if parts:
            for _, file in parts:
                data = self.load_part(file)
                self.juegos_procesados.update(j["appid"] for j in data)
            self.last_part_num, last_file = parts[-1]
            last_data = self.load_part(last_file)
            if len(last_data) < self.MAX_PER_PART:
                self.juegos_extraidos = last_data
                print(f"🔄 Reanudando parte {self.last_part_num}, {len(self.juegos_extraidos)} juegos ya extraídos")
            else:
                self.last_part_num += 1
                self.juegos_extraidos = []
                print(f"✨ Todas las partes previas llenas, arrancando parte {self.last_part_num}")
        else:
            print("✨ Comenzando en parte 1")

    def run(self):
        self.resume_or_start()
        games = self.get_all_games()
        print(f"🎮 Total de juegos disponibles: {len(games)}")

        for i, game in enumerate(games):
            appid = game.get("appid")
            name = game.get("name", "").strip()

            if not name or appid in self.juegos_procesados:
                print(f"[{i+1}] ⏩ App sin nombre o ya procesado ({appid}), se omite.")
                continue

            print(f"\n[{i+1}] 📦 Procesando: {name} ({appid})")

            if len(self.juegos_extraidos) >= self.MAX_PER_PART:
                self.save_part()
                self.last_part_num += 1
                self.juegos_extraidos = []
                print(f"➡️ Pasando a parte {self.last_part_num}")

            details, err_d = self.get_game_details(appid)
            reviews, err_r = self.get_game_reviews(appid)

            juego_info = {
                "appid": appid,
                "name": name,
                "details": details if details else None,
                "error_details": bool(err_d),
                "reviews": reviews if reviews else [],
                "error_reviews": bool(err_r)
            }

            self.juegos_extraidos.append(juego_info)
            self.juegos_procesados.add(appid)

            if len(self.juegos_extraidos) % 100 == 0:
                self.save_part()
                print(f"💾 Guardado parcial dentro de parte {self.last_part_num}: {len(self.juegos_extraidos)} juegos")

            time.sleep(0.8)

        self.save_part()
        print("✅ Proceso completado.")
