import requests
import json
import time
import logging
from datetime import date
import os



class ApiSteam:

    def __init__(self, trusted_client,appids_to_process):

        self.trusted_client = trusted_client

        # Parámetros
        self.reviews_per_game =  100
        self.country_code     =  "us"

        # Paths de landing zone
        self.lz_games_dir   = os.path.join("landing_zone", "api_steam")
        os.makedirs(self.lz_games_dir, exist_ok=True)

        # appdi a los que se les buscara su review y news
        self.appids_to_process = appids_to_process

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
        
    def get_reviews_since_ts(self, appid, since_ts=0, start_cursor="*", stop_at_ts=True):
        """
        Descarga reviews orden: más recientes -> más antiguas.
        - start_cursor: permite reanudar histórico desde un cursor guardado (o "*" para empezar del último).
        - stop_at_ts=True: si encontramos una review con timestamp_created <= since_ts, cortamos (modo incremental).
        En histórico usar stop_at_ts=False (ignora since_ts y recorre hasta vaciar).
        Devuelve: (reviews, error, last_cursor_usado)
        """
        cursor = start_cursor or "*"
        last_cursor = cursor  # inicializar antes del try
        try:
            time.sleep(0.8)
            page = 1
            all_reviews = []

            while True:
                logging.info(f"🔍 Página {page} (cursor={cursor[:10]}…) appid={appid}")
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
                    # no hay más páginas
                    return all_reviews, False, last_cursor

                for r in reviews:
                    # en incremental podemos cortar cuando “ya llegamos” a lo que teníamos
                    if stop_at_ts and r.get("timestamp_created", 0) <= since_ts:
                        logging.info("⏹ Límite incremental alcanzado (timestamp <= since_ts).")
                        return all_reviews, False, last_cursor
                    all_reviews.append(r)

                # avanzar paginado
                cursor = data.get("cursor")
                last_cursor = cursor
                page += 1

        except Exception as e:
            logging.error(f"❌ Error al obtener reseñas para {appid}: {e}")
            return [], True, last_curso

    def get_new_for_appid(self, appid, ultimas_news):
        """Devuelve la lista de news para el appid. No pagina; solo las 'ultimas_news' que
        Steam permita devolver. Inserta 'appid' en cada item para trazabilidad."""
        try:
            time.sleep(1.3)
            url = (
                "https://api.steampowered.com/ISteamNews/GetNewsForApp/v2/"
                f"?appid={appid}&count={ultimas_news}"
            )
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            payload = res.json() or {}
            items = (payload.get("appnews", {}) or {}).get("newsitems", []) or []

            # Aseguramos que cada noticia lleva el appid
            for it in items:
                it["appid"] = appid

            logging.info(f"📰 {appid}: recuperadas {len(items)} news (solicitadas {ultimas_news})")
            return items, False

        except Exception as e:
            logging.error(f"❌ Error al obtener news para {appid}: {e}")
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


        if self.appids_to_process is not None:
            all_appids=self.appids_to_process

        # 3) Reseñas: para *todos* los appids (incluyendo los que están o no en la BD) a menos que este en modo MVP
        #    Guardar TODO en UN NDJSON unificado del día: steam_reviews_{fecha_formateada}.ndjson
        reviews_ndjson_path = os.path.join(self.lz_games_dir, f"steam_reviews_{fecha_formateada}.ndjson")

        # --- (A) Cargar / construir checkpoint del día ---
        ckpt_dir = os.path.join(self.lz_games_dir, "checkpoints")
        os.makedirs(ckpt_dir, exist_ok=True)
        ckpt_path = os.path.join(ckpt_dir, f"reviews_progress_{fecha_formateada}.json")

        checkpoint = {}
        if os.path.exists(ckpt_path):
            try:
                with open(ckpt_path, "r", encoding="utf-8") as fck:
                    checkpoint = json.load(fck) or {}
            except Exception:
                logging.warning("⚠️ No pude leer checkpoint; continuaré sin él.")
                checkpoint = {}

        # --- (B) Extra opcional: si se quiere usar lo ya escrito en el NDJSON del día como fuente de max_ts por appid ---
        daily_written_max_ts = {}
        if os.path.exists(reviews_ndjson_path) and os.path.getsize(reviews_ndjson_path) > 0:
            try:
                with open(reviews_ndjson_path, "r", encoding="utf-8") as fr:
                    for line in fr:
                        try:
                            r = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        aid = r.get("appid")
                        ts  = r.get("timestamp_created", 0)
                        if aid is not None and isinstance(ts, int):
                            prev = daily_written_max_ts.get(aid, 0)
                            if ts > prev:
                                daily_written_max_ts[aid] = ts
            except Exception as e:
                logging.warning(f"⚠️ No pude escanear {reviews_ndjson_path}: {e}")


        # --- (C) Abrimos una sola vez en append (se crea si no existe) ---
        with open(reviews_ndjson_path, "a", encoding="utf-8") as fr_all:
            for appid in all_appids:
                # ¿Ya existe checkpoint para este appid hoy?
                ck = checkpoint.get(str(appid), {})

                # ¿Hay reviews en trusted para decidir el modo?
                has_trusted = self.trusted_client.reviews.count_documents({"appid": appid}, limit=1) > 0

                # Si checkpoint existe, respetar su modo; si no, decidir por trusted
                mode = ck.get("mode") or ("incremental" if has_trusted else "historical")

                if mode == "incremental":
                    # Base ts de tres fuentes: trusted / checkpoint / ndjson del día (opcional)
                    ts_trusted = 0
                    if has_trusted:
                        rec = self.trusted_client.reviews.find_one(
                            {"appid": appid},
                            sort=[("timestamp_created", -1)],
                            projection={"timestamp_created": 1}
                        )
                        ts_trusted = rec["timestamp_created"] if rec else 0

                    ts_ckpt = int(ck.get("max_ts", 0)) if ck else 0
                    ts_daily = int(daily_written_max_ts.get(appid, 0))

                    since_ts = max(ts_trusted, ts_ckpt, ts_daily)

                    logging.info(f"⭐ [INC] appid={appid} desde ts={since_ts} (trusted={ts_trusted}, ckpt={ts_ckpt}, daily={ts_daily})")
                    reviews, err, last_cursor = self.get_reviews_since_ts(
                        appid, since_ts=since_ts, start_cursor="*", stop_at_ts=True
                    )
                    if err:
                        logging.warning(f"❌ Falló descarga reseñas (INC) {appid}")
                        continue

                    escritos = 0
                    local_max_ts = since_ts
                    for r in reviews:
                        r["appid"] = appid
                        fr_all.write(json.dumps(r, ensure_ascii=False) + "\n")
                        escritos += 1
                        tsc = r.get("timestamp_created", 0)
                        if isinstance(tsc, int) and tsc > local_max_ts:
                            local_max_ts = tsc

                    logging.info(f"✔ [INC] {appid}: {escritos} reviews nuevas")

                    # actualizar checkpoint (sin cursor, pero con max_ts)
                    ck_new = {
                        "mode": "incremental",
                        "max_ts": local_max_ts,
                        "done": True  # el incremental es por “ventana”; marcamos done para hoy
                    }
                    checkpoint[str(appid)] = ck_new
                    try:
                        with open(ckpt_path, "w", encoding="utf-8") as fck:
                            json.dump(checkpoint, fck, ensure_ascii=False)
                    except Exception as e:
                        logging.warning(f"⚠️ No pude escribir checkpoint INC para {appid}: {e}")

                else:
                    # HISTÓRICO (no cortar por ts; reanudar por cursor si lo hay)
                    start_cursor = ck.get("cursor", "*")
                    already_done = ck.get("done", False)
                    if already_done:
                        logging.info(f"↪️ [HIST] {appid} ya marcado como done; skip.")
                        continue

                    logging.info(f"⭐ [HIST] appid={appid} desde cursor={start_cursor[:10]}...")
                    # NOTA: since_ts se ignora porque stop_at_ts=False
                    reviews, err, last_cursor = self.get_reviews_since_ts(
                        appid, since_ts=0, start_cursor=start_cursor, stop_at_ts=False
                    )
                    if err:
                        logging.warning(f"❌ Falló descarga reseñas (HIST) {appid}; guardo cursor para retomar.")
                        # Guardar cursor por si nos caímos en medio de una página
                        ck_new = {
                            "mode": "historical",
                            "cursor": last_cursor or start_cursor,
                            "max_ts": int(ck.get("max_ts", 0)),
                            "done": False
                        }
                        checkpoint[str(appid)] = ck_new
                        try:
                            with open(ckpt_path, "w", encoding="utf-8") as fck:
                                json.dump(checkpoint, fck, ensure_ascii=False)
                        except Exception as e2:
                            logging.warning(f"⚠️ No pude escribir checkpoint HIST (error) {appid}: {e2}")
                        continue

                    escritos = 0
                    local_max_ts = int(ck.get("max_ts", 0))
                    for r in reviews:
                        r["appid"] = appid
                        fr_all.write(json.dumps(r, ensure_ascii=False) + "\n")
                        escritos += 1
                        tsc = r.get("timestamp_created", 0)
                        if isinstance(tsc, int) and tsc > local_max_ts:
                            local_max_ts = tsc

                    logging.info(f"✔ [HIST] {appid}: {escritos} reviews nuevas")

                    # Si no llegaron reviews, es muy probable que ya no haya más páginas -> marcamos done
                    # (La función retorna cuando la API devuelve lista vacía)
                    is_done = (len(reviews) == 0)

                    ck_new = {
                        "mode": "historical",
                        "cursor": last_cursor or start_cursor,
                        "max_ts": local_max_ts,
                        "done": is_done
                    }
                    checkpoint[str(appid)] = ck_new
                    try:
                        with open(ckpt_path, "w", encoding="utf-8") as fck:
                            json.dump(checkpoint, fck, ensure_ascii=False)
                    except Exception as e:
                        logging.warning(f"⚠️ No pude escribir checkpoint HIST para {appid}: {e}")



        # 4) News: para *todos* los appids (incluyendo los que esta en o no estan la base de datos) a menos que este en modo MVP
        #    - Si NO hay ninguna noticia guardada en Mongo para ese appid -> pedir 9999 (histórico inicial)
        #    - Si SÍ hay al menos una -> pedir 1000 (incremental)
        news_ndjson_path = os.path.join(self.lz_games_dir, f"steam_news_{fecha_formateada}.ndjson")
        # abrimos en append, se creará si no existe
        with open(news_ndjson_path, "w", encoding="utf-8") as fn:
            for appid in all_appids:
                try:
                    # ¿Hay al menos una noticia guardada para este appid?
                    # Nota: la colección trusted_zone.news_games tiene índice único en 'gid'.
                    #       No pasa nada por consultar por 'appid' aunque no tenga índice.
                    ya_hay_news = self.trusted_client.news_games.count_documents(
                        {"appid": appid}, limit=1
                    ) > 0

                    ultimas_news = 1000 if ya_hay_news else 9999
                    logging.info(
                        f"🗞️ Fetch news {appid}: {'incremental 1000' if ya_hay_news else 'histórico 9999'}"
                    )

                    news_items, err = self.get_new_for_appid(appid, ultimas_news)
                    if err:
                        logging.warning(f"⚠️ Falló descarga de news para {appid}")
                        continue

                    # Guardar cada item en el NDJSON unificado del día
                    escritos = 0
                    for it in news_items:
                        # Por si acaso añadimos 'appid' (ya lo agrega get_new_for_appid)
                        if "appid" not in it:
                            it["appid"] = appid
                        fn.write(json.dumps(it, ensure_ascii=False) + "\n")
                        escritos += 1

                    logging.info(f"✔ News de {appid} escritas en {news_ndjson_path} ({escritos})")


                except Exception as e:
                    logging.error(f"❌ Error procesando news de {appid}: {e}")
    

        # 5) Extraer nombres nuevos procesados de appids (que no estaban en la base de datos) y devolverlos al pipeline
        game_names = []
        with open(games_ndjson_path, encoding="utf-8") as fg:
            for line in fg:
                det = json.loads(line).get("details") or {}
                if det.get("name"):
                    game_names.append(det["name"])

        # logging.info("🎮 Nombres de juegos procesados:")
        # for n in game_names:
        #     logging.info(f"   • {n}")
        return game_names
