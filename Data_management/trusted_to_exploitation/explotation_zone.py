from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
from db.mongodb import MongoDBClient
import json
from collections import OrderedDict
from datetime import date
from pymongo.errors import DuplicateKeyError
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, LongType

class TrustedToExploitation:
    """
    Mueve juegos desde trusted_zone a explotation_zone:
      - Solo type in {'game','dlc'} y name no vacío
      - Dedup por (name,type) usando las referencias de DLC->fullgame para los 'game'
      - Produce SOLO docs type='game' con campo 'dlc' construido desde DLC reales
      - Upsert por appid en explotation_zone.juegos_steam
    """

    def __init__(self, spark: SparkSession,trusted_client: MongoDBClient,explo_client: MongoDBClient):

        self.spark = spark
        self.trusted = trusted_client
        self.explo = explo_client

        self.trusted_coll_uri = f"{self.trusted.uri}/{self.trusted.db_name}.juegos_steam"
        self.explo_coll_name = "juegos_steam"  # destino
                
        self.trusted_news_uri = f"{self.trusted.uri}/{self.trusted.db_name}.news_games"
        self.explo_news_coll  = "news_games"

        self.today_str = date.today().strftime("%Y-%m-%d")

        self.is_full_refresh = False


    def _explo_is_empty(self) -> bool:
        """True si la colección no existe o tiene 0 documentos."""
        client = MongoClient(self.explo.uri)
        db = client[self.explo.db_name]
        exists = self.explo_coll_name in db.list_collection_names()
        if not exists:
            client.close()
            return True
        count = db[self.explo_coll_name].estimated_document_count()
        client.close()
        return count == 0

    # -------------------- LOAD --------------------
    def load_trusted(self) -> DataFrame:
        """
        Carga desde trusted_zone:
        - full refresh: sin filtro de fecha
        - incremental: filtra updated_at == hoy
        """
        match_stage = {
            "name": {"$nin": ["", None]},
            "type": {"$in": ["game", "dlc"]},
        }
        if not self.is_full_refresh:
            match_stage["updated_at"] = self.today_str

        pipeline = [
            {"$match": match_stage},
            {"$addFields": {
                "appid": {"$toInt": "$appid"},
                "fullgame.appid": {
                    "$cond": [{"$ne": ["$fullgame.appid", None]}, {"$toInt": "$fullgame.appid"}, None]
                },
                "dlc": {"$ifNull": ["$dlc", []]},
                "categories": {"$ifNull": ["$categories", []]},
                "genres": {"$ifNull": ["$genres", []]},
                "developers": {"$ifNull": ["$developers", []]},
                "publishers": {"$ifNull": ["$publishers", []]},
                "price_overview.initial": {
                    "$cond": [{"$eq": ["$price_overview.initial", None]}, "", {"$toString": "$price_overview.initial"}]
                },
                "price_overview.discount_percent": {
                    "$cond": [{"$eq": ["$price_overview.discount_percent", None]}, "", {"$toString": "$price_overview.discount_percent"}]
                },
                "price_overview.final_formatted": {
                    "$cond": [{"$eq": ["$price_overview.final_formatted", None]}, "", {"$toString": "$price_overview.final_formatted"}]
                },
                "price_overview.final": {
                    "$cond": [{"$eq": ["$price_overview.final", None]}, "", {"$toString": "$price_overview.final"}]
                },
                "price_overview.recurring_sub_desc": {
                    "$cond": [{"$eq": ["$price_overview.recurring_sub_desc", None]}, "", {"$toString": "$price_overview.recurring_sub_desc"}]
                },
                "price_overview.currency": {
                    "$cond": [{"$eq": ["$price_overview.currency", None]}, "", {"$toString": "$price_overview.currency"}]
                },
                "price_overview.initial_formatted": {
                    "$cond": [{"$eq": ["$price_overview.initial_formatted", None]}, "", {"$toString": "$price_overview.initial_formatted"}]
                },
                "price_overview.recurring_sub": {
                    "$cond": [{"$eq": ["$price_overview.recurring_sub", None]}, "", {"$toString": "$price_overview.recurring_sub"}]
                },
            }},
        ]

        return (
            self.spark.read
            .format("mongo")
            .option("uri", self.trusted_coll_uri)
            .option("pipeline", json.dumps(pipeline))
            .option("inferSchema.sampleSize", "200000")
            .load()
        )
    


    # -------------------- TRANSFORM --------------------
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Devuelve:
          - final_games: DataFrame de 'game' deduplicados (si full refresh con 'dlc' embebido; si incremental sin 'dlc')
          - dlcs_to_attach: DataFrame [base_appid, dlc_doc] para $addToSet (solo incremental)
        """
        # --- full refresh: embebo TODOS los DLC que apunten a winners y hago replace_one ---
        if self.is_full_refresh:

            # ===== 1) elegir winner por nombre (solo games) =====
            games_min = (
                df.where(F.col("type") == "game")
                .withColumn("appid", F.col("appid").cast("int"))
                .withColumn("nname",
                            F.lower(
                                F.regexp_replace(
                                    F.regexp_replace(F.trim(F.col("name")), r"[™®]", ""),
                                    r"\s+", " "
                                )))
                .withColumn("rec_tot_norm", F.coalesce(F.col("recommendations_total").cast("long"), F.lit(0)))
                .withColumn("dlc_len", F.size(F.coalesce(F.col("dlc"), F.array())))
                .withColumn("has_dlc_list", (F.col("dlc_len") > 0).cast("int"))
                .select("appid", "name", "nname", "rec_tot_norm", "has_dlc_list")
            )

            dlc_refs_min = (
                df.where(F.col("type") == "dlc")
                .select(
                    F.col("appid").cast("int").alias("dlc_appid"),
                    F.col("fullgame.appid").cast("int").alias("fullgame_appid")
                )
                .where(F.col("fullgame_appid").isNotNull())
                .dropDuplicates(["dlc_appid", "fullgame_appid"])
            )

            refs_per_game = dlc_refs_min.groupBy("fullgame_appid").agg(F.countDistinct("dlc_appid").alias("dlc_ref_count"))

            ge = (games_min.alias("g")
                .join(refs_per_game.alias("r"), F.col("g.appid") == F.col("r.fullgame_appid"), "left")
                .drop("fullgame_appid")
                .withColumn("dlc_ref_count", F.coalesce(F.col("dlc_ref_count"), F.lit(0)))
                .withColumn("is_referenced", (F.col("dlc_ref_count") > 0).cast("int")))

            ge = ge.repartition(64, "nname")
            w = Window.partitionBy("nname").orderBy(
                F.desc("is_referenced"),
                F.desc("has_dlc_list"),
                F.desc("dlc_ref_count"),
                F.desc("rec_tot_norm"),
                F.desc("appid"),
            )
            winners_min = (ge.withColumn("rn", F.row_number().over(w))
                            .where(F.col("rn") == 1)
                            .select("appid", "name", "nname"))

            games_full = df.where(F.col("type") == "game").withColumn("appid", F.col("appid").cast("int"))
            winners_full = (winners_min.alias("w")
                            .join(games_full.alias("gf"), F.col("w.appid") == F.col("gf.appid"), "left")
                            .select("gf.*"))
            winners_full = winners_full.drop("dlc")

            # ===== 2) DLCs =====
            winners_keys = winners_min.select(F.col("appid").alias("base_appid"))
            dlcs_raw = (
                df.where(F.col("type") == "dlc")
                .withColumn("appid", F.col("appid").cast("int"))
                .withColumn("fullgame_appid", F.col("fullgame.appid").cast("int"))
                .where(F.col("fullgame_appid").isNotNull())
            )


            dlc_order = [
                "appid","type","name","required_age","is_free","controller_support",
                "detailed_description","about_the_game","short_description",
                "supported_languages","legal_notice",
                "pc_requirements","mac_requirements","linux_requirements",
                "developers","publishers","platforms","categories","genres",
                "release_date","recommendations_total","metacritic_score","fullgame_appid",
            ]
            def _col_or_null(dfcols, name): return F.col(name) if name in dfcols else F.lit(None).alias(name)

            dlcs_filtered = (dlcs_raw.join(F.broadcast(winners_keys),
                                           F.col("fullgame_appid") == F.col("base_appid"), "inner")
                                       .dropDuplicates(["base_appid", "appid"]))
            dlc_struct_cols = [_col_or_null(dlcs_filtered.columns, c) for c in dlc_order]
            dlcs_embedded = (dlcs_filtered
                             .withColumn("dlc_doc", F.struct(*dlc_struct_cols))
                             .repartition(128, "base_appid")
                             .groupBy("base_appid")
                             .agg(F.collect_list("dlc_doc").alias("dlc")))

            final_games = (winners_full.alias("wf")
                           .join(dlcs_embedded, F.col("wf.appid") == F.col("base_appid"), "left")
                           .drop("base_appid")
                           .withColumn("dlc", F.coalesce(F.col("dlc"), F.array()))
            )

            # _id = appid
            if "_id" in final_games.columns:
                final_games = final_games.drop("_id")
            final_games = final_games.withColumn("_id", F.col("appid"))

            desired = [
                "appid","type","name","required_age","is_free","controller_support",
                "detailed_description","about_the_game","short_description",
                "supported_languages","legal_notice",
                "pc_requirements","mac_requirements","linux_requirements",
                "developers","publishers","platforms","categories","genres",
                "release_date","recommendations_total","metacritic_score","dlc",
            ]
            for c in desired:
                if c not in final_games.columns:
                    final_games = final_games.withColumn(c, F.lit(None))
            final_games = final_games.select(*desired, "_id")

            dlcs_to_attach = self.spark.createDataFrame([], schema="base_appid INT, dlc_doc STRUCT<dummy:INT>")
            return final_games, dlcs_to_attach

        # --- incremental: NO embebemos 'dlc' (evita pisar). Adjuntamos todos los DLC de hoy por $addToSet ---
        # construir dlc_doc (mismo orden que arriba)
        games_today = (
            df.where(F.col("type") == "game")
            .withColumn("appid", F.col("appid").cast("int"))
            .withColumn("type", F.lit("game"))
            .withColumn("_id", F.col("appid"))
        )

        desired_no_dlc = [
            "appid","type","name","required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score",
        ]
        for c in desired_no_dlc:
            if c not in games_today.columns:
                games_today = games_today.withColumn(c, F.lit(None))
        final_games = games_today.select(*desired_no_dlc, "_id")

        # DLC de hoy → attach al juego base por fullgame.appid
        dlc_order = [
            "appid","type","name","required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score","fullgame_appid",
        ]
        dlcs_today = (
            df.where(F.col("type") == "dlc")
            .withColumn("appid", F.col("appid").cast("int"))
            .withColumn("fullgame_appid", F.col("fullgame.appid").cast("int"))
            .where(F.col("fullgame_appid").isNotNull())
        )
        def _col_or_null(dfcols, name): 
            return F.col(name) if name in dfcols else F.lit(None).alias(name)

        dlc_struct_cols = [_col_or_null(dlcs_today.columns, c) for c in dlc_order]
        dlcs_to_attach = (dlcs_today
                        .withColumn("base_appid", F.col("fullgame_appid"))
                        .withColumn("dlc_doc", F.struct(*dlc_struct_cols))
                        .select("base_appid", "dlc_doc")
                        .dropDuplicates(["base_appid", "dlc_doc"]))

        return final_games, dlcs_to_attach
    
    # -------------------- WRITE (UPSERT) --------------------
    def write_upsert_games(self, df_games: DataFrame):
        """
        - full refresh: replace_one con documento completo (incluye 'dlc').
        - incremental: $set de campos del juego (SIN 'dlc') para no pisar la lista existente.
        """
        uri      = self.explo.uri
        dbname   = self.explo.db_name
        collname = self.explo_coll_name
        is_full  = self.is_full_refresh
        set_fields = [
            "appid","type","name","required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score",
        ]

        def _write(rows):
            client = MongoClient(uri); coll = client[dbname][collname]
            for r in rows:
                rd = r.asDict(recursive=True)
                if is_full:
                    # full refresh: documento completo (puede traer 'dlc')
                    coll.replace_one({"appid": rd["appid"]}, rd, upsert=True)
                    continue

                # incremental: nunca tocar el array dlc aquí
                payload = {k: rd.get(k) for k in set_fields if k in rd}

                # 1) intenta actualizar por appid (sin upsert)
                res = coll.update_one({"appid": rd["appid"]}, {"$set": payload}, upsert=False)
                if res.matched_count > 0:
                    continue

                # 2) intenta actualizar por (name,type) (sin upsert) para no chocar con índice único
                res2 = coll.update_one({"name": rd.get("name"), "type": rd.get("type")}, {"$set": payload}, upsert=False)
                if res2.matched_count > 0:
                    # opcional: si quieres asegurar que appid quede correcto cuando coincida por nombre/type:
                    # coll.update_one({"name": rd["name"], "type": rd["type"]}, {"$set": {"appid": rd["appid"]}})
                    continue

                # 3) no existe ni por appid ni por (name,type) → inserta nuevo (sin tocar 'dlc')
                try:
                    # Inserta doc mínimo con appid y payload
                    doc = {"appid": rd["appid"], "dlc": [], **payload}
                    coll.insert_one(doc)
                except DuplicateKeyError:
                    # Rara condición de carrera: alguien insertó (name,type) mientras tanto.
                    # En ese caso, actualiza por (name,type).
                    coll.update_one({"name": rd.get("name"), "type": rd.get("type")}, {"$set": payload}, upsert=False)

            client.close()

        df_games.foreachPartition(_write)

    def attach_dlcs_incremental(self, df_dlcs) -> None:
        """
        Para cada (base_appid, dlc_doc) hace:
          update_one({appid: base_appid}, {$addToSet: {dlc: dlc_doc}}, upsert=False)
        """
        if self.is_full_refresh or df_dlcs.rdd.isEmpty():
            return

        uri, dbname, collname = self.explo.uri, self.explo.db_name, self.explo_coll_name

        def _attach(rows):
            client = MongoClient(uri); coll = client[dbname][collname]
            for r in rows:
                rd = r.asDict(recursive=True)
                base = rd["base_appid"]
                dlc_doc = rd["dlc_doc"]
                # añade solo si ya existe el juego base (no creamos docs huérfanos)
                coll.update_one({"appid": base}, {"$addToSet": {"dlc": dlc_doc}}, upsert=False)
            client.close()

        df_dlcs.foreachPartition(_attach)


    def _explo_news_is_empty(self) -> bool:
        client = MongoClient(self.explo.uri)
        db = client[self.explo.db_name]
        exists = self.explo_news_coll in db.list_collection_names()
        if not exists:
            client.close()
            return True
        count = db[self.explo_news_coll].estimated_document_count()
        client.close()
        return count == 0

    def _read_trusted_news(self, only_today: bool):
        match_stage = {}
        if only_today:
            match_stage["updated_at"] = self.today_str  # 'YYYY-MM-DD'

        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})

        # Seleccionamos sólo las columnas pedidas
        pipeline.append({"$project": {
            "_id": 0,
            "gid": {"$toString": "$gid"},   # forzamos string por seguridad
            "title": 1,
            "contents": 1,
            "date": 1,
            "appid": 1,
            "updated_at": 1
        }})

        return (
            self.spark.read
                .format("mongo")
                .option("uri", self.trusted_news_uri)
                .option("pipeline", json.dumps(pipeline))
                .load()
        )

    def _write_news_only_new(self, df_news):
        """
        Inserta en explotation_zone.news_games sólo los gid que no existan.
        """
        if df_news.rdd.isEmpty():
            return

        # Traemos gids existentes del destino para anti-join
        client = MongoClient(self.explo.uri)
        existing_gids = list(client[self.explo.db_name][self.explo_news_coll].distinct("gid"))
        client.close()

        if existing_gids:
            existing_df = self.spark.createDataFrame([(g,) for g in existing_gids], ["gid"])
            df_to_insert = df_news.dropDuplicates(["gid"]).join(existing_df, on="gid", how="left_anti")
        else:
            df_to_insert = df_news.dropDuplicates(["gid"])

        new_cnt = df_to_insert.count()
        if new_cnt == 0:
            return

        (df_to_insert
            .select("gid","title","contents","date","appid","updated_at")
            .write
            .format("mongo")
            .mode("append")
            .option("uri", self.explo.uri)
            .option("database", self.explo.db_name)
            .option("collection", self.explo_news_coll)
            .save())


    def run_news(self):
        """
        - Si explotation_zone.news_games está vacía -> full refresh (todo trusted).
        - Si no, sólo registros de hoy (updated_at == hoy) desde trusted.
        - Siempre inserta sólo gid nuevos en destino.
        """
        is_dest_empty = self._explo_news_is_empty()
        df_trusted_news = self._read_trusted_news(only_today=not is_dest_empty)

        df_trusted_news = (
            df_trusted_news
            .withColumn("gid", F.col("gid").cast(StringType()))
            .withColumn("title", F.col("title").cast(StringType()))
            .withColumn("contents", F.col("contents").cast(StringType()))
            .withColumn("date", F.col("date").cast(StringType()))
            .withColumn("appid", F.col("appid").cast(IntegerType()))
            .withColumn("updated_at", F.col("updated_at").cast(StringType()))
        )

        total_read = df_trusted_news.count()
        mode_str = "FULL REFRESH" if is_dest_empty else f"INCREMENTAL (updated_at={self.today_str})"
        print(f"📰 News trusted leídas: {total_read} | modo: {mode_str}")

        self._write_news_only_new(df_trusted_news)
        print("✅ News movidas a explotation_zone.news_games (solo nuevas por gid).")

    # -------------------- RUN --------------------
    def run(self):
        # 1) decidir modo (para juegos)
        self.is_full_refresh = self._explo_is_empty()

        # 2) leer trusted (full o incremental)  juegos
        df_trusted = self.load_trusted()
        if df_trusted.rdd.isEmpty():
            print("ℹ️ No hay juegos que mover (filtro updated_at=today no produjo filas). Se continúa con NEWS.")
        else:
            final_games, dlcs_to_attach = self.transform(df_trusted) # transformar (según modo)
            final_games = final_games.coalesce(64) # escribir juegos
            self.write_upsert_games(final_games)
            self.attach_dlcs_incremental(dlcs_to_attach) # (solo incremental) adjuntar DLC nuevos sin pisar los existentes

        # 3) mover NEWS
        self.run_news()