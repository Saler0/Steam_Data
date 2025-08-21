from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
from db.mongodb import MongoDBClient
import json
from collections import OrderedDict
from datetime import date

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

        # --- full refresh: embebo TODOS los DLC que apunten a winners y hago replace_one ---
        if self.is_full_refresh:
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
                           .withColumn("type", F.lit("game")))

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
        dlc_order = [
            "appid","type","name","required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score","fullgame_appid",
        ]
        def _col_or_null(dfcols, name): return F.col(name) if name in dfcols else F.lit(None).alias(name)

        dlcs_today = dlcs_raw  # ya está filtrado por updated_at en load_trusted
        dlc_struct_cols = [_col_or_null(dlcs_today.columns, c) for c in dlc_order]
        dlcs_to_attach = (dlcs_today
                          .withColumn("base_appid", F.col("fullgame_appid"))
                          .withColumn("dlc_doc", F.struct(*dlc_struct_cols))
                          .select("base_appid", "dlc_doc")
                          .dropDuplicates(["base_appid", "dlc_doc"]))

        # final_games SIN 'dlc' (solo setear campos del juego)
        final_games = (winners_full
                       .withColumn("type", F.lit("game"))
                       .withColumn("_id", F.col("appid")))

        desired_no_dlc = [
            "appid","type","name","required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score",
        ]
        for c in desired_no_dlc:
            if c not in final_games.columns:
                final_games = final_games.withColumn(c, F.lit(None))
        final_games = final_games.select(*desired_no_dlc, "_id")

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
            client = MongoClient(uri)
            coll = client[dbname][collname]
            for r in rows:
                rd = r.asDict(recursive=True)
                if is_full:
                    # replace completo (puede traer 'dlc' si fue construido en transform)
                    coll.replace_one({"appid": rd["appid"]}, rd, upsert=True)
                else:
                    # incremental: seteo campos sin tocar 'dlc'
                    payload = {k: rd.get(k) for k in set_fields if k in rd}
                    coll.update_one({"appid": rd["appid"]}, {"$set": payload}, upsert=True)
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

    # -------------------- RUN --------------------
    def run(self):
        # 1) decidir modo
        self.is_full_refresh = self._explo_is_empty()

        # 2) leer trusted (full o incremental)
        df_trusted = self.load_trusted()

        # 3) transformar (según modo)
        final_games, dlcs_to_attach = self.transform(df_trusted)

        # 4) escribir juegos
        final_games = final_games.coalesce(64)
        self.write_upsert_games(final_games)

        # 5) (solo incremental) adjuntar DLC nuevos sin pisar los existentes
        self.attach_dlcs_incremental(dlcs_to_attach)