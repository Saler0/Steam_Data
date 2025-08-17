from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
from db.mongodb import MongoDBClient
import json
from collections import OrderedDict

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

    # -------------------- LOAD --------------------
    def load_trusted(self) -> DataFrame:
      # Normalizamos tipos problemáticos ANTES de que Spark infiera el esquema
      pipeline = [
          {
              "$match": {
                  "name": {"$nin": ["", None]},
                  "type": {"$in": ["game", "dlc"]},
              }
          },
          # Normalizaciones de tipo:
          {
              "$addFields": {
                  # appid siempre int
                  "appid": {"$toInt": "$appid"},
                  # fullgame.appid siempre int si existe
                  "fullgame.appid": {
                      "$cond": [
                          {"$ne": ["$fullgame.appid", None]},
                          {"$toInt": "$fullgame.appid"},
                          None
                      ]
                  },
                  # Asegurar arrays vacíos donde aplica
                  "dlc": {"$ifNull": ["$dlc", []]},
                  "categories": {"$ifNull": ["$categories", []]},
                  "genres": {"$ifNull": ["$genres", []]},
                  "developers": {"$ifNull": ["$developers", []]},
                  "publishers": {"$ifNull": ["$publishers", []]},
                  # price_overview: forzar todo a string (o "" si null) para evitar NullType
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
              }
          },
      ]

      return (
          self.spark.read
          .format("mongo")
          .option("uri", self.trusted_coll_uri)
          .option("pipeline", json.dumps(pipeline))
          # ayuda extra al inferido de esquema
          .option("inferSchema.sampleSize", "200000")
          .load()
      )
    


    # -------------------- TRANSFORM --------------------
    def transform(self, df: DataFrame) -> DataFrame:
        # ========== 1) Elegir 1 'game' por nombre con ventana liviana ==========
        games_min = (
            df.where(F.col("type") == "game")
              .select(
                  F.col("appid").cast("int").alias("appid"),
                  "name",
                  F.coalesce(F.col("recommendations_total").cast("long"), F.lit(0)).alias("rec_tot_norm")
              )
        )

        # referencias desde DLC (solo ids) para elegir el winner
        dlc_refs_min = (
            df.where(F.col("type") == "dlc")
              .select(
                  F.col("appid").cast("int").alias("dlc_appid"),
                  F.col("fullgame.appid").cast("int").alias("fullgame_appid")
              )
              .where(F.col("fullgame_appid").isNotNull())
              .dropDuplicates(["dlc_appid", "fullgame_appid"])
        )

        refs_per_game = (
            dlc_refs_min.groupBy("fullgame_appid")
                        .agg(F.countDistinct("dlc_appid").alias("dlc_ref_count"))
        )

        ge = (
            games_min.alias("g")
            .join(refs_per_game.alias("r"), F.col("g.appid") == F.col("r.fullgame_appid"), "left")
            .drop("fullgame_appid")
            .withColumn("dlc_ref_count", F.coalesce(F.col("dlc_ref_count"), F.lit(0)))
            .withColumn("is_referenced", (F.col("dlc_ref_count") > 0).cast("int"))
        )

        # repartir por 'name' y aplicar ventana
        ge = ge.repartition(64, "name")
        w = Window.partitionBy("name").orderBy(
            F.desc("is_referenced"),
            F.desc("dlc_ref_count"),
            F.desc("rec_tot_norm"),
            F.desc("appid"),
        )
        winners_min = (
            ge.withColumn("rn", F.row_number().over(w))
              .where(F.col("rn") == 1)
              .select("appid", "name")
        )

        # ========== 2) Traer TODOS los campos del juego ganador ==========
        games_full = df.where(F.col("type") == "game").withColumn("appid", F.col("appid").cast("int"))
        winners_full = (
            winners_min.alias("w")
            .join(games_full.alias("gf"), F.col("w.appid") == F.col("gf.appid"), "left")
            .select("gf.*")
        )
        winners_full = winners_full.drop("dlc")

        # ========== 3) Construir DLC embebidos (documento completo) SOLO para winners ==========
        # Filtrar DLC que apunten a los winners (broadcast para reducir shuffle)
        winners_keys = winners_min.select(F.col("appid").alias("base_appid"))
        dlcs_raw = (
            df.where(F.col("type") == "dlc")
              .withColumn("appid", F.col("appid").cast("int"))
              .withColumn("fullgame_appid", F.col("fullgame.appid").cast("int"))
              .where(F.col("fullgame_appid").isNotNull())
        )

        dlcs_filtered = (
            dlcs_raw.join(F.broadcast(winners_keys),
                          F.col("fullgame_appid") == F.col("base_appid"),
                          "inner")
                    .dropDuplicates(["base_appid", "appid"])   # 1 dlc por base_appid+appid
        )

        # Estructura con TODO el documento DLC (incluye _id, type, etc.)
        dlc_cols = [c for c in dlcs_filtered.columns if c not in {"base_appid"}]
        dlcs_embedded = (
            dlcs_filtered
            .withColumn("dlc_doc", F.struct(*[F.col(c) for c in dlc_cols]))
            .repartition(128, "base_appid")
            .groupBy("base_appid")
            .agg(F.collect_list("dlc_doc").alias("dlc"))
        )

        # ========== 4) Unir dlc embebido + normalizar ==========
        final_games = (
            winners_full.alias("wf")
            .join(dlcs_embedded, F.col("wf.appid") == F.col("base_appid"), "left")
            .drop("base_appid")
            .withColumn("type", F.lit("game"))
        )

        # _id = appid (idempotencia)
        if "_id" in final_games.columns:
            final_games = final_games.drop("_id")
        final_games = final_games.withColumn("_id", F.col("appid"))

        # Orden EXACTO solicitado (si falta alguna, se crea nula)
        desired = [
            "appid","type","name",
            "required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score",
            "dlc",  # documentos completos
        ]
        for c in desired:
            if c not in final_games.columns:
                final_games = final_games.withColumn(c, F.lit(None))

        if "wf.dlc" in final_games.columns:
            final_games = final_games.drop("wf.dlc")        
                    
        final_games = final_games.select(*desired, "_id")
        return final_games
    # -------------------- WRITE (UPSERT) --------------------
    def write_upsert(self, df: DataFrame):
        """
        Upsert por appid en explotation_zone.juegos_steam usando PyMongo en foreachPartition.
        Requiere índices únicos recomendados:
          - unique en {"appid":1}
          - optional: unique en {"name":1, "type":1}
        """
        uri = self.explo.uri
        dbname = self.explo.db_name
        collname = self.explo_coll_name
        order = [
            "appid","type","name",
            "required_age","is_free","controller_support",
            "detailed_description","about_the_game","short_description",
            "supported_languages","legal_notice",
            "pc_requirements","mac_requirements","linux_requirements",
            "developers","publishers","platforms","categories","genres",
            "release_date","recommendations_total","metacritic_score",
            "dlc","_id",
        ]

        def _upsert_partition(rows):
            client = MongoClient(uri)
            coll = client[dbname][collname]
            for r in rows:
                rd = r.asDict(recursive=True)
                # dlc vacío si None
                if rd.get("dlc") is None:
                    rd["dlc"] = []
                # documento en el ORDEN exacto
                doc = OrderedDict()
                for k in order:
                    if k in rd:
                        doc[k] = rd[k]
                # extras (si Mongo Trusted añade nuevos campos en winners_full) van al final
                for k, v in rd.items():
                    if k not in doc:
                        doc[k] = v
                coll.replace_one({"appid": doc["appid"]}, doc, upsert=True)
            client.close()


        df.foreachPartition(_upsert_partition)

    # -------------------- RUN --------------------
    def run(self):
        df_trusted = self.load_trusted()
        final_games = self.transform(df_trusted)
        final_games = final_games.coalesce(64)
        self.write_upsert(final_games)
