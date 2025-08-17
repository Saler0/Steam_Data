from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pymongo import MongoClient
from db.mongodb import MongoDBClient
import json

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
        pipeline = [
            {
                "$match": {
                    "name": {"$nin": ["", None]},
                    "type": {"$in": ["game", "dlc"]},
                }
            }
        ]
        return (
            self.spark.read
            .format("mongo")
            .option("uri", self.trusted_coll_uri)
            .option("pipeline", json.dumps(pipeline))
            .load()
        )

    # -------------------- TRANSFORM --------------------
    def transform(self, df: DataFrame) -> DataFrame:
      
      # 1) separar games / dlcs
      games = df.where(F.col("type") == "game")
      dlcs  = df.where(F.col("type") == "dlc")

      # 2) referencias DLC -> juego base (fullgame.appid)
      dlcs_refs = (
          dlcs
          .withColumn("fullgame_appid", F.col("fullgame.appid").cast("int"))
          .withColumn("dlc_appid", F.col("appid").cast("int"))
          .where(F.col("fullgame_appid").isNotNull())
          .select("dlc_appid", "fullgame_appid")
          .dropDuplicates(["dlc_appid", "fullgame_appid"])
      )

      # 3) stats por juego base (cuántos dlc lo referencian)
      refs_per_game = (
          dlcs_refs
          .groupBy("fullgame_appid")
          .agg(
              F.countDistinct("dlc_appid").alias("dlc_ref_count")
          )
      )

      # 4) enriquecer games con stats y escoger "winner" por nombre
      games_enriched = (
          games
          .withColumn("appid", F.col("appid").cast("int"))
          .join(refs_per_game, games["appid"] == refs_per_game["fullgame_appid"], "left")
          .drop("fullgame_appid")
          .withColumn("dlc_ref_count", F.coalesce(F.col("dlc_ref_count"), F.lit(0)))
          .withColumn("is_referenced", (F.col("dlc_ref_count") > 0).cast("int"))
          .withColumn("rec_tot_norm", F.coalesce(F.col("recommendations_total").cast("long"), F.lit(0)))
      )

      # 4b) window: un solo "game" por nombre
      w = Window.partitionBy("name").orderBy(
          F.desc("is_referenced"),
          F.desc("dlc_ref_count"),
          F.desc("rec_tot_norm"),
          F.desc("appid"),
      )
      winners = (
          games_enriched
          .withColumn("rn", F.row_number().over(w))
          .where(F.col("rn") == 1)
          .drop("rn", "rec_tot_norm")
      )

      # 5) construir documentos DLC completos (struct con TODOS los campos del dlc)
      #    - primero preparamos el df de dlcs con una columna struct (dlc_doc)
      dlc_cols = [c for c in dlcs.columns]           # todos los campos que vengan de trusted
      dlcs_full = dlcs.withColumn("appid", F.col("appid").cast("int"))
      dlc_doc_struct = F.struct(*[F.col(c) for c in dlc_cols])
      dlcs_full = dlcs_full.withColumn("dlc_doc", dlc_doc_struct)

      # 6) quedarnos solo con los DLC que apunten a cada winner (join por fullgame_appid)
      dlcs_linked = (
          dlcs_full
          .withColumn("fullgame_appid", F.col("fullgame.appid").cast("int"))
          .join(
              winners.select(F.col("appid").alias("base_appid")),
              F.col("fullgame_appid") == F.col("base_appid"),
              "inner"
          )
          .select("base_appid", "dlc_doc", "appid")   # appid del dlc para dedupe
          .dropDuplicates(["base_appid", "appid"])
      )

      # 7) agregamos la lista de dlc_doc por juego base
      dlc_embedded = (
          dlcs_linked
          .groupBy("base_appid")
          .agg(F.collect_list("dlc_doc").alias("dlc"))
      )

      # 8) unimos a winners dejando TODOS los campos del winner + dlc embebido + type="game"
      final_games = (
          winners.alias("g")
          .join(dlc_embedded, F.col("g.appid") == F.col("base_appid"), "left")
          .drop("base_appid")
          .withColumn("dlc", F.coalesce(F.col("dlc"), F.array().cast("array<struct<>>")))
          .withColumn("type", F.lit("game"))
      )

      # 9) idempotencia: _id = appid (por si el winner traía otro _id)
      if "_id" in final_games.columns:
          final_games = final_games.drop("_id")
      final_games = final_games.withColumn("_id", F.col("appid"))

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

        def _upsert_partition(rows):
            client = MongoClient(uri)
            coll = client[dbname][collname]
            for r in rows:
                doc = {k: r[k] for k in r.asDict().keys()}
                # _id ya es appid
                coll.update_one({"appid": doc["appid"]}, {"$set": doc}, upsert=True)
            client.close()

        df.foreachPartition(_upsert_partition)

    # -------------------- RUN --------------------
    def run(self):
        df_trusted = self.load_trusted()
        final_games = self.transform(df_trusted)
        self.write_upsert(final_games)
