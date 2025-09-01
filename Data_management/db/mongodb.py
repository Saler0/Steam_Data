from pymongo import MongoClient, ASCENDING

class MongoDBClient:
    """    Gestiona conexiones a MongoDB:
     - Zona “trusted_zone” con sus colecciones
     - Zona “exploitation_zone” con sus colecciones"""
    def __init__(self,
                 uri: str = "mongodb://localhost:27017",
                 db_name : str = None):
        

        # Guardamos URI y nombre de la BD para usos externos (p. ej. Spark)
        self.uri     = uri
        self.db_name = db_name

        # crea y guarda la única instancia de PyMongo que vas a usar
        self.client = MongoClient(uri)
        self.db     = self.client[db_name]

        if db_name == "trusted_zone":
            # COLLECCIONES DE LA TRUSTED ZONE

            #  == Steam juegos ==
            self.juegos  = self.db["juegos_steam"]
            
            # Crea indice sólo si no existe
            self.juegos.create_index(
                [("appid", ASCENDING)],
                unique=True,        # evita duplicados
                background=True     # lo construye en segundo plano
            )

            #  == Reviews juegos ==
            self.reviews   = self.db["steam_reviews"]

            #  == Notiicas juegos ==
            self.news_games = self.db["news_games"]

            # Crea indice sólo si no existe
            self.news_games.create_index(
                [("gid", ASCENDING)],
                unique=True,        # evita duplicados
                background=True     # lo construye en segundo plano
            )


            # Youtube
            self.videos_youtube     = self.db["video_youtube"]
            self.comentarios_youtube = self.db["comentarios_youtube"]

        elif db_name == "explotation_zone":

            #  == Steam juegos ==
            self.juegos_explo  = self.db["juegos_steam"]
            
            # Crea indice sólo si no existe
            self.juegos_explo.create_index(
                [("appid", ASCENDING)],
                unique=True,        # evita duplicados
                background=True     # lo construye en segundo plano
            )
            self.juegos_explo.create_index(
                [("name", 1), ("type", 1)],
                unique=True,
                background=True)
            
            #  == Notiicas juegos ==
            self.news_games = self.db["news_games"]

            # Crea indice sólo si no existe
            self.news_games.create_index(
                [("gid", ASCENDING)],
                unique=True,        # evita duplicados
                background=True     # lo construye en segundo plano
            )


        else:
            raise ValueError(f"DB desconocida: {db_name}")
        