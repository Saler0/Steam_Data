from pymongo import MongoClient

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

            # Steam
            self.juegos  = self.db["juegos_steam"]
            self.reviews   = self.db["steam_reviews"]

            # Youtube
            self.videos_youtube     = self.db["video_youtube"]
            self.comentarios_youtube = self.db["comentarios_youtube"]

        elif db_name == "explotation_zone":
            # COLLECCIONES DE LA EXPLOTATION ZONE
            pass

        else:
            raise ValueError(f"DB desconocida: {db_name}")
        