from pymongo import MongoClient

class MongoDBClient:
    """Gestiona la conexión a MongoDB y expone las colecciones."""
    def __init__(self,
                 uri: str = "mongodb://localhost:27017",
                 db_name: str = "trusted_zone",
                 juegos_coll: str = "juegos_steam",
                 reviews_coll: str = "steam_reviews",
                 video_youtube_coll: str = "video_youtube",
                 comentarios_youtube_coll: str = "comentarios_youtube",
                 log_coll: str = "import_log"):
        
        # Guardamos URI y nombre de la BD para usos externos (p. ej. Spark)
        self.uri     = uri
        self.db_name = db_name

        # crea y guarda la única instancia de PyMongo que vas a usar
        self.client = MongoClient(uri)
        self.db     = self.client[db_name]

        # Steam
        self.juegos  = self.db[juegos_coll]
        self.reviews   = self.db[reviews_coll]
        self.import_log = self.db[log_coll]

        # Youtube
        self.videos_youtube     = self.db[video_youtube_coll]
        self.comentarios_youtube = self.db[comentarios_youtube_coll]
