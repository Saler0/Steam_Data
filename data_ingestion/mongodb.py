from pymongo import MongoClient

class MongoDBClient:
    """Gestiona la conexión a MongoDB y expone las colecciones."""
    def __init__(self,
                 uri: str = "mongodb://localhost:27017",
                 db_name: str = "steam_data",
                 juegos_coll: str = "steam_data",
                 reviews_coll: str = "steam_reviews",
                 log_coll: str = "import_log"):
        client = MongoClient(uri)
        db = client[db_name]
        self.juegos    = db[juegos_coll]
        self.reviews   = db[reviews_coll]
        self.import_log = db[log_coll]