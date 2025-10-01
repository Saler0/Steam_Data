from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import CollectionInvalid


class MongoDBClient:
    """Simple helper to manage MongoDB access for the backend."""
    def __init__(self, uri: str = "mongodb://localhost:27017", db_name: str = "exploitation_zone"):
        self.uri = uri
        self.db_name = db_name

        self.client = MongoClient(uri)
        self.db = self.client[db_name]

    def ping(self) -> bool:
        self.client.admin.command("ping")
        return True

    def get_collection(self, name: str) -> Collection:
        if name not in self.db.list_collection_names():
            try:
                self.db.create_collection(name)
            except CollectionInvalid:
                pass
        return self.db[name]
