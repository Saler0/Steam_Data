from pymongo import MongoClient, ASCENDING


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
