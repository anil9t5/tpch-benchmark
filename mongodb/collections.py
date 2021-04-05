from mongodb.initialize_db import InitilizeDB
import string


class Collections:
    def __init__(self, db):
        super().__init__()
        self.db = db

    def create_collections(self):
        InitilizeDB.init()

        list_collections = InitilizeDB.list_collections()

        for collection in list_collections:
            if collection is not None:
                InitilizeDB.drop_collections(collection)

        values = {}
        collection_names = ["region", "part", "supplier",
                            "partsupp", "customer", "orders", "lineitem", "nation"]
