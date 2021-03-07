from mongodb.initialize_db import InitilizeDB
import string


class Collections:
    def __init__(self):
        super().__init__()

    def create_collections(self):
        InitilizeDB.init()

        list_collections = InitilizeDB.list_collections()

        for collection in list_collections:
            if collection is not None:
                InitilizeDB.drop_collections(collection)

        intitial_values = {}
        collection_names = ["region", "part", "supplier",
                            "partsupp", "customer", "orders", "lineitem", "nation"]

        for each_val in collection_names:
            InitilizeDB.insert(each_val, intitial_values)
