from mongodb.initialize_db import InitilizeDB


class Collections:
    def __init__(self):
        super().__init__()

    def create_collections(self):
        db = InitilizeDB.init()
        collection_names = ["part", "supplier", "partsupp",
                            "customer", "orders", "lineitem", "nation", "region"]

        if db is not None:
            print(db)
            for each_val in collection_names:
                print(each_val)
                col = db[each_val]  # create collection
                col.insert_one({"name": 10})
