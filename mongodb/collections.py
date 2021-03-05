from mongodb.initialize_db import InitilizeDB


class Collections:
    def __init__(self):
        super().__init__()

    def create_collections(self):
        InitilizeDB.init()
        # collection_names = ["part", "supplier", "partsupp",
        #                     "customer", "orders", "lineitem", "nation", "region"]
        intitial_values = {}
        collection_names = ["region"]
        for each_val in collection_names:
            InitilizeDB.insert(each_val, intitial_values)
