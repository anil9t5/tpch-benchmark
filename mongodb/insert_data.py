from mongodb.initialize_db import InitilizeDB


class InsertData:
    def __init__(self):
        super().__init__()

    @staticmethod
    def region():
        region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
        region_collection_values = []

        for i, val in enumerate(region_names):
            values = {
                "region_key": i,
                "name": val,
                "comment": "I think it should be fine...let's hope it works"
            }
            region_collection_values.append(values)

        InitilizeDB.insert(
            InitilizeDB.DATABASE["region"].name, region_collection_values)

    def insert_to_collections(self):
        InsertData.region()
