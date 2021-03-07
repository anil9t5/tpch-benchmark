from mongodb.initialize_db import InitilizeDB
import string
import random


class InsertData:
    def __init__(self):
        super().__init__()

    @staticmethod
    def region():
        region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
        region_collection_values = []

        # initializing size of string
        N = 100

        # generating random strings
        res = ''.join(random.choices(string.ascii_lowercase, k=N))
        for i, val in enumerate(region_names):
            values = {
                "region_key": i,
                "name": val,
                "comment": str(res)
            }
            region_collection_values.append(values)

        InitilizeDB.insert(
            InitilizeDB.DATABASE["region"].name, region_collection_values)

    def insert_to_collections(self):
        InsertData.region()
