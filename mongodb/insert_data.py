from mongodb.initialize_db import InitilizeDB
import string
import random


class InsertData:
    def __init__(self):
        super().__init__()

    @staticmethod
    def generate_random_data():
        # initializing size of string
        N = 100
        # generating random data..
        res = ''.join(random.choices(string.ascii_lowercase, k=N))
        return str(res)

    @staticmethod
    def populate_region_or_nation(data):
        values = []

        for i, val in enumerate(data):
            value = {
                "region_key": i,
                "name": val,
                "comment": InsertData.generate_random_data()
            }
            values.append(value)
        return values

    @staticmethod
    def region():
        region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
        region_collection_values = InsertData.populate_region_or_nation(
            region_names)
        InitilizeDB.insert(
            InitilizeDB.DATABASE["region"].name, region_collection_values)

    @staticmethod
    def insert_nation():
        nations = {"ALGERIA,0", "ARGENTINA,1", "BRAZIL,1", "CANADA,1", "EGYPT,4", "ETHIOPIA,0",
                                "FRANCE,3", "GERMANY,3", "INDIA,2", "INDONESIA,2", "IRAN,4", "IRAQ,4", "JAPAN,2", "JORDAN,4", "KENYA,0",
                                "MOROCCO,0", "MOZAMBIQUE,0", "PERU,1", "CHINA,2", "ROMANIA,3", "SAUDI ARABIA,4", "VIETNAM,2",
                                "RUSSIA,3", "UNITED KINGDOM,3", "UNITED STATES,1"}
        nations_collection_values = InsertData.populate_region_or_nation(
            nations)
        InitilizeDB.insert(
            InitilizeDB.DATABASE["nation"].name, nations_collection_values)

    def insert_to_collections(self):
        InsertData.region()
        InsertData.insert_nation()
