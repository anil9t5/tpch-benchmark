import pymongo


class InitilizeDB(object):
    URI = "mongodb://127.0.0.1:27017"
    DATABASE = None

    @staticmethod
    def init():
        client = pymongo.MongoClient(InitilizeDB.URI)
        InitilizeDB.DATABASE = client["tpch"]

    @staticmethod
    def insert(collection, data):
        InitilizeDB.DATABASE[collection].insert(data)

    @staticmethod
    def list_collections():
        return InitilizeDB.DATABASE.list_collection_names()

    @staticmethod
    def drop_collections(name):
        InitilizeDB.DATABASE[name].drop()