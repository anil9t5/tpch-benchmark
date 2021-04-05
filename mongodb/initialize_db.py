import bson
import pymongo
from pymongo import errors


class InitilizeDB(object):
    URI = "mongodb://127.0.0.1:27017"
    DATABASE = None
    try:
        def init():
            client = pymongo.MongoClient(InitilizeDB.URI)
            InitilizeDB.DATABASE = client["tpch"]

            return InitilizeDB.DATABASE

        def list_collections():
            return InitilizeDB.DATABASE.list_collection_names()

        def drop_collections(name):
            InitilizeDB.DATABASE[name].drop()

    except errors.ServerSelectionTimeoutError as err:
        print("pymongo ERROR:", err)
