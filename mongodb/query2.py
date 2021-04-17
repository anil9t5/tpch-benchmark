from mongodb.initialize_db import InitilizeDB
from bson.son import SON
import pymongo
from pymongo import errors
import datetime
import time


class Query2:
    URI = "mongodb://127.0.0.1:27017"
    DATABASE = None

    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            client = pymongo.MongoClient(Query2.URI)
            Query2.DATABASE = client["jointpch"]
            db = Query2.DATABASE

            pipeline = [
                {
                    "$project": {
                        "partsupp.supplier.S_ACCTBAL": 1,
                        "partsupp.supplier.S_NAME": 1,
                        "partsupp.supplier.nation.region.R_NAME": 1,
                        "partsupp.supplier.nation.N_NAME": 1,
                        "partsupp.part.P_PARTKEY": 1,
                        "partsupp.part.P_TYPE": 1,
                        "partsupp.part.P_SIZE": 1,
                        "partsupp.part.P_MFGR": 1,
                        "partsupp.supplier.S_ADDRESS": 1,
                        "partsupp.supplier.S_PHONE": 1,
                        "partsupp.supplier.S_COMMENT": 1,
                        "partsupp.PS_SUPPLYCOST": 1,
                        "c_rkT0s_nk": {
                            "$cmp": ["$partsupp.supplier.nation.N_NATIONKEY", "$partsupp.supplier.nation.region.R_REGIONKEY"]
                            # $cmp is a compare operator that stores zero,1,-1 in c_nkTOs_nk field. The numbers mean equal/greater/less than respectively
                        }
                    }
                },
                {"$match": {
                    "partsupp.part.P_SIZE": 16,
                    "partsupp.part.P_TYPE": {"$regex": "COPPER", "$options": "g"},
                    "partsupp.supplier.nation.region.R_NAME": "AFRICA",
                    "c_rkT0s_nk": {"$eq": 1}
                }
                },
                {
                    "$sort": {
                        "partsupp.supplier.S_ACCTBAL": -1,
                        "partsupp.supplier.nation.N_NAME": 1,
                        "partsupp.supplier.S_NAME": 1,
                        "partsupp.part.P_PARTKEY": 1
                    }
                }
            ]

            start_time = time.time()
            db["deals"].aggregate(pipeline)

            end_time = time.time()
            print("---------------Query 2-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
