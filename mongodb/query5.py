from mongodb.initialize_db import InitilizeDB
import pymongo
from pymongo import errors
import datetime
import time
import random


class Query5:
    URI = "mongodb://127.0.0.1:27017"
    DATABASE = None

    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            # collection = InitilizeDB.init()
            client = pymongo.MongoClient(Query5.URI)
            Query5.DATABASE = client["jointpch"]
            db = Query5.DATABASE
            pipeline = [
                {
                    "$project": {
                        "partsupp.supplier.nation.N_NAME": 1,
                        "partsupp.supplier.nation.region.R_NAME": 1,
                        "L_EXTENDEDPRICE": 1,
                        "L_DISCOUNT": 1,
                        "order.O_ORDERDATE": 1,
                        "l_dis_min_1": {
                            "$subtract": [
                                1,
                                "$L_DISCOUNT"
                            ]
                        },
                        "c_nkT0s_nk":{
                            "$cmp": ["$order.customer.C_NATIONKEY", "$partsupp.supplier.S_NATIONKEY"]
                        }
                    }
                },
                {
                    "$match": {
                        "partsupp.supplier.nation.region.R_NAME": "AFRICA",
                        "c_nkT0s_nk": {
                            "$eq": 0
                        },
                        "order.O_ORDERDATE": {
                            "$gte": "1996-01-01",
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "N_NAME": "$partsupp.supplier.nation.N_NAME"
                        },
                        "revenue": {
                            "$sum": {
                                "$multiply": [
                                    "$L_EXTENDEDPRICE",
                                    "$l_dis_min_1"
                                ]
                            }
                        }
                    }
                }
            ]

            start_time = time.time()
            db["deals"].aggregate(pipeline)

            end_time = time.time()
            print("---------------Query 5-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
