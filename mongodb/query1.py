from mongodb.initialize_db import InitilizeDB
from bson.son import SON
import pymongo
from pymongo import errors
import datetime
import time


class Query1:
    URI = "mongodb://127.0.0.1:27017"
    DATABASE = None

    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            client = pymongo.MongoClient(Query1.URI)
            Query1.DATABASE = client["jointpch"]
            db = Query1.DATABASE

            pipeline = [
                {
                    "$match": {
                        "L_SHIPDATE": {
                            "$lte": "1998-12-01"
                        }
                    }
                },
                {
                    "$project": {
                        "L_RETURNFLAG": 1,
                        "L_LINESTATUS": 1,
                        "L_QUANTITY": 1,
                        "L_EXTENDEDPRICE": 1,
                        "L_DISCOUNT": 1,
                        "l_dis_min_1": {
                            "$subtract": [
                                1,
                                "$L_DISCOUNT"
                            ]
                        },
                        "l_tax_plus_1":{
                            "$add": [
                                "$L_TAX",
                                1
                            ]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "L_RETURNFLAG": "$L_RETURNFLAG",
                            "L_LINESTATUS": "$L_LINESTATUS"
                        },
                        "L_QUANTITY": {
                            "$sum": "$L_QUANTITY"
                        },
                        "sum_base_price": {
                            "$sum": "$L_EXTENDEDPRICE"
                        },
                        "sum_disc_price": {
                            "$sum": {
                                "$multiply": [
                                    "$L_EXTENDEDPRICE",
                                    "$l_dis_min_1"
                                ]
                            }
                        },
                        "sum_charge":{
                            "$sum": {
                                "$multiply": [
                                    "$L_EXTENDEDPRICE",
                                    {
                                        "$multiply": [
                                            "$l_tax_plus_1",
                                            "$l_dis_min_1"
                                        ]
                                    }
                                ]
                            }
                        },
                        "avg_price":{
                            "$avg": "$L_EXTENDEDPRICE"
                        },
                        "avg_disc": {
                            "$avg": "$L_DISCOUNT"
                        },
                        "count_order": {
                            "$sum": 1
                        }
                    }
                },
                {
                    "$sort": {
                        "L_RETURNFLAG": 1,
                        "L_LINESTATUS": 1
                    }
                }
            ]

            start_time = time.time()
            db["deals"].aggregate(pipeline)
            end_time = time.time()
            print("---------------Query 1-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
