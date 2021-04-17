from mongodb.initialize_db import InitilizeDB
from bson.son import SON
from pymongo import errors
import datetime
import time


class Query3:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            collection = InitilizeDB.init()

            pipeline = [
                {
                    "$match": {
                        "C_MKTSEGMENT": "AUTOMOBILE",
                        "L_SHIPDATE": {
                            "$gte": "1995-03-01",
                        },
                        "O_ORDERDATE": {
                            "$lte": "1995-03-31",
                        }
                    }
                },
                {
                    "$project": {
                        "O_ORDERDATE": 1,
                        "O_SHIPPRIORITY": 1,
                        "L_EXTENDEDPRICE": 1,
                        "l_dis_min_1": {
                            "$subtract": [
                                1,
                                "$L_DISCOUNT"
                            ]
                        }
                    }
                },

                {
                    "$group": {
                        "_id": {
                            "O_ORDERKEY": "$L_ORDERKEY",
                            "O_ORDERDATE": "$O_ORDERDATE",
                            "O_SHIPPRIORITY": "$O_SHIPPRIORITY"
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
                },
                {
                    "$sort": {
                        "revenue": 1,
                        "O_ORDERDATE": 1
                    }
                }
            ]

            start_time = time.time()
            collection["rel_lineitem_orders_customer"].aggregate(
                pipeline)

            end_time = time.time()
            print("---------------Query 3-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
