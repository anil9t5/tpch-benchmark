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
                {"$lookup": {
                    "from": "customer",
                    "localField": "cust_key",
                    "foreignField": "cust_key",
                    "as": "customer_docs"
                }},

                {
                    "$unwind": "$customer_docs"
                },
                {"$lookup": {
                    "from": "lineitem",
                    "localField": "order_key",
                    "foreignField": "l_orderkey",
                    "as": "lineitem_docs"
                }},
                {
                    "$unwind": "$lineitem_docs"
                },
                {
                    "$match": {
                        "customer_docs.mkt_segment": "AUTOMOBILE",
                        "lineitem_docs.l_shipdate": {
                            "$gte": datetime.datetime(1995, 3, 1),
                        },
                        "order_date": {
                            "$lte": datetime.datetime(1995, 3, 31),
                        }
                    }
                },
                {
                    "$project": {
                        "order_date": 1,
                        "ship_priority": 1,
                        "lineitem_docs.l_extendedprice": 1,
                        "l_dis_min_1": {
                            "$subtract": [
                                1,
                                "$lineitem_docs.l_discount"
                            ]
                        }
                    }
                },

                {
                    "$group": {
                        "_id": {
                            "order_key": "$lineitems_docs.l_orderkey",
                            "order_date": "$order_date",
                            "ship_priority": "$ship_priority"
                        },
                        "revenue": {
                            "$sum": {
                                "$multiply": [
                                    "$lineitem_docs.l_extendedprice",
                                    "$l_dis_min_1"
                                ]
                            }
                        }
                    }
                },
                {
                    "$sort": {
                        "revenue": 1,
                        "order_date": 1
                    }
                }
            ]

            start_time = time.time()
            result = collection["orders"].aggregate(pipeline)
            print(list(result))
            end_time = time.time()
            print("---------------Query 3-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
