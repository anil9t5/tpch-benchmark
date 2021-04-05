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
                    "from": "lineitem",
                    "localField": "cust_key",
                    "foreignField": "l_orderkey",
                    "as": "lineitem_docs"
                }},

                {
                    "$unwind": "$lineitem_docs"
                },
                {
                    "$match": {
                        "mkt_segment": "AUTOMOBILE",
                        "lineitem_docs.l_shipdate": {
                            "$gte": datetime.datetime(1995, 3, 15)
                        }
                    }
                },
                {"$lookup": {
                    "from": "orders",
                    "localField": "cust_key",
                    "foreignField": "order_key",
                    "as": "orders_docs"
                }},
                {
                    "$unwind": "$orders_docs"
                },
                {
                    "$project": {
                        "orders_docs.order_date": 1,
                        "orders_docs.ship_priority": 1,
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
                            "order_key": "$orders_docs.order_key",
                            "order_date": "$orders_docs.order_date",
                            "ship_priority": "$orders_docs.ship_priority"
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
                        "orders_docs.order_date": 1
                    }
                }
            ]

            start_time = time.time()
            result = collection["customer"].aggregate(pipeline)
            print(list(result))
            end_time = time.time()
            print("---------------Query 3-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
