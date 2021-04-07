from mongodb.initialize_db import InitilizeDB
from bson.son import SON
from pymongo import errors
import datetime
import time
import random


class Query5:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            collection = InitilizeDB.init()
            region_table = collection['region']
            orders_table = collection['orders']
            part_table = collection['part']
            nation_table = collection['nation']
            customer_table = collection['customer']
            lineitem_table = collection['lineitem']
            partsupp_table = collection['partsupp']
            supplier_table = collection['supplier']

            regions = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
            random_region = regions[random.randint(0, 4)]

            years = [1993, 1994, 1995, 1996, 1997]
            random_date = datetime.datetime(years[random.randint(0, 4)], 1, random.randint(1, 31), 0, 0)

            queriesPipe = [
                {"$lookup": {
                    "from": "orders",
                    "let": {"custkey": "$cust_key", "orderdate": "$order_date"},
                    "pipeline": [
                        {"$match":
                            {"$expr":
                                {"$and":
                                [
                                    {"$eq": ["$cust_key", "$$custkey"]},
                                    {"$gte": ["$$orderdate","datetime.datetime(1994,1,1)"]},
                                    {"$lt": ["$$orderdate","datetime.datetime(1995,1,1)"]}
                                ]
                                 }
                             }
                         },
                        { "$project": {"order_key": 1, "cust_key":1, "nation_key":1, "order_date":1, "_id": 0}}
                    ],
                    "as": "lj_customer_order"
                }
                },
                {
                    "$unwind": "$lj_customer_order"
                },


            ]

            start_time = time.time()
            result = customer_table.aggregate(queriesPipe)
            print(list(result))
            end_time = time.time()
            print("---------------Query 1-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
