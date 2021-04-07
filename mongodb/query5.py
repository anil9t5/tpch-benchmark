from mongodb.initialize_db import InitilizeDB
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

            pipeline = [
                {"$match":
                    {"$and":[{"order_date" : {"$gte": datetime.datetime(1994, 1, 1)}},
                              {"order_date": {"$lt": datetime.datetime(1995, 1, 1)}}]
                     }
                },
                {"$lookup": {
                    "from": "customer",
                    "localField": "cust_key",
                    "foreignField": "cust_key",
                    "as": "customer_docs"
                    }
                },
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
                {"$lookup": {
                    "from": "supplier",
                    "let": {"nationkey": "$nation_key", "supkey": "$supplier_key"},
                    "pipeline": [
                        {"$match":
                            {"$expr":
                                {"$and":
                                    [
                                        {"$eq": ["$customer_docs.nation_key", "$$nationkey"]},
                                        {"$eq": ["$lineitem_docs.l_suppkey", "$$supkey"]}
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "supplier_docs"
                    }
                },
                {
                    "$unwind": "$supplier_docs"
                }

            ]

            start_time = time.time()
            result = orders_table.aggregate(pipeline)
            print(list(result))
            end_time = time.time()
            print("---------------Query 5-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)







