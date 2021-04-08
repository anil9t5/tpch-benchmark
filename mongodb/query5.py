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
                    "localField": "lineitem_docs.l_suppkey",
                    "foreignField": "supplier_key",
                    "as": "supplier_docs"
                }},
                {
                    "$unwind": "$supplier_docs"
                },
                {"$lookup": {
                    "from": "nation",
                    "localField": "supplier_docs.nation_key",
                    "foreignField": "nation_key",
                    "as": "nation_docs"
                }},
                {
                    "$unwind": "$nation_docs"
                },
                {"$lookup": {
                    "from": "region",
                    "localField": "nation_docs.region_key",
                    "foreignField": "region_key",
                    "as": "region_docs"
                }},
                {
                    "$unwind": "$region_docs"
                },
                {
                    "$project":{
                        "nation_docs.name":1,
                        "region_docs.name":1,
                        "lineitem_docs.l_extendedprice":1,
                        "lineitem_docs.l_discount":1,
                        "order_date":1,
                        "customer_docs.nation_key":1,
                        "supplier_docs.nation_key":1,
                        "ldist_minus1":{
                            "$subtract":[ 1, "$lineitem_docs.l_discount" ]
                        },
                        "compare_nationkey":{
                            "$cmp":["$customer_docs.nation_key","$supplier_docs.nation_key"]
                            #$cmp is a compare operator that stores zero,1,-1 in c_nkTOs_nk field. The numbers mean equal/greater/less than respectively
                        }
                    }
                },
                {
                    "$match":{
                        "region_docs.name": "AFRICA",
                        "compare_nationkey":{"$eq":0}
                    }
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










