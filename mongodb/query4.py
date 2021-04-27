# We joined codes found on  https://www.ifi.uzh.ch/dam/jcr:ffffffff-96c1-007c-0000-000010c732ce/VertiefungRutishauser.pdf and added our own to make them work.
from mongodb.initialize_db import InitilizeDB
from bson.son import SON
from pymongo import errors
import datetime
import time
import random
import pprint


class Query4:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            collection = InitilizeDB.init()
            lineitem_table = collection["lineitem"]
            orders_table = collection['orders']

            all_years = [1993, 1994, 1995, 1996, 1997]
            full_years = [1993, 1994, 1995, 1996]
            random_month = random.randint(1, 12)

            #datetime.datetime(1994, 5, 9, 0, 0)
            if random_month > 10:
                random_date = datetime.datetime(
                    full_years[random.randint(0, 3)], random_month, 1, 0, 0)
            else:
                random_date = datetime.datetime(
                    all_years[random.randint(0, 4)], random_month, 1, 0, 0)

            temp_pipeline = [
                {
                    "$project": {
                        "_id": 0,
                        "L_ORDERKEY": 1,
                        "eq": {
                            "$cond": [{
                                "$lt": ["$L_COMMITDATE", "$L_RECEIPTDATE"]
                            },
                                1,
                                0
                            ]
                        }
                    }
                },
                {
                    "$match": {
                        "eq": {
                            "$eq": 1
                        }
                    }
                }
            ]

            # temp_dict = list(lineitem_table.aggregate(temp_pipeline))
            # orderkeys_list = []
            # for item in temp_dict:
            #     orderkeys_list.append(item['L_ORDERKEY'])

            pipeline = [
                {
                    "$project": {
                        "O_ORDERDATE": 1,
                        "O_ORDERPRIORITY": 1,
                        "O_ORDERKEY": 1
                    }
                },
                {
                    "$match": {
                        "O_ORDERDATE": {
                            "$gte": "1993-01-01",
                            "$lt": "1994-01-01",
                        },
                        # "O_ORDERKEY": {
                        #     "$in": orderkeys_list
                        # }

                    }
                },
                {
                    "$group": {
                        "_id": {
                            "O_ORDERPRIORITY": "$O_ORDERPRIORITY"
                        },
                        "order_count": {
                            "$sum": 1
                        }
                    }
                },
                {
                    "$sort": {
                        "O_ORDERPRIORITY": 1
                    }
                }
            ]

            start_time = time.time()
            collection["orders"].aggregate(pipeline)

            end_time = time.time()
            print("---------------Query 4-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
