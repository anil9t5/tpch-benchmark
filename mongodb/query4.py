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
            lineitem_table=collection["lineitem"]
            orders_table = collection['orders']

            all_years = [1993, 1994, 1995, 1996, 1997]
            full_years = [1993, 1994, 1995, 1996]
            random_month = random.randint(1, 12)

            #datetime.datetime(1994, 5, 9, 0, 0)
            if random_month > 10:
                random_date = datetime.datetime(full_years[random.randint(0, 3)], random_month, 1, 0, 0)
            else:
                random_date = datetime.datetime(all_years[random.randint(0, 4)], random_month, 1, 0, 0)

            temp_pipeline=[
                {
                    "$project":{
                        "_id":0,
                        "l_orderkey":1,
                        "eq":{
                            "$cond":[{
                                "$lt":["$l_commitdate","$l_receiptdate"]
                            },
                            1,
                            0
                            ]
                        }
                    }
                },
                {
                    "$match":{
                        "eq":{
                            "$eq":1
                        }
                    }
                }
            ]

            temp_dict = list(lineitem_table.aggregate(temp_pipeline))

            orderkeys_list = []
            for item in temp_dict:
                orderkeys_list.append(item['l_orderkey'])


            pipeline = [
                {
                    "$project": {
                        "order_date": 1,
                        "order_priority": 1,
                        "order_key":1
                    }
                },
                {
                    "$match": {
                        "order_date": {
                            "$gte": datetime.datetime(1993, 1, 1),
                            "$lt": datetime.datetime(1994, 1, 1)
                        },
                        "order_key":{
                            "$in":orderkeys_list
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "order_priority": "$order_priority"
                        },
                        "order_count": {
                            "$sum": 1
                        }
                    }
                },
                {
                    "$sort": {
                        "order_priority": 1
                    }
                }
            ]

            start_time = time.time()
            result = collection["orders"].aggregate(pipeline)
            print(list(result))
            end_time = time.time()
            print("---------------Query 4-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)

