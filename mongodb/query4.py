from mongodb.initialize_db import InitilizeDB
from bson.son import SON
from pymongo import errors
import datetime
import time


class Query4:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            collection = InitilizeDB.init()

            pipeline = [
                {
                    "$project": {
                        "order_date": 1,
                        "order_priority": 1,
                        "eq": {
                            "$cond": [
                                {
                                    "$lt": [
                                        "$lineitems.l_commitdate",
                                        "$lineitems.l_receiptdate"
                                    ]
                                },
                                0,
                                1
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
            print("---------------Query 3-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
