from mongodb.initialize_db import InitilizeDB
from bson.son import SON
from pymongo import errors
import datetime
import time


class Query1:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            collection = InitilizeDB.init()

            pipeline = [
                {
                    "$match": {
                        "l_shipdate": {
                            "$lte": datetime.datetime(1998, 12, 1)
                        }
                    }
                },
                {
                    "$project": {
                        "l_returnflag": 1,
                        "l_linestatus": 1,
                        "l_quantity": 1,
                        "l_extendedprice": 1,
                        "l_discount": 1,
                        "l_dis_min_1": {
                            "$subtract": [
                                1,
                                "$l_discount"
                            ]
                        },
                        "l_tax_plus_1":{
                            "$add": [
                                "$l_tax",
                                1
                            ]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "return_flag": "$l_returnflag",
                            "line_status": "$l_linestatus"
                        },
                        "l_quantity": {
                            "$sum": "$l_quantity"
                        },
                        "sum_base_price": {
                            "$sum": "$l_extendedprice"
                        },
                        "sum_disc_price": {
                            "$sum": {
                                "$multiply": [
                                    "$l_extendedprice",
                                    "$l_dis_min_1"
                                ]
                            }
                        },
                        "sum_charge":{
                            "$sum": {
                                "$multiply": [
                                    "$l_extendedprice",
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
                            "$avg": "$l_extendedprice"
                        },
                        "avg_disc": {
                            "$avg": "$l_discount"
                        },
                        "count_order": {
                            "$sum": 1
                        }
                    }
                },
                {
                    "$sort": {
                        "l_returnflag": 1,
                        "l_linestatus": 1
                    }
                }
            ]

            start_time = time.time()
            result = collection["lineitem"].aggregate(pipeline)
            print(list(result))
            end_time = time.time()
            print("---------------Query 1-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
