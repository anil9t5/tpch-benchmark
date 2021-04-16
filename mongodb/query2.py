from mongodb.initialize_db import InitilizeDB
from bson.son import SON
from pymongo import errors
import datetime
import time


class Query2:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            collection = InitilizeDB.init()

            pipeline = [
                {"$lookup": {
                    "from": "supplier",
                    "localField": "PS_SUPPKEY",
                    "foreignField": "S_SUPPKEY",
                    "as": "supplier_docs"
                }},
                {
                    "$unwind": "$supplier_docs"
                },

                {"$lookup": {
                    "from": "nation",
                    "localField": "supplier_docs.S_NATIONKEY",
                    "foreignField": "N_NATIONKEY",
                    "as": "nation_docs"
                }
                },
                {
                    "$unwind": "$nation_docs"
                },
                {"$lookup": {
                    "from": "part",
                    "localField": "PS_PARTKEY",
                    "foreignField": "P_PARTKEY",
                    "as": "part_docs"
                }},
                {
                    "$unwind": "$part_docs"
                },
                {"$lookup": {
                    "from": "region",
                    "localField": "nation_docs.N_REGIONKEY",
                    "foreignField": "R_REGIONKEY",
                    "as": "region_docs"
                }},
                {
                    "$unwind": "$region_docs"
                },
                {"$match": {
                    "part_docs.P_SIZE": 17,
                    "part_docs.P_TYPE": {"$regex": "BRASS", "$options": "g"},
                    "region_docs.R_NAME": "EUROPE",
                    "compare_region_key": {"$eq": 0}
                }
                },
                {
                    "$project": {
                        "supplier_docs.S_ACCTBAL": 1,
                        "supplier_docs.S_NAME": 1,
                        "nation_docs.N_NAME": 1,
                        "part_docs.P_PARTKEY": 1,
                        "part_docs.P_MFGR": 1,
                        "supplier_docs.S_ADDRESS": 1,
                        "supplier_docs.S_PHONE": 1,
                        "supplier_docs.S_COMMENT": 1,
                        "PS_SUPPLYCOST": 1,
                        "compare_region_key": {
                            "$cmp": ["$nation_docs.N_REGIONKEY", "$region_docs.R_REGIONKEY"]
                            # $cmp is a compare operator that stores zero,1,-1 in c_nkTOs_nk field. The numbers mean equal/greater/less than respectively
                        }
                    }
                },

                {
                    "$sort": {
                        "supplier_docs.S_ACCTBAL": -1,
                        "nation_docs.N_NAME": 1,
                        "supplier_docs.S_NAME": 1,
                        "part_docs.P_PARTKEY": 1
                    }
                }
            ]

            start_time = time.time()
            collection["partsupp"].aggregate(pipeline)

            end_time = time.time()
            print("---------------Query 2-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)
