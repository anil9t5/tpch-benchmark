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
            partsupp_table = collection['partsupp']

            pipeline = [
                {"$lookup": {
                    "from": "supplier",
                    "localField": "supp_key",
                    "foreignField": "supplier_key",
                    "as": "supplier_docs"
                }},
                {
                    "$unwind": "$supplier_docs"
                }
                ,

                {"$lookup": {
                    "from": "nation",
                    "localField": "supplier_docs.nation_key",
                    "foreignField": "nation_key",
                    "as": "nation_docs"
                }
                },
                {
                    "$unwind": "$nation_docs"
                }
                ,
                {"$lookup": {
                    "from": "part",
                    "localField": "part_key",
                    "foreignField": "part_key",
                    "as": "part_docs"
                }},
                {
                    "$unwind": "$part_docs"
                }
                ,
                {"$lookup": {
                    "from": "region",
                    "localField": "nation_docs.region_key",
                    "foreignField": "region_key",
                    "as": "region_docs"
                }},
                {
                    "$unwind": "$region_docs"
                }
                ,
                {
                    "$project": {
                        "supplier_docs.acct_bal": 1,
                        "supplier_docs.name": 1,
                        "nation_docs.name":1,
                        "part_docs.part_key":1,
                        "part_docs.mfgr": 1,
                        "supplier_docs.address": 1,
                        "supplier_docs.phone": 1,
                        "supplier_docs.comment": 1,
                        "supply_cost":1,
                        "compare_region_key": {
                            "$cmp": ["$nation_docs.region_key", "$region_docs.region_key"]
                            # $cmp is a compare operator that stores zero,1,-1 in c_nkTOs_nk field. The numbers mean equal/greater/less than respectively
                        }
                    }
                },
                {"$match": {
                    # "part_doc.size": 17,
                    # "part_doc.type": {"$regex": "BRASS", "$options": "g"},
                    # "region_doc.name": "EUROPE",
                    "compare_region_key": {"$eq": 0}
                }
                },
                {
                    "$sort": {
                        "supplier_docs.acct_bal": -1,
                        "nation_docs.name":1,
                        "supplier_docs.name":1,
                        "part_docs.part_key":1
                    }
                }
            ]

            start_time = time.time()
            result = partsupp_table.aggregate(pipeline)
            print(list(result))
            end_time = time.time()
            print("---------------Query 2-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)