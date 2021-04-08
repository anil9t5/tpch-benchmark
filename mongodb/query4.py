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

            all_years = [1993, 1994, 1995, 1996, 1997]
            full_years = [1993, 1994, 1995, 1996]
            random_month = random.randint(1, 12)

            #datetime.datetime(1994, 5, 9, 0, 0)
            if random_month > 10:
                random_date = datetime.datetime(full_years[random.randint(0, 3)], random_month, 1, 0, 0)
            else:
                random_date = datetime.datetime(all_years[random.randint(0, 4)], random_month, 1, 0, 0)

            pipeline = [

            ]

            start_time = time.time()
            result = lineitem_table.aggregate(pipeline)
            pprint.pprint(list(result))
            end_time = time.time()
            print("---------------Query 4-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))

        except errors.ServerSelectionTimeoutError as err:
            print("pymongo ERROR:", err)


#---------------------Crashes
# {"$lookup": {
#                     "from": "supplier",
#                     "let": {"nationkey": "$customer_docs.nation_key", "supkey": "$lineitem_docs.l_suppkey"},
#                     "pipeline": [
#                         {"$match":
#                             {"$expr":
#                                 {"$and":
#                                     [
#                                         {"$eq": ["$nation_key", "$$nationkey"]},
#                                         {"$eq": ["$supplier_key", "$$supkey"]}
#                                     ]
#                                 }
#                             }
#                         }
#                     ],
#                     "as": "supplier_docs"
#                     }
#                 },
#                 {
#                     "$unwind": "$supplier_docs"
#                 }

#---------Separate fields
# {"$lookup": {
#     "from": "supplier",
#     "localField": "customer_docs.nation_key",
#     "foreignField": "nation_key",
#     "as": "supplierN_docs"
# }},
# {
#     "$unwind": "$supplierN_docs"
# }
# ,
# {"$lookup": {
#     "from": "supplier",
#     "localField": "lineitem_docs.l_suppkey",
#     "foreignField": "supplier_key",
#     "as": "supplierS_docs"
# }},
# {
#     "$unwind": "$supplierS_docs"
# },


#--------------------Unwined and match
# ,
#                 {"$lookup": {
#                     "from": "supplier",
#                     "localField": "customer_docs.nation_key",
#                     "foreignField": "nation_key",
#                     "as": "supplierN_docs"
#                 }},
#                 {
#                     "$unwind": "$supplier_docs"
#                 },
#                 {"$match":
#                      {"lineitem_docs.l_suppkey":"supplier_docs.supplier_key"}
#                 }





# {
#                     {"$lookup": {
#                         "from": "supplier",
#                         "let": {"sNationKey": "$nation_key", "sSupKey":"$supplier_key"},
#                         "pipeline": [
#                             {"$match":
#                                  {"$and": [{"customer_docs.nation_key": {"$eq": "sNationKey"}},
#                                            {"order_date": {"$lt": datetime.datetime(1995, 1, 1)}}]
#                                   }
#                             },
#                             {"$lookup": {
#
#                                 }
#
#                             }
#
#                         ]
#
#                         }
#                     }
#                 }




# {"$lookup": {
#                     "from": "supplier",
#                     "localField": "lineitem_docs.l_suppkey",
#                     "foreignField": "supplier_key",
#                     "as": "supplier_docs"
#                 }},
#                 {
#                     "$unwind": "$supplier_docs"
#                 },
#                 {"$match":
#                      {"customer_docs.nation_key":"supplier_docs.nation_key"}
#                 }