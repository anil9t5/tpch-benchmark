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
