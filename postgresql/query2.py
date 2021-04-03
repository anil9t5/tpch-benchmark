import psycopg2
import time
import random
from postgresql.config import config

class Query2:
    def __init__(self, conn):
        super().__init__()
        self.conn = conn
    def execute(self):
        try:
            cur = self.conn.cursor()
            # DELTA is randomly selected within [60. 120]
            random_day = random.randint(60, 120)
            # DELTA is set to 90 for validation
            random_day = 90

            command = '''select count(*) from LINEITEM;'''.format(random_day)
            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            self.conn.commit()
            te = time.time()
            print("---------------Query 2-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            print(resultAll)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)