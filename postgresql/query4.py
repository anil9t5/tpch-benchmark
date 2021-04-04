import psycopg2
import time
import random
from datetime import datetime, timedelta

class Query4:
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def execute(self):
        try:
            cur = self.conn.cursor()

            all_years=["1993","1994","1995","1996","1997"]
            full_years = ["1993", "1994", "1995", "1996"]
            random_month=random.randint(1,12)
            if random_month>10:
                random_date='"'+full_years[random.randint(0,3)]+'-'+str(random_month)+'-1"'
            else:
                random_date = '"' + all_years[random.randint(0,4)] + '-' + str(random_month) + '-1"'

            # #Query Validation:
            # random_date = "1993-7-1"

            command = '''select
                        o_orderpriority,
                        count(*) as order_count
                        from
                        orders
                        where
                        o_orderdate >= date '{0}'
                        and o_orderdate < date '{0}' + interval '3' month
                        and exists (
                        select
                        *
                        from
                        lineitem
                        where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
                        )
                        group by
                        o_orderpriority
                        order by
                        o_orderpriority;'''.format(random_date)

            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            self.conn.commit()
            te = time.time()
            print("---------------Query 4-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            #print(resultAll)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
