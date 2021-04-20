import phoenixdb
import psycopg2
import time
import random
from datetime import datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta

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
            random_date = "1993-7-1"
            rd_date = datetime. strptime(random_date, "%Y-%m-%d") + timedelta(days=90)
            rd_date = rd_date.date()

            command = '''select
                        o_orderpriority,
                        count(*) as order_count
                        from
                        orders
                        where
                        o_orderdate >= date '{0}'
                        and o_orderdate < date '{0}' 
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
                        o_orderpriority'''.format(rd_date)

            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            te = time.time()
            print("---------------Query 4-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            print(resultAll)

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)


