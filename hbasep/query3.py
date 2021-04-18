import phoenixdb
import psycopg2
import time
from datetime import datetime, timedelta
import random

class Query3:
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def execute(self):
        try:
            cur = self.conn.cursor()
            segment = ["AUTOMOBILE", "BUILDING","FURNITURE","MACHINERY","HOUSEHOLD"]

            start_date = datetime.strptime("1995-3-1", "%Y-%m-%d")
            end_date = datetime.strptime("1995-3-31", "%Y-%m-%d")
            time_between_dates = end_date - start_date
            days_between_dates = time_between_dates.days
            random_number_of_days = random.randrange(days_between_dates)
            random_date = start_date + timedelta(days=random_number_of_days)
            random_segment = segment[random.randint(0, 4)]

            # #Query Validation:
            random_segment ="BUILDING"
            random_date ="1995-03-15"

            command = '''select
                        l_orderkey,
                        sum(l_extendedprice*(1-l_discount)) as revenue,
                        o_orderdate,
                        o_shippriority
                        from
                        customer,
                        orders,
                        lineitem
                        where
                        c_mktsegment = '{0}'
                        and c_custkey = o_custkey
                        and l_orderkey = o_orderkey
                        and o_orderdate < date '{1}'
                        and l_shipdate > date '{1}'
                        group by
                        l_orderkey,
                        o_orderdate,
                        o_shippriority
                        order by
                        revenue desc,
                        o_orderdate'''.format(random_segment, random_date)

            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            te = time.time()
            print("---------------Query 3-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            print(resultAll)

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)