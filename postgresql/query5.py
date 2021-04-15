import psycopg2
import time
import random
from datetime import datetime, timedelta

class Query5:
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def execute(self):
        try:
            cur = self.conn.cursor()

            regions = ["AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"]
            random_region = regions[random.randint(0,4)]

            years = ["1993", "1994", "1995", "1996", "1997"]
            random_date = '"' + str(years[random.randint(0, 4)]) + '-1-'+str(random.randint(1,31))+'"'


            #Query Validation:
            # random_region ="ASIA"
            # random_date ="1994-1-1"

            command = '''select
                        n_name,
                        sum(l_extendedprice * (1 - l_discount)) as revenue
                        from
                        customer,
                        orders,
                        lineitem,
                        supplier,
                        nation,
                        region
                        where
                        c_custkey = o_custkey
                        and l_orderkey = o_orderkey
                        and l_suppkey = s_suppkey
                        and c_nationkey = s_nationkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = '{0}'
                        and o_orderdate >= date '{1}'
                        and o_orderdate < date '{1}' + interval '1' year
                        group by
                        n_name
                        order by
                        revenue desc;'''.format(random_region,random_date)

            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            self.conn.commit()
            te = time.time()
            print("---------------Query 5-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            print(resultAll)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
