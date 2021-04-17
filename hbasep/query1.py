import time
import random

class Query1:
    def __init__(self, conn):
        super().__init__()
        self.conn = conn

    def execute(self):
        try:
            cur = self.conn.cursor()
            random_day =random.randint(60,120)
            # #Query Validation:
            # random_day =90

            command = '''select l_returnflag, l_linestatus,
                                sum(l_quantity) as sum_qty,
                                sum(l_extendedprice) as sum_base_price,
                                sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
                                sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
                                avg(l_quantity) as avg_qty,
                                avg(l_extendedprice) as avg_price,
                                avg(l_discount) as avg_disc,
                                count(*) as count_order
                                from 
                                lineitem
                                where
                                l_shipdate <= date '1998-12-01' - interval '{0}' day
                                group by
                                l_returnflag,
                                l_linestatus
                                order by
                                l_returnflag,
                                l_linestatus;'''.format(random_day)
            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            self.conn.commit()
            te = time.time()
            print("---------------Query 1-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            print(resultAll)

        except:
            print("query1")
