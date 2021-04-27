# We joined codes found on  https://github.com/aiquis/tpch-neo4j and added our own to make them work.
from neo4jp.initialize_db import InitilizeDB
import time


class Query1:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            graphDB = InitilizeDB.init()
            start_time = time.time()

            result = graphDB.run(
                "WITH date('1998-12-01') - duration('P90D') AS my_date "
                "MATCH (item:LINEITEM) "
                "WHERE date(item.L_SHIPDATE) <= date(my_date) "
                "RETURN item.L_RETURNFLAG,item.L_LINESTATUS,sum(item.L_QUANTITY) AS sum_qty, sum(item.L_EXTENDEDPRICE) AS sum_base_price, sum(item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT)) AS sum_disc_price,sum(item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT)*(1+item.L_TAX)) AS sum_charge,avg(item.L_QUANTITY) AS avg_qty, avg(item.L_EXTENDEDPRICE) AS avg_price, avg(item.L_DISCOUNT) AS avg_disc, COUNT(*) AS count_order "
                "ORDER BY item.L_RETURNFLAG, item.L_LINESTATUS; ")
            print(result)
            end_time = time.time()
            print("---------------Query 1-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))
        except:
            print("py2neo ERROR:")
