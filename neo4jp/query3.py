from neo4jp.initialize_db import InitilizeDB
import time


class Query3:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            graphDB= InitilizeDB.init()
            start_time = time.time()

            result =graphDB.run(
                "MATCH (lineitem:LINEITEM)-[:IS_PART_OF]->(order:ORDER)-[:MADE_BY]->(customer:CUSTOMER) "
                "WHERE customer.C_MKTSEGMENT = 'BUILDING' AND date(order.O_ORDERDATE) < date('1995-03-15') AND date(lineitem.L_SHIPDATE) > date('1995-03-15') "
                "RETURN order.id, sum(lineitem.L_EXTENDEDPRICE*(1-lineitem.L_DISCOUNT)) AS REVENUE, order.O_ORDERDATE, order.O_SHIPPRIORITY "
                "ORDER BY REVENUE DESC, order.O_ORDERDATE "
                "LIMIT 10; ")

            print(list(result))
            end_time = time.time()
            print("---------------Query 3-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))
        except:
            print("py2neo ERROR:")

