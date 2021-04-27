# We joined codes found on  https://github.com/aiquis/tpch-neo4j and added our own to make them work.
from neo4jp.initialize_db import InitilizeDB
import time


class Query4:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            graphDB = InitilizeDB.init()
            start_time = time.time()

            result = graphDB.run(
                "WITH date('1993-07-01') + duration('P3M') AS my_date "
                "MATCH (lineitem: LINEITEM)-[:BELONGS_TO_7]->(order: ORDER) "
                "WHERE  date(lineitem.L_COMMITDATE) < date(lineitem.L_RECEIPTDATE) AND date(order.O_ORDERDATE) >= date('1993-07-01') AND date(order.O_ORDERDATE) < date(my_date) "
                "RETURN order.O_ORDERPRIORITY, COUNT(*) AS ORDER_COUNT "
                "ORDER BY order.O_ORDERPRIORITY; ")

            print(result)
            end_time = time.time()
            print("---------------Query 4-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))
        except:
            print("py2neo ERROR:")
