from neo4jp.initialize_db import InitilizeDB
import time


class Query5:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            graphDB = InitilizeDB.init()
            start_time = time.time()

            result=graphDB.run(
                "WITH date('1994-01-01') + duration('P1Y') AS my_date "
                "MATCH (lineitem: LINEITEM)-[:BELONGS_TO_7]->(order: ORDER)-[:BY_5]->(customer: CUSTOMER)-[:FROM_4]->(nation: NATION)-[:FROM_10]-(region: REGION) "
                "WHERE region.R_NAME = 'ASIA' AND date(order.O_ORDERDATE) >= date('1994-01-01') AND date(order.O_ORDERDATE) < date(my_date) "
                "RETURN nation.N_NAME, SUM(lineitem.L_EXTENDEDPRICE * (1 - lineitem.L_DISCOUNT)) AS REVENUE "
                "ORDER BY REVENUE DESC; ")

            print(result)
            end_time = time.time()
            print("---------------Query 5-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))
        except:
            print("py2neo ERROR:")
