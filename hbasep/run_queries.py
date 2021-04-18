#!/usr/bin/python
import phoenixdb
import time
from hbasep.config import config
from hbasep.query1 import Query1
from hbasep.query2 import Query2
from hbasep.query3 import Query3
from hbasep.query4 import Query4
from hbasep.query5 import Query5

class RunQueries:
    def __init__(self):
        super().__init__()

    def run_queries(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            q1= Query1(conn)
            q1.execute()

            # q2=Query2(conn)
            # q2.execute()
            #
            # q3 = Query3(conn)
            # q3.exescute()
            #
            # q4 = Query4(conn)
            # q4.execute()
            #
            # q5 = Query5(conn)
            # q5.execute()



        except:
            print("Error run_queries")
        finally:
            if conn is not None:
                conn.close()