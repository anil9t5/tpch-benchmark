#!/usr/bin/python
import psycopg2
import time
from postgresql.config import config
from postgresql.query1 import Query1
from postgresql.query2 import Query2
from postgresql.query3 import Query3
from postgresql.query4 import Query4
from postgresql.query5 import Query5

class RunQueries:
    def __init__(self):
        super().__init__()

    def run_queries(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)

            # q1= Query1(conn)
            # q1.execute()

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



        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()