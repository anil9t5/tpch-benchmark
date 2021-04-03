#!/usr/bin/python
import psycopg2
import time
from postgresql.config import config
from postgresql.query1 import Query1
from postgresql.query2 import Query2

class RunQueries:
    def __init__(self):
        super().__init__()

    def run_queries(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)

            q1= Query1(conn)
            q1.execute()

            q2=Query2(conn)
            q2.execute()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()