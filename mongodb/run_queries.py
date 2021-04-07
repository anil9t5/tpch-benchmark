#!/usr/bin/python
from mongodb.query1 import Query1
from mongodb.query3 import Query3
from mongodb.query5 import Query5


class RunQueries:
    def __init__(self):
        super().__init__()

    def run_queries(self):
        # query1 = Query1()
        # query1.execute()

        # query3 = Query3()
        # query3.execute()

        query5 = Query5()
        query5.execute()

