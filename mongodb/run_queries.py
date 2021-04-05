#!/usr/bin/python
from mongodb.query1 import Query1
from mongodb.query3 import Query3


class RunQueries:
    def __init__(self):
        super().__init__()

    def run_queries(self):
        # query1 = Query1()
        # query1.execute()

        query3 = Query3()
        query3.execute()
