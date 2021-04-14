# -*- coding: utf-8 -*-
import sys
from neo4jp.initialize_db import InitilizeDB
from neo4jp.run_queries import RunQueries
from neo4jp.insert_data import InsertData

def main():
    print("Neo4j db started...")
    db = InitilizeDB.init()


    #db.run()
    #session = db.session()
    #session.close()

    insert_task = InsertData(db)
    insert_task.insert_nodes()
    insert_task.insert_relations()

    # query = RunQueries()
    # query.run_queries()

    print("Successful!")

