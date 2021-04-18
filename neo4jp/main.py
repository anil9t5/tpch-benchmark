# -*- coding: utf-8 -*-
import sys
from neo4jp.initialize_db import InitilizeDB
from neo4jp.run_queries import RunQueries
from neo4jp.insert_data import InsertData
from neo4jp.delete_data import DeleteData


def main():
    print("Neo4j db started...")
    db = InitilizeDB.init()

    insert_data=False
    if insert_data:
        insert_task = InsertData(db)
        insert_task.insert_nodes()
        #insert_task.insert_relations()

    delete_data=True
    if delete_data:
        delete_task =DeleteData(db)
        delete_task.delete_nodes()


    run_queries=False
    if run_queries:
        query = RunQueries()
        query.run_queries()

    print("Successful!")

