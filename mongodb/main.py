# -*- coding: utf-8 -*-
import sys
from mongodb.collections import Collections
from mongodb.insert_data_csv import InsertDataCsv
from mongodb.run_queries import RunQueries
from mongodb.initialize_db import InitilizeDB


def main():
    print("Mongo db started...")
    db = InitilizeDB.init()
    create_collections = False
    if create_collections:
        task = Collections(db)
        task.create_collections()

        insert_task = InsertDataCsv(0.05, db)
        insert_task.insert_to_collections()
    else:
        query = RunQueries()
        query.run_queries()

    print("Successful!")
