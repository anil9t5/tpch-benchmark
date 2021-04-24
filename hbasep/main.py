# -*- coding: utf-8 -*-
from hbasep.insert_data_csv import InsertDataCsv
from hbasep.tables import Tables
from hbasep.run_queries import RunQueries

def main():
    print("HBase db started...")

    #Create tables
    create_table=True
    if create_table:
        task = Tables()
        task.create_tables()

    # Insert data
    insert_data = True
    if insert_data:
        insert_task = InsertDataCsv(0.5)
        insert_task.insert_to_tables()

    # Queries
    run_queries = True
    if run_queries:
        query = RunQueries()
        query.run_queries()

    print("Successful!")
