# -*- coding: utf-8 -*-
from insert_data_csv import InsertDataCsv
from insert_data import InsertData
from tables import Tables
from run_queries import RunQueries

def main():
    print("HBase db started...")

    #Create tables
    create_table=True
    if create_table:
        task = Tables()
        task.create_tables()

    # Insert data
    insert_data = False
    read_from_csv = True
    if insert_data:
        if read_from_csv:
            insert_task = InsertDataCsv(1)
            insert_task.insert_to_tables()
        else:
            insert_task = InsertData(0.05)
            insert_task.insert_to_tables()

    # Queries
    run_queries = False
    if run_queries:
        query = RunQueries()
        query.run_queries()

    print("Successful!")
