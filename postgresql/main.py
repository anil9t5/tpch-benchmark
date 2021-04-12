# -*- coding: utf-8 -*-
from postgresql.tables import Tables
from postgresql.insert_data import InsertData
from postgresql.insert_data_csv import InsertDataCsv
from postgresql.run_queries import RunQueries

def main():
    print("PostgreSQL db started...")
    create_table=True
    if create_table:
        task = Tables()
        task.create_tables()

    insert_data =True
    read_from_csv=True
    if insert_data:
        if read_from_csv:
            insert_task=InsertDataCsv(1)
            insert_task.insert_to_tables()
        else:
            insert_task = InsertData(0.05)
            insert_task.insert_to_tables()

    run_queries=False
    if run_queries:
        query = RunQueries()
        query.run_queries()

    print("Successful!")
