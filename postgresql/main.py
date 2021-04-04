# -*- coding: utf-8 -*-
from postgresql.tables import Tables
from postgresql.insert_data import InsertData
from postgresql.run_queries import RunQueries

def main():
    print("PostgreSQL db started...")

    task = Tables()
    task.create_tables()
    insert_task = InsertData(0.05)
    insert_task.insert_to_tables()

    query = RunQueries()
    query.run_queries()

    print("Successful!")
