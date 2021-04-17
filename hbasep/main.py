# -*- coding: utf-8 -*-
import phoenixdb
import phoenixdb.cursor
from insert_data_csv import InsertDataCsv;
from tables import Tables;
from run_queries import RunQueries;

def main():
    print("HBase db started...")

    task = Tables()
    task.create_tables()

    insert_task = InsertDataCsv(1)
    insert_task.insert_to_tables()

    query = RunQueries()
    query.run_queries()

    print("Successful!")
