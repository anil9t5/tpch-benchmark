# -*- coding: utf-8 -*-
from postgresql.tables import Tables
from postgresql.insert_data import InsertData
from postgresql.insert_data_csv import InsertDataCsv
from postgresql.run_queries import RunQueries
from postgresql.export_data_csv import ExportDataCsv

def main():
    print("PostgreSQL db started...")
    create_table=False
    if create_table:
        task = Tables()
        task.create_tables()

    insert_data = False
    read_from_csv = True
    if insert_data:
        if read_from_csv:
            insert_task=InsertDataCsv(1)
            insert_task.insert_to_tables()
        else:
            insert_task = InsertData(0.05)
            insert_task.insert_to_tables()

    run_queries=True
    if run_queries:
        query = RunQueries()
        query.run_queries()

    export_csv_data=False
    if export_csv_data:
        export_task = ExportDataCsv()
        export_task.export_to_csv()



    print("Successful!")
