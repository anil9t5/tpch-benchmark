# -*- coding: utf-8 -*-
import sys
from postgresql.tables import Tables
from postgresql.insert_data import InsertData
import psycopg2


def main():
    print("PostgreSQL db started...")
    task = Tables()
    task.create_tables()
    insert_task = InsertData(0.05)
    print("Successful!")
