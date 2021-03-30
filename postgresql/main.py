# -*- coding: utf-8 -*-
import sys
from postgresql.tables import Tables
import psycopg2



def main():
    print("PostgreSQL db started...")
    print("List of argument strings from postgresql module: %s" % sys.argv[1:])
    task = Tables()
    task.drop_tables()
    task.create_tables()
    print("Successful!")