import psycopg2
import time
import random
import psycopg2
from postgresql.config import config
import csv

class ExportDataCsv:
    def __init__(self):
        super().__init__()


    @staticmethod
    def export_CUSTOMER(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()

            command = '''COPY (
            SELECT * FROM customer) 
            TO '/home/shermin/Desktop/Projs/BigData/Data/exports/{0}.csv' 
            DELIMITER '|' CSV HEADER;'''.format("customer_exp")
            cur.execute(command)
            cur.close()
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def export_CUSTOMER_NATION_NATIONKEY(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()

            command = '''COPY 
            (SELECT C_CUSTKEY, C_NATIONKEY  
            FROM customer AS c INNER JOIN nation AS n 
            ON c.C_NATIONKEY = n.N_NATIONKEY) 
            TO '/home/shermin/Desktop/Projs/BigData/Data/exports/{0}.csv' 
            DELIMITER '|' CSV HEADER;'''.format("customer_nation_nationkey_relation")
            print(command)
            cur.execute(command)
            cur.close()
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()


    def export_to_csv(self):
        # ExportDataCsv.export_CUSTOMER(self)
        ExportDataCsv.export_CUSTOMER_NATION_NATIONKEY(self)
        # ExportDataCsv.export_PART(self)
        # ExportDataCsv.export_SUPPLIER(self)
        # ExportDataCsv.export_PARTSUPP(self)

        # ExportDataCsv.export_ORDERS_LINEITEM(self)
        # ExportDataCsv.export_NATION(self)
        # ExportDataCsv.export_REGION(self)