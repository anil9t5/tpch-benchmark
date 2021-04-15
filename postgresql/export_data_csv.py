import psycopg2
import time
import random
import psycopg2
from postgresql.config import config
import csv

class ExportDataCsv:
    def __init__(self):
        super().__init__()


    # @staticmethod
    # def export_CUSTOMER(self):
    #     conn = None
    #     try:
    #         params = config()
    #         conn = psycopg2.connect(**params)
    #         cur = conn.cursor()
    #
    #         command = '''COPY (
    #         SELECT * FROM customer)
    #         TO '/home/shermin/Desktop/Projs/BigData/Data/exports/{0}.csv'
    #         DELIMITER '|' CSV HEADER;'''.format("customer_exp")
    #         cur.execute(command)
    #         cur.close()
    #         conn.commit()
    #
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)
    #
    #     finally:
    #         if conn is not None:
    #             conn.close()

    @staticmethod
    def export_rel_customer_nation(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name="rel_customer_nation"
            path="/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
            (SELECT C_CUSTKEY, C_NATIONKEY  
            FROM customer AS c INNER JOIN nation AS n 
            ON c.C_NATIONKEY = n.N_NATIONKEY) 
            TO '{0}{1}.csv' 
            DELIMITER '|' CSV HEADER;'''.format(path,file_name)
            print(command)
            cur.execute(command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def export_rel_lineitem_orders(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_lineitem_orders"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, O_CUSTKEY  
                FROM lineitem AS l INNER JOIN orders AS o 
                ON l.L_ORDERKEY = o.O_ORDERKEY) 
                TO '{0}{1}.csv' 
                DELIMITER '|' CSV HEADER;'''.format(path, file_name)
            print(command)
            cur.execute(command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

#--------------
    @staticmethod
    def export_rel_lineitem_part(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_lineitem_part"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, P_NAME 
                FROM lineitem AS l INNER JOIN part AS p 
                ON l.L_PARTKEY = p.P_PARTKEY) 
                TO '{0}{1}.csv' 
                DELIMITER '|' CSV HEADER;'''.format(path, file_name)
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