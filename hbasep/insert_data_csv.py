import random
from random import choice
from string import ascii_lowercase
import phoenixdb
from postgresql.config import config
import csv
from datetime import datetime, timedelta

class InsertDataCsv:

    csv_path = "/home/shermin/Desktop/Projs/BigData/Data/datacsv/"
    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor
        self.retail_price_part_table = {}


    @staticmethod
    def insert_PART(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path + 'part.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_SUPPLIER(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path + 'supplier.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO SUPPLIER(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,  S_PHONE,  S_ACCTBAL,  S_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_PARTSUPP(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'partsupp.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES (%s, %s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_CUSTOMER(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'customer.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY,C_PHONE,C_ACCTBAL, C_MKTSEGMENT,C_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_ORDERS(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path + 'orders.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_LINEITEM(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            with open(InsertDataCsv.csv_path + 'lineitem.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO LINEITEM(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_NATION(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'nation.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO NATION(N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT) VALUES (%s, %s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_REGION(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'region.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO REGION(R_REGIONKEY,R_NAME,R_COMMENT) VALUES (%s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()



    def insert_to_tables(self):
        InsertDataCsv.insert_PART(self)
        InsertDataCsv.insert_SUPPLIER(self)
        InsertDataCsv.insert_PARTSUPP(self)
        InsertDataCsv.insert_CUSTOMER(self)
        InsertDataCsv.insert_ORDERS(self)
        InsertDataCsv.insert_LINEITEM(self)
        InsertDataCsv.insert_NATION(self)
        InsertDataCsv.insert_REGION(self)



