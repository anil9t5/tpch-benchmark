import random
from random import choice
from string import ascii_lowercase
import phoenixdb
from hbasep.config import config
from decimal import Decimal
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
        try:
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path + 'part.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT into PART (P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT) "
                        "VALUES (?,?,?,?,?,?,?,?,?)", (int(row[0]), row[1], row[2], row[3], row[4], int(
                            row[5]), row[6], float(row[7]), row[8])

                    )
            cur.close()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

    @staticmethod
    def insert_SUPPLIER(self):
        conn = None
        try:
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path + 'supplier.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO SUPPLIER(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,  S_PHONE,  S_ACCTBAL,  S_COMMENT) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)", (int(row[0]), row[1], row[2], int(
                            row[3]), row[4], float(row[5]), row[6])
                    )
            cur.close()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

    # --------------------
    @staticmethod
    def insert_PARTSUPP(self):
        try:
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'partsupp.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (int(row[0]), int(row[1]), int(
                            row[2]), float(row[3]), row[4])
                    )
            cur.close()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

    @staticmethod
    def insert_CUSTOMER(self):
        conn = None
        try:
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'customer.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO CUSTOMER (C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY,C_PHONE,C_ACCTBAL, C_MKTSEGMENT,C_COMMENT) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (int(row[0]), row[1], row[2], int(row[3]),
                         row[4], float(row[5]), row[6], row[7])
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
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path + 'orders.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (int(row[0]), int(row[1]), row[2], float(row[3]),
                         row[4], row[5], row[6], int(row[7]), row[8])
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
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()

            with open(InsertDataCsv.csv_path + 'lineitem.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO LINEITEM(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (int(row[0]), int(row[1]), int(row[2]), int(row[3]),
                         float(row[4]), float(row[5]), float(row[6]), float(row[7]), row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15])
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
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'nation.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO NATION(N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT) "
                        "VALUES (?, ?, ?, ?)",
                        (int(row[0]), row[1], int(row[2]), row[3])
                    )
            cur.close()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

    @staticmethod
    def insert_REGION(self):
        conn = None
        try:
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()
            with open(InsertDataCsv.csv_path+'region.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "UPSERT INTO REGION(R_REGIONKEY,R_NAME,R_COMMENT) "
                        "VALUES (?, ?, ?)",
                        (int(row[0]), row[1], row[2])
                    )
            cur.close()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def insert_to_tables(self):
        # InsertDataCsv.insert_PART(self)
        # InsertDataCsv.insert_SUPPLIER(self)
        InsertDataCsv.insert_PARTSUPP(self)
        # InsertDataCsv.insert_CUSTOMER(self)
        # InsertDataCsv.insert_ORDERS(self)
        # InsertDataCsv.insert_LINEITEM(self)
        # InsertDataCsv.insert_NATION(self)
        # InsertDataCsv.insert_REGION(self)
