#!/usr/bin/python
import psycopg2
from postgresql.config import config


class Tables:

    def __init__(self):
        super().__init__()

    def create_tables(self):
        conn = None
        table_names = ("PART", "SUPPLIER", "PARTSUPP", "CUSTOMER",
                       "ORDERS", "LINEITEM", "NATION", "REGION")

        try:

            # read the connection parameters
            params = config()

            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)

            # create a cursor to connect to database only once..
            cur = conn.cursor()

            # Drop table each time to prevent duplicate tables...
            for table_name in table_names:
                cur.execute("DROP TABLE IF EXISTS {};".format(table_name))

            # Create tables...
            """ create tables in the PostgreSQL database"""
            commands = (
                """
                CREATE TABLE IF NOT EXISTS PART(
                    P_PARTKEY BIGINT NOT NULL PRIMARY KEY,
                    P_NAME VARCHAR(55),
                    P_MFGR CHAR(25),
                    P_BRAND CHAR(10),
                    P_TYPE VARCHAR(25),
                    P_SIZE INTEGER,
                    P_CONTAINER CHAR(10),
                    P_RETAILPRICE DECIMAL(12,2),
                    P_COMMENT VARCHAR(23)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS SUPPLIER(
                    S_SUPPKEY BIGINT NOT NULL PRIMARY KEY,
                    S_NAME CHAR(25),
                    S_ADDRESS VARCHAR(40),
                    S_NATIONKEY INTEGER,
                    S_PHONE CHAR(15),
                    S_ACCTBAL DECIMAL(12,2),
                    S_COMMENT VARCHAR(101)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS PARTSUPP(
                    PS_PARTKEY BIGINT NOT NULL,
                    PS_SUPPKEY BIGINT NOT NULL,
                    PS_AVAILQTY INTEGER,
                    PS_SUPPLYCOST DECIMAL(12,2),
                    PS_COMMENT VARCHAR(199),
                    PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS CUSTOMER(
                    C_CUSTKEY BIGINT NOT NULL PRIMARY KEY,
                    C_NAME VARCHAR(25),
                    C_ADDRESS VARCHAR(40),
                    C_NATIONKEY BIGINT,
                    C_PHONE CHAR(15),
                    C_ACCTBAL DECIMAL(12,2),
                    C_MKTSEGMENT CHAR(10),
                    C_COMMENT VARCHAR(117)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS ORDERS(
                    O_ORDERKEY BIGINT NOT NULL PRIMARY KEY,
                    O_CUSTKEY BIGINT,
                    O_ORDERSTATUS CHAR(1),
                    O_TOTALPRICE DECIMAL(12,2),
                    O_ORDERDATE DATE,
                    O_ORDERPRIORITY CHAR(15),
                    O_CLERK CHAR(15),
                    O_SHIPPRIORITY INTEGER,
                    O_COMMENT VARCHAR(79)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS LINEITEM(
                    L_ORDERKEY BIGINT NOT NULL,
                    L_PARTKEY BIGINT,
                    L_SUPPKEY BIGINT,
                    L_LINENUMBER INTEGER NOT NULL,
                    L_QUANTITY DECIMAL(12,2),
                    L_EXTENDEDPRICE DECIMAL(12,2),
                    L_DISCOUNT DECIMAL(12,2),
                    L_TAX DECIMAL(12,2),
                    L_RETURNFLAG CHAR(1),
                    L_LINESTATUS CHAR(1),
                    L_SHIPDATE DATE,
                    L_COMMITDATE DATE,
                    L_RECEIPTDATE DATE,
                    L_SHIPINSTRUCT CHAR(25),
                    L_SHIPMODE CHAR(10),
                    L_COMMENT VARCHAR(44),
                    PRIMARY KEY (L_ORDERKEY,L_LINENUMBER)
                )
                """,
                """
                CREATE TABLE IF NOT EXISTS NATION(
                    N_NATIONKEY BIGINT NOT NULL PRIMARY KEY,
                    N_NAME CHAR(25),
                    N_REGIONKEY BIGINT,
                    N_COMMENT VARCHAR(152)
                )
                """,
                """
                CREATE TABLE REGION(
                    R_REGIONKEY BIGINT NOT NULL PRIMARY KEY,
                    R_NAME CHAR(25),
                    R_COMMENT VARCHAR(152)
                )
                """)
            for command in commands:
                cur.execute(command)
            cur.close()
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()
