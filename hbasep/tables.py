#!/usr/bin/python
import phoenixdb
from hbasep.config import config


class Tables:

    def __init__(self):
        super().__init__()

    def create_tables(self):
        conn = None
        table_names = ("PART", "SUPPLIER", "PARTSUPP", "CUSTOMER",
                       "ORDERS", "LINEITEM", "NATION", "REGION")

        try:
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()

            # Empty database by dropping tables
            for table_name in table_names:
                cur.execute("DROP TABLE IF EXISTS {0}".format(table_name))

            table_PART=""" CREATE TABLE PART(
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
                            """
            cur.execute(table_PART)

            table_SUPPLIER = """ CREATE TABLE SUPPLIER(
                    S_SUPPKEY BIGINT NOT NULL PRIMARY KEY,
                    S_NAME CHAR(25),
                    S_ADDRESS VARCHAR(40),
                    S_NATIONKEY INTEGER,
                    S_PHONE CHAR(15),
                    S_ACCTBAL DECIMAL(12,2),
                    S_COMMENT VARCHAR(101)
                )
            """
            cur.execute(table_SUPPLIER)

            table_PARTSUPP = """ CREATE TABLE PARTSUPP(
                    PS_PARTKEY BIGINT NOT NULL,
                    PS_SUPPKEY BIGINT NOT NULL,
                    PS_AVAILQTY INTEGER,
                    PS_SUPPLYCOST DECIMAL(12,2),
                    PS_COMMENT VARCHAR(199),
                    CONSTRAINT PK PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
                 )
                """
            cur.execute(table_PARTSUPP)

            table_CUSTOMER = """ CREATE TABLE CUSTOMER(
                    C_CUSTKEY BIGINT NOT NULL PRIMARY KEY,
                    C_NAME VARCHAR(25),
                    C_ADDRESS VARCHAR(40),
                    C_NATIONKEY BIGINT,
                    C_PHONE CHAR(15),
                    C_ACCTBAL DECIMAL(12,2),
                    C_MKTSEGMENT CHAR(10),
                    C_COMMENT VARCHAR(117)
                )
                """
            cur.execute(table_CUSTOMER)

            table_ORDERS = """ CREATE TABLE ORDERS(
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
            """
            cur.execute(table_ORDERS)

            table_LINEITEM = """ CREATE TABLE LINEITEM(
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
                    CONSTRAINT PK PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
                )
                """
            cur.execute(table_LINEITEM)

            table_NATION = """ CREATE TABLE NATION(
                    N_NATIONKEY BIGINT NOT NULL PRIMARY KEY,
                    N_NAME CHAR(25),
                    N_REGIONKEY BIGINT,
                    N_COMMENT VARCHAR(152)
                )
                        """
            cur.execute(table_NATION)

            table_REGION = """ CREATE TABLE REGION(
                    R_REGIONKEY BIGINT NOT NULL PRIMARY KEY,
                    R_NAME CHAR(25),
                    R_COMMENT VARCHAR(152)
                )
                """
            cur.execute(table_REGION)

            cur.close()

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)


