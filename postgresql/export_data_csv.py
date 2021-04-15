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


    @staticmethod
    def export_rel_lineitem_partsupp(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_lineitem_partsupp"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, PS_PARTKEY, PS_SUPPKEY  
                FROM lineitem AS l 
                INNER JOIN partsupp AS p 
                ON l.L_PARTKEY = p.PS_PARTKEY 
                INNER JOIN partsupp AS s
                ON l.L_SUPPKEY = s.PS_SUPPKEY
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


    @staticmethod
    def export_rel_lineitem_supplier(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_lineitem_supplier"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
               (SELECT L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_SUPPKEY  
               FROM lineitem AS l INNER JOIN supplier AS s 
               ON l.L_SUPPKEY = s.S_SUPPKEY) 
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


    @staticmethod
    def export_rel_nation_region(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_nation_region"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                (SELECT N_NATIONKEY, N_REGIONKEY  
                FROM nation AS n INNER JOIN region AS r 
                ON r.R_REGIONKEY = n.N_REGIONKEY) 
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


    @staticmethod
    def export_rel_nation_supplier(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_nation_supplier"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                    (SELECT S_SUPPKEY, S_NATIONKEY 
                    FROM nation AS n INNER JOIN supplier AS s 
                    ON n.N_NATIONKEY = s.S_NATIONKEY) 
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


    @staticmethod
    def export_rel_orders_customer(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_orders_customer"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                        (SELECT O_ORDERKEY, O_CUSTKEY 
                        FROM orders AS o INNER JOIN customer AS c 
                        ON o.O_CUSTKEY = c.C_CUSTKEY) 
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


    @staticmethod
    def export_rel_part_partsupp(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_part_partsupp"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                            (SELECT P_PARTKEY, P_PARTKEY 
                            FROM part AS p INNER JOIN partsupp AS s 
                            ON p.P_PARTKEY = s.PS_PARTKEY) 
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


    @staticmethod
    def export_rel_supplier_partsupp(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_supplier_partsupp"
            path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"
            command = '''COPY 
                                (SELECT PS_PARTKEY, PS_SUPPKEY 
                                FROM supplier AS s INNER JOIN partsupp AS p 
                                ON s.S_SUPPKEY = p.PS_SUPPKEY) 
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