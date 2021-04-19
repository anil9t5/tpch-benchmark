import os
import psycopg2
import time
import random
import psycopg2
from postgresql.config import config
import csv
from glob import glob


class ExportDataCsv:
    path = "/home/shermin/Desktop/Projs/BigData/Data/exports/"

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
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)
    #     ExportDataCsv.uppercase_header(self, ExportDataCsv.path + file_name + '.csv')
    #     finally:
    #         if conn is not None:
    #             conn.close()

    @staticmethod
    def export_node_orders(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "orders"
            command = '''COPY (
            SELECT *, split_part(o_orderdate::TEXT,'-',1) AS o_year, split_part(o_orderdate::TEXT,'-',2) AS 
            o_month, split_part(o_orderdate::TEXT,'-',3) AS o_day FROM orders)
            TO '{0}{1}.csv'
            DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
            cur.execute(command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def export_node_lineitem(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "lineitem"
            command = '''COPY (
            SELECT *, split_part(l_shipdate::TEXT,'-',1) AS l_shipdate_year, split_part(l_shipdate::TEXT,'-',2) AS 
            l_shipdate_month, split_part(l_shipdate::TEXT,'-',3) AS l_shipdate_day, split_part(l_commitdate::TEXT,'-',1) AS 
            l_commitdate_year, split_part(l_commitdate::TEXT,'-',2) AS l_commitdate_month, split_part(l_commitdate::TEXT,'-',3) AS 
            l_commitdate_day, split_part(l_receiptdate::TEXT,'-',1) AS l_receiptdate_year, split_part(l_receiptdate::TEXT,'-',2) AS 
            l_receiptdate_month, split_part(l_receiptdate::TEXT,'-',3) AS l_receiptdate_day  FROM lineitem)
            TO '{0}{1}.csv'
            DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
            cur.execute(command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def export_rel_customer_nation(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            file_name = "rel_customer_nation"

            command = '''COPY 
            (SELECT C_CUSTKEY, C_NATIONKEY 
            FROM customer AS c INNER JOIN nation AS n 
            ON c.C_NATIONKEY = n.N_NATIONKEY) 
            TO '{0}{1}.csv' 
            DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, O_CUSTKEY  
                FROM lineitem AS l INNER JOIN orders AS o 
                ON l.L_ORDERKEY = o.O_ORDERKEY) 
                TO '{0}{1}.csv' 
                DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, P_NAME 
                FROM lineitem AS l INNER JOIN part AS p 
                ON l.L_PARTKEY = p.P_PARTKEY) 
                TO '{0}{1}.csv' 
                DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                (SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, PS_PARTKEY, PS_SUPPKEY  
                FROM lineitem AS l 
                INNER JOIN partsupp AS p 
                ON l.L_PARTKEY = p.PS_PARTKEY 
                AND l.L_SUPPKEY = p.PS_SUPPKEY)
                TO '{0}{1}.csv' 
                DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
               (SELECT L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_SUPPKEY  
               FROM lineitem AS l INNER JOIN supplier AS s 
               ON l.L_SUPPKEY = s.S_SUPPKEY) 
               TO '{0}{1}.csv' 
               DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                (SELECT N_NATIONKEY, N_REGIONKEY  
                FROM nation AS n INNER JOIN region AS r 
                ON r.R_REGIONKEY = n.N_REGIONKEY) 
                TO '{0}{1}.csv' 
                DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                    (SELECT S_SUPPKEY, S_NATIONKEY 
                    FROM nation AS n INNER JOIN supplier AS s 
                    ON n.N_NATIONKEY = s.S_NATIONKEY) 
                    TO '{0}{1}.csv' 
                    DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                        (SELECT O_ORDERKEY, O_CUSTKEY 
                        FROM orders AS o INNER JOIN customer AS c 
                        ON o.O_CUSTKEY = c.C_CUSTKEY) 
                        TO '{0}{1}.csv' 
                        DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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
            command = '''COPY 
                            (SELECT P_PARTKEY, P_PARTKEY 
                            FROM part AS p INNER JOIN partsupp AS s 
                            ON p.P_PARTKEY = s.PS_PARTKEY) 
                            TO '{0}{1}.csv' 
                            DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
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

            rel_supplier_partsupp = ExportDataCsv.path + file_name

            command = '''COPY 
                                (SELECT PS_PARTKEY, PS_SUPPKEY 
                                FROM supplier AS s INNER JOIN partsupp AS p 
                                ON s.S_SUPPKEY = p.PS_SUPPKEY) 
                                TO '{0}{1}.csv' 
                                DELIMITER '|' CSV HEADER;'''.format(ExportDataCsv.path, file_name)
            cur.execute(command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

    def uppercase_header(self, path):
        for fname in glob(path+'*'):
            with open(fname, newline='') as f:
                reader = csv.reader(f)
                header = next(reader)
                header = [column.upper() for column in header]
                rows = [header] + list(reader)
            with open(fname, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(rows)

    def export_to_csv(self):
        # ExportDataCsv.export_rel_customer_nation(self)
        # ExportDataCsv.export_rel_lineitem_part(self)
        # ExportDataCsv.export_rel_lineitem_partsupp(self)
        # ExportDataCsv.export_rel_lineitem_supplier(self)
        # ExportDataCsv.export_rel_nation_region(self)
        # ExportDataCsv.export_rel_nation_supplier(self)
        # ExportDataCsv.export_rel_orders_customer(self)
        # ExportDataCsv.export_rel_part_partsupp(self)
        # ExportDataCsv.export_rel_supplier_partsupp(self)

        # ExportDataCsv.export_rel_lineitem_orders(self)
        # ExportDataCsv.export_node_lineitem(self)
        # ExportDataCsv.export_node_orders(self)

        os.system("sudo chmod -R a+rwx {0}*.csv".format(ExportDataCsv.path))
        ExportDataCsv.uppercase_header(self, ExportDataCsv.path)
