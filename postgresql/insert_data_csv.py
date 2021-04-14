import random
from random import choice
from string import ascii_lowercase
import psycopg2
from postgresql.config import config
import csv
from datetime import datetime, timedelta

class InsertDataCsv:

    csv_path = "/home/shermin/Desktop/Projs/BigData/Data/datacsv/"
    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor
        self.retail_price_part_table = {}

    # #---------------------------------
    # @staticmethod
    # def insert_PART(self):
    #     conn = None
    #     try:
    #
    #         params = config()
    #         conn = psycopg2.connect(**params)
    #         cur = conn.cursor()
    #
    #         data_size = int(self.scale_factor * 200000)
    #
    #         keys=list(range(data_size))
    #         random.shuffle(keys)
    #
    #         part_names = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
    #                       "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
    #                       "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
    #                       "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
    #                       "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
    #                       "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
    #                       "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
    #                       "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell",
    #                       "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise",
    #                       "violet", "wheat", "white", "yellow"]
    #
    #         len_part_names = len(part_names)
    #
    #         types_syllable_1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    #         types_syllable_2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
    #         types_syllable_3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
    #
    #         containers_syllable_1 = ["SM", "LG", "MED", "JUMBO", "WRAP"]
    #         containers_syllable_2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]
    #
    #
    #         for i in range(data_size):
    #
    #             P_PARTKEY= keys[i]
    #
    #             random_names=[]
    #             while (len(random_names)<5):
    #                 random_index = random.randint(0,(len_part_names-1))
    #                 if (not random_names.__contains__(part_names[random_index])):
    #                     random_names.append(part_names[random_index])
    #             space_char=" "
    #             P_NAME = space_char.join(random_names)
    #
    #             M = random.randint(1,5)
    #             P_MFGR ="Manufacturer#{0}".format(M)
    #
    #             N= random.randint(1,5)
    #             P_BRAND= "Brand#" + str(M) + str(N)
    #
    #             P_TYPE =types_syllable_1[random.randint(0, 5)] + " " + \
    #                     types_syllable_2[random.randint(0, 4)] + " " + \
    #                     types_syllable_3[random.randint(0, 4)]
    #
    #             P_SIZE = str(random.randint(1,50))
    #
    #             P_CONTAINER =containers_syllable_1[random.randint(0, 4)] + " " +\
    #                          containers_syllable_2[random.randint(0, 7)]
    #
    #             temp_P_RETAILPRICE= (90000 + ((P_PARTKEY/10) % 20001 ) + 100 * (P_PARTKEY % 1000))/100.0
    #             P_RETAILPRICE = '{:.2f}'.format(temp_P_RETAILPRICE)
    #             self.retail_price_part_table[P_PARTKEY] = temp_P_RETAILPRICE
    #
    #             P_COMMENT=InsertData.generate_random_string_data(random.randint(5, 22))
    #
    #             cur.execute("INSERT INTO PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",(str(P_PARTKEY), P_NAME, P_MFGR, P_BRAND, P_TYPE, str(P_SIZE), P_CONTAINER, str(P_RETAILPRICE),  P_COMMENT))
    #
    #         cur.close()
    #         conn.commit()
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)
    #
    #     finally:
    #         if conn is not None:
    #             conn.close()
    #
    # ---------------------------------
    @staticmethod
    def insert_SUPPLIER(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
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

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()


    @staticmethod
    def insert_PARTSUPP(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
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

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()


    #---------------------------------
    @staticmethod
    def insert_CUSTOMER(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
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

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # #---------------------------------
    # @staticmethod
    # def insert_ORDERS_LINEITEM(self):
    #     conn = None
    #
    #     Priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
    #     STARTDATE =datetime.strptime("1992-1-1", "%Y-%m-%d")
    #     ENDDATE =datetime.strptime("1998-12-31", "%Y-%m-%d")
    #     CURRENTDATE =datetime.strptime("1995-6-17", "%Y-%m-%d")
    #     Instructions=[ "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
    #     Modes=["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
    #
    #     try:
    #         params = config()
    #         conn = psycopg2.connect(**params)
    #         cur = conn.cursor()
    #
    #         customer_data_size = int(self.scale_factor * 150000)
    #         order_data_size = int(customer_data_size * 10)  #1500000
    #         order_key_size = int(order_data_size * 4) #1500000 * 4
    #         part_data_size = int(self.scale_factor * 200000)
    #
    #         subtracted_date = ENDDATE - timedelta(151)
    #         time_between_dates = subtracted_date - STARTDATE
    #         days_between_dates = time_between_dates.days
    #
    #         keys = list(range(order_key_size))
    #         random.shuffle(keys)
    #
    #         for id_order in range(order_data_size):
    #
    #             O_ORDERKEY = keys[id_order]
    #
    #             if O_ORDERKEY%4 ==0: #Only 25% of range key is populated
    #
    #                 customer_key = random.randint(1, customer_data_size)
    #                 # not all customers have order. Every third customer is not assigned any order
    #                 while (customer_key%3==0):
    #                     customer_key = random.randint(1, customer_data_size)
    #                 O_CUSTKEY = customer_key
    #
    #                 random_number_of_days = random.randrange(days_between_dates)
    #                 O_ORDERDATE = STARTDATE + timedelta(random_number_of_days)
    #
    #                 O_ORDERPRIORITY= Priorities[random.randint(0,4)]
    #                 O_CLERK = "Clerk#" + '{:09d}'.format(random.randint(1, int(self.scale_factor * 1000)))
    #                 O_SHIPPRIORITY = 0
    #                 O_COMMENT=InsertData.generate_random_string_data(random.randint(19,78))
    #
    #                 #-----------LINEITEM table
    #                 data_size_lineitem = random.randint(1,7)
    #                 O_TOTALPRICE=0
    #                 O_ORDERSTATUS = "P"
    #
    #                 part_key_temp=[]
    #                 temp_key = random.randint(0, (part_data_size-1))
    #                 part_key_temp.append(temp_key)
    #                 for id_lineitem in range(data_size_lineitem):
    #                     L_ORDERKEY = O_ORDERKEY
    #                     L_PARTKEY = temp_key
    #                     while (temp_key in part_key_temp):
    #                         temp_key = random.randint(0, (part_data_size-1))
    #                     part_key_temp.append(temp_key)
    #
    #
    #                     i_sup= random.randint(0,3)
    #                     S=int(self.scale_factor * 10000)
    #                     L_SUPPKEY = (L_PARTKEY + int((i_sup * ((S / 4) + int((L_PARTKEY - 1) / S))) % S) + 1)
    #
    #                     L_LINENUMBER = id_lineitem
    #
    #                     L_QUANTITY = random.randint(1,50)
    #
    #                     L_EXTENDEDPRICE = L_QUANTITY * self.retail_price_part_table[L_PARTKEY]
    #                     L_DISCOUNT =random.uniform(0.0, 0.10)
    #                     L_TAX=random.uniform(0.0, 0.08)
    #
    #                     L_SHIPDATE=O_ORDERDATE + timedelta(random.randint(1,121))
    #                     if L_SHIPDATE > CURRENTDATE:
    #                         L_LINESTATUS="O"
    #                     else:
    #                         L_LINESTATUS = "F"
    #
    #
    #                     if (L_LINESTATUS == "F"):
    #                         O_ORDERSTATUS = "F"
    #                     elif (L_LINESTATUS == "O"):
    #                         O_ORDERSTATUS = "O"
    #
    #
    #                     L_COMMITDATE=O_ORDERDATE+ timedelta(random.randint(30,90))
    #                     L_RECEIPTDATE= L_SHIPDATE+ timedelta(random.randint(1,30))
    #                     if L_RECEIPTDATE <= CURRENTDATE:
    #                         if random.random()<0.5:
    #                             L_RETURNFLAG = "R"
    #                         else:
    #                             L_RETURNFLAG = "A"
    #                     else:
    #                         L_RETURNFLAG = "N"
    #                     L_SHIPINSTRUCT=Instructions[random.randint(0,3)]
    #                     L_SHIPMODE=Modes[random.randint(0,6)]
    #                     L_COMMENT= InsertData.generate_random_string_data(random.randint(10,43))
    #
    #                     O_TOTALPRICE += L_EXTENDEDPRICE * (1 + L_TAX) * (1 - L_DISCOUNT)
    #                     cur.execute("INSERT INTO LINEITEM(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(L_ORDERKEY), str(L_PARTKEY), str(L_SUPPKEY), str(L_LINENUMBER), str(L_QUANTITY), str(L_EXTENDEDPRICE), str(L_DISCOUNT), str(L_TAX), str(L_RETURNFLAG), str(L_LINESTATUS), str(L_SHIPDATE), str(L_COMMITDATE), str(L_RECEIPTDATE), str(L_SHIPINSTRUCT), str(L_SHIPMODE), str(L_COMMENT)))
    #
    #                 cur.execute(
    #                     "INSERT INTO ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
    #                     (str(O_ORDERKEY), str(O_CUSTKEY), O_ORDERSTATUS, str(O_TOTALPRICE), str(O_ORDERDATE),
    #                      str(O_ORDERPRIORITY), str(O_CLERK), str(O_SHIPPRIORITY), O_COMMENT))
    #
    #         cur.close()
    #         conn.commit()
    #
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)
    #
    #     finally:
    #         if conn is not None:
    #             conn.close()

    # ---------------------------------
    @staticmethod
    def insert_NATION(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
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

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # ---------------------------------
    @staticmethod
    def insert_REGION(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()

            with open(InsertDataCsv.csv_path+'nation.csv', 'r') as f:
                reader = csv.reader(f, delimiter='|')
                next(reader)  # Skip the header row.
                for row in reader:
                    cur.execute(
                        "INSERT INTO REGION(R_REGIONKEY,R_NAME,R_COMMENT) VALUES (%s, %s, %s)",
                        row
                    )
            cur.close()
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # # ---------------------------------
    # @staticmethod
    # def generate_random_string_data(string_length):
    #     # generating random data..
    #     res = ''.join(choice(ascii_lowercase) for i in range(string_length))
    #     return res

    def insert_to_tables(self):
        # InsertDataCsv.insert_PART(self)
        # InsertDataCsv.insert_SUPPLIER(self)
        # InsertDataCsv.insert_PARTSUPP(self)
          InsertDataCsv.insert_CUSTOMER(self)
        # InsertDataCsv.insert_ORDERS_LINEITEM(self)
        # InsertDataCsv.insert_NATION(self)
        # InsertDataCsv.insert_REGION(self)



