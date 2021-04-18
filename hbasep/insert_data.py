import random
from random import choice
from string import ascii_lowercase
import phoenixdb
from hbasep.config import config
from datetime import datetime, timedelta

class InsertData:
    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor
        self.retail_price_part_table = {}

    #---------------------------------
    @staticmethod
    def insert_PART(self):
        # conn = None
        try:

            # params = config()
            # conn = phoenixdb.connect(**params)
            conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)
            cur = conn.cursor()

            data_size = int(self.scale_factor * 200000)

            keys=list(range(data_size))
            random.shuffle(keys)

            part_names = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
                          "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
                          "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
                          "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
                          "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
                          "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
                          "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
                          "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell",
                          "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise",
                          "violet", "wheat", "white", "yellow"]

            len_part_names = len(part_names)

            types_syllable_1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
            types_syllable_2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
            types_syllable_3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

            containers_syllable_1 = ["SM", "LG", "MED", "JUMBO", "WRAP"]
            containers_syllable_2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]


            for i in range(data_size):

                P_PARTKEY= keys[i]

                random_names=[]
                while (len(random_names)<5):
                    random_index = random.randint(0,(len_part_names-1))
                    if (not random_names.__contains__(part_names[random_index])):
                        random_names.append(part_names[random_index])
                space_char=" "
                P_NAME = space_char.join(random_names)

                M = random.randint(1,5)
                P_MFGR ="Manufacturer#{0}".format(M)

                N= random.randint(1,5)
                P_BRAND= "Brand#" + str(M) + str(N)

                P_TYPE =types_syllable_1[random.randint(0, 5)] + " " + \
                        types_syllable_2[random.randint(0, 4)] + " " + \
                        types_syllable_3[random.randint(0, 4)]

                P_SIZE = str(random.randint(1,50))

                P_CONTAINER =containers_syllable_1[random.randint(0, 4)] + " " +\
                             containers_syllable_2[random.randint(0, 7)]

                temp_P_RETAILPRICE= (90000 + ((P_PARTKEY/10) % 20001 ) + 100 * (P_PARTKEY % 1000))/100.0
                P_RETAILPRICE = '{:.2f}'.format(temp_P_RETAILPRICE)
                self.retail_price_part_table[P_PARTKEY] = temp_P_RETAILPRICE

                P_COMMENT=InsertData.generate_random_string_data(random.randint(5, 22))

                cur.execute("INSERT INTO PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",(str(P_PARTKEY), P_NAME, P_MFGR, P_BRAND, P_TYPE, str(P_SIZE), P_CONTAINER, str(P_RETAILPRICE),  P_COMMENT))

            cur.close()
            conn.commit()
        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # ---------------------------------
    @staticmethod
    def insert_SUPPLIER(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            data_size = int(self.scale_factor * 10000)

            keys = list(range(data_size))
            random.shuffle(keys)

            comment_ratio = int(self.scale_factor * 5)

            for i in range(data_size):
                S_SUPPKEY = keys[i]
                S_NAME = "Supplier#r"+"{:09d}".format(S_SUPPKEY)

                S_ADDRESS =InsertData.generate_random_string_data(random.randint(10, 40))

                S_NATIONKEY =random.randint(0, 24)

                country_code=S_NATIONKEY+10
                local_number1=random.randint(100, 999)
                local_number2 = random.randint(100, 999)
                local_number3 = random.randint(1000, 9999)
                S_PHONE=str(country_code)+ "-"+ str(local_number1)+"-"+str(local_number2)+"-"+str(local_number3)

                S_ACCTBAL='{:.2f}'.format(random.uniform(-999.99, 9999.99))

                comment=InsertData.generate_random_string_data(random.randint(25, 100))
                if S_SUPPKEY<comment_ratio:
                    while (comment<25 or comment>100):
                        comment=InsertData.generate_random_string_data(random.randint(0, 20))+"Customer " + InsertData.generate_random_string_data(random.randint(0, 20)) + "Complaints"+InsertData.generate_random_string_data(random.randint(0, 20))
                if S_SUPPKEY > data_size-comment_ratio:
                    while (comment < 25 or comment > 100):
                        comment=InsertData.generate_random_string_data(random.randint(0, 20))+"Customer " + InsertData.generate_random_string_data(random.randint(0, 20)) + "Recommends"+InsertData.generate_random_string_data(random.randint(0, 20))
                S_COMMENT = comment

                cur.execute(
                    "INSERT INTO SUPPLIER(S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY,  S_PHONE,  S_ACCTBAL,  S_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    (str(S_SUPPKEY), S_NAME, S_ADDRESS, str(S_NATIONKEY), S_PHONE, str(S_ACCTBAL), S_COMMENT))

            cur.close()
            conn.commit()

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    #---------------------------------
    @staticmethod
    def insert_PARTSUPP(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            data_size = int(self.scale_factor * 200000)

            #Repeating Keys generated in part table
            keys = list(range(data_size))

            S = self.scale_factor * 10000

            for ind_part in range(data_size):
                for ind_partSupp in range (4):
                    PS_PARTKEY = keys[ind_part]
                    PS_SUPPKEY =int( ((PS_PARTKEY + (ind_partSupp * ((S/4) + ((int(PS_PARTKEY-1 ))/S)))) % S) + 1)

                    PS_AVAILQTY=random.randint(1,9999)
                    PS_SUPPLYCOST= random.uniform(1.0,1000.0)
                    PS_COMMENT= InsertData.generate_random_string_data(random.randint(49, 198))

                    cur.execute(
                        "INSERT INTO PARTSUPP(PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT) VALUES (%s, %s, %s, %s, %s)",
                        (str(PS_PARTKEY), str(PS_SUPPKEY), str(PS_AVAILQTY),str(PS_SUPPLYCOST),PS_COMMENT ))

            cur.close()
            conn.commit()

        except (Exception, phoenixdb.DatabaseError) as error:
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
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            data_size = int(self.scale_factor * 150000)

            keys = list(range(data_size))
            random.shuffle(keys)

            segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]

            for i in range(data_size):
                C_CUSTKEY=keys[i]
                C_NAME = "Customer#"+'{:09d}'.format(C_CUSTKEY)
                C_ADDRESS = InsertData.generate_random_string_data(random.randint(10,40))
                C_NATIONKEY = random.randint(0,24)

                country_code = C_NATIONKEY + 10
                local_number1 = random.randint(100, 999)
                local_number2 = random.randint(100, 999)
                local_number3 = random.randint(1000, 9999)
                C_PHONE = str(country_code) + "-" + str(local_number1) + "-" + str(local_number2) + "-" + str(local_number3)
                C_ACCTBAL = random.uniform(-999.99,9999.99)
                C_MKTSEGMENT=segments[random.randint(0,4)]
                C_COMMENT = InsertData.generate_random_string_data(random.randint(29,116))

                cur.execute(
                    "INSERT INTO CUSTOMER(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY,C_PHONE,C_ACCTBAL, C_MKTSEGMENT,C_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (str(C_CUSTKEY), C_NAME, C_ADDRESS, str(C_NATIONKEY),C_PHONE, str(C_ACCTBAL), C_MKTSEGMENT, C_COMMENT))

            cur.close()
            conn.commit()

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    #---------------------------------
    @staticmethod
    def insert_ORDERS_LINEITEM(self):
        conn = None

        Priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
        STARTDATE =datetime.strptime("1992-1-1", "%Y-%m-%d")
        ENDDATE =datetime.strptime("1998-12-31", "%Y-%m-%d")
        CURRENTDATE =datetime.strptime("1995-6-17", "%Y-%m-%d")
        Instructions=[ "DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
        Modes=["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]

        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            customer_data_size = int(self.scale_factor * 150000)
            order_data_size = int(customer_data_size * 10)  #1500000
            order_key_size = int(order_data_size * 4) #1500000 * 4
            part_data_size = int(self.scale_factor * 200000)

            subtracted_date = ENDDATE - timedelta(151)
            time_between_dates = subtracted_date - STARTDATE
            days_between_dates = time_between_dates.days

            keys = list(range(order_key_size))
            random.shuffle(keys)

            for id_order in range(order_data_size):

                O_ORDERKEY = keys[id_order]

                if O_ORDERKEY%4 ==0: #Only 25% of range key is populated

                    customer_key = random.randint(1, customer_data_size)
                    # not all customers have order. Every third customer is not assigned any order
                    while (customer_key%3==0):
                        customer_key = random.randint(1, customer_data_size)
                    O_CUSTKEY = customer_key

                    random_number_of_days = random.randrange(days_between_dates)
                    O_ORDERDATE = STARTDATE + timedelta(random_number_of_days)

                    O_ORDERPRIORITY= Priorities[random.randint(0,4)]
                    O_CLERK = "Clerk#" + '{:09d}'.format(random.randint(1, int(self.scale_factor * 1000)))
                    O_SHIPPRIORITY = 0
                    O_COMMENT=InsertData.generate_random_string_data(random.randint(19,78))

                    #-----------LINEITEM table
                    data_size_lineitem = random.randint(1,7)
                    O_TOTALPRICE=0
                    O_ORDERSTATUS = "P"

                    part_key_temp=[]
                    temp_key = random.randint(0, (part_data_size-1))
                    part_key_temp.append(temp_key)
                    for id_lineitem in range(data_size_lineitem):
                        L_ORDERKEY = O_ORDERKEY
                        L_PARTKEY = temp_key
                        while (temp_key in part_key_temp):
                            temp_key = random.randint(0, (part_data_size-1))
                        part_key_temp.append(temp_key)


                        i_sup= random.randint(0,3)
                        S=int(self.scale_factor * 10000)
                        L_SUPPKEY = (L_PARTKEY + int((i_sup * ((S / 4) + int((L_PARTKEY - 1) / S))) % S) + 1)

                        L_LINENUMBER = id_lineitem

                        L_QUANTITY = random.randint(1,50)

                        L_EXTENDEDPRICE = L_QUANTITY * self.retail_price_part_table[L_PARTKEY]
                        L_DISCOUNT =random.uniform(0.0, 0.10)
                        L_TAX=random.uniform(0.0, 0.08)

                        L_SHIPDATE=O_ORDERDATE + timedelta(random.randint(1,121))
                        if L_SHIPDATE > CURRENTDATE:
                            L_LINESTATUS="O"
                        else:
                            L_LINESTATUS = "F"


                        if (L_LINESTATUS == "F"):
                            O_ORDERSTATUS = "F"
                        elif (L_LINESTATUS == "O"):
                            O_ORDERSTATUS = "O"


                        L_COMMITDATE=O_ORDERDATE+ timedelta(random.randint(30,90))
                        L_RECEIPTDATE= L_SHIPDATE+ timedelta(random.randint(1,30))
                        if L_RECEIPTDATE <= CURRENTDATE:
                            if random.random()<0.5:
                                L_RETURNFLAG = "R"
                            else:
                                L_RETURNFLAG = "A"
                        else:
                            L_RETURNFLAG = "N"
                        L_SHIPINSTRUCT=Instructions[random.randint(0,3)]
                        L_SHIPMODE=Modes[random.randint(0,6)]
                        L_COMMENT= InsertData.generate_random_string_data(random.randint(10,43))

                        O_TOTALPRICE += L_EXTENDEDPRICE * (1 + L_TAX) * (1 - L_DISCOUNT)
                        cur.execute("INSERT INTO LINEITEM(L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(L_ORDERKEY), str(L_PARTKEY), str(L_SUPPKEY), str(L_LINENUMBER), str(L_QUANTITY), str(L_EXTENDEDPRICE), str(L_DISCOUNT), str(L_TAX), str(L_RETURNFLAG), str(L_LINESTATUS), str(L_SHIPDATE), str(L_COMMITDATE), str(L_RECEIPTDATE), str(L_SHIPINSTRUCT), str(L_SHIPMODE), str(L_COMMENT)))

                    cur.execute(
                        "INSERT INTO ORDERS(O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (str(O_ORDERKEY), str(O_CUSTKEY), O_ORDERSTATUS, str(O_TOTALPRICE), str(O_ORDERDATE),
                         str(O_ORDERPRIORITY), str(O_CLERK), str(O_SHIPPRIORITY), O_COMMENT))

            cur.close()
            conn.commit()

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # ---------------------------------
    @staticmethod
    def insert_NATION(self):
        conn = None
        try:
            params = config()
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            #Dict of nation_name["N_NATIONKEY"] = "N_NAME"
            nation_name={0:"ALGERIA", 1:"ARGENTINA", 2:"BRAZIL", 3:"CANADA",
                         4:"EGYPT", 5:"ETHIOPIA", 6:"FRANCE", 7:"GERMANY",
                         8:"INDIA", 9:"INDONESIA", 10:"IRAN", 11:"IRAQ",
                         12:"JAPAN", 13:"JORDAN", 14:"KENYA", 15:"MOROCCO",
                         16:"MOZAMBIQUE", 17:"PERU", 18:"CHINA", 19:"ROMANIA",
                         20:"SAUDI ARABIA", 21:"VIETNAM", 22:"RUSSIA", 23:"UNITED KINGDOM",
                         24:"UNITED STATES"}

            # Dict of region_key["R_REGIONKEY"] = "R_NAME"
            region_key = {"ALGERIA":0, "ARGENTINA":1, "BRAZIL":1, "CANADA":1, "EGYPT":4, "ETHIOPIA":0,
                           "FRANCE":3, "GERMANY":3, "INDIA":2, "INDONESIA":2, "IRAN":4, "IRAQ":4,
                          "JAPAN":2, "JORDAN":4, "KENYA":0, "MOROCCO":0, "MOZAMBIQUE":0, "PERU":1,
                          "CHINA":2, "ROMANIA":3, "SAUDI ARABIA":4, "VIETNAM":2,
                           "RUSSIA":3, "UNITED KINGDOM":3, "UNITED STATES":1}


            data_size =25
            for N_NATIONKEY in range(data_size):
                N_NAME=nation_name[N_NATIONKEY]
                N_REGIONKEY=region_key[N_NAME]
                N_COMMENT=InsertData.generate_random_string_data(random.randint(31,114))
                cur.execute(
                    "INSERT INTO NATION(N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT) VALUES (%s, %s, %s, %s)",
                    (str(N_NATIONKEY), N_NAME, str(N_REGIONKEY), N_COMMENT))
            cur.close()
            conn.commit()

        except (Exception, phoenixdb.DatabaseError) as error:
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
            conn = phoenixdb.connect(**params)
            cur = conn.cursor()

            data_size =5
            region_names = ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]

            for i in range(data_size):
                R_REGIONKEY=i
                R_NAME=region_names[i]
                R_COMMENT=InsertData.generate_random_string_data(random.randint(31,115))
                cur.execute(
                    "INSERT INTO REGION(R_REGIONKEY,R_NAME,R_COMMENT) VALUES (%s, %s, %s)",
                    (str(R_REGIONKEY), R_NAME, R_COMMENT))
            cur.close()
            conn.commit()

        except (Exception, phoenixdb.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # ---------------------------------
    @staticmethod
    def generate_random_string_data(string_length):
        # generating random data..
        res = ''.join(choice(ascii_lowercase) for i in range(string_length))
        return res

    def insert_to_tables(self):
        InsertData.insert_PART(self)
        InsertData.insert_SUPPLIER(self)
        InsertData.insert_PARTSUPP(self)
        InsertData.insert_CUSTOMER(self)
        InsertData.insert_ORDERS_LINEITEM(self)
        InsertData.insert_NATION(self)
        InsertData.insert_REGION(self)



