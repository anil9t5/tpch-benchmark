import random
from random import choice
from string import ascii_lowercase
import psycopg2
from postgresql.config import config
from faker import Faker

class InsertData:


    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor


    #----Inser Part
    @staticmethod
    def insert_PART(self):
        conn = None
        try:

            params = config()
            conn = psycopg2.connect(**params)
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

                temp_P_RETAILPRICE= (90000 + ((P_PARTKEY/10) % 20001 ) + 100 * (P_PARTKEY % 1000))/100
                P_RETAILPRICE = '{:.2f}'.format(temp_P_RETAILPRICE)

                P_COMMENT=InsertData.generate_random_string_data(random.randint(5, 22))

                cur.execute("INSERT INTO PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",(str(P_PARTKEY), P_NAME, P_MFGR, P_BRAND, P_TYPE, str(P_SIZE), P_CONTAINER, str(P_RETAILPRICE),  P_COMMENT))

            cur.close()
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()



    # ----Insert SUPPLIER
    @staticmethod
    def insert_SUPPLIER(self):
        conn = None
        try:
            params = config()
            conn = psycopg2.connect(**params)
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

                nations_code=random.randint(0,25)
                country_code=nations_code+10
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

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    # ----Insert SUPPLIER
    @staticmethod
    def insert_PARTSUPP(self):
        pass
        # conn = None
        # try:
        #     params = config()
        #     conn = psycopg2.connect(**params)
        #     cur = conn.cursor()
        #
        #     data_size=
        #     #Keys generated in part table
        #     keys = list(range(int(self.scale_factor * 200000)))
        #
        #
        #     for i in range(data_size):
        #         PS_PARTKEY = keys[i]
        #         PS_SUPPKEY =







            # cur.close()
            # conn.commit()

        # except (Exception, psycopg2.DatabaseError) as error:
        #     print(error)
        #
        # finally:
        #     if conn is not None:
        #         conn.close()

#==================================================
    # ----Insert PARTSUPP
    @staticmethod
    def insert_PARTSUPP(self):
        pass


    # ----Insert CUSTOMER
    @staticmethod
    def insert_CUSTOMER(self):
        pass


    # ----Insert CUSTOMER
    @staticmethod
    def insert_CUSTOMER(self):
        pass

    # ----Insert ORDERS
    @staticmethod
    def insert_ORDERS(self):
        pass


    # ----Insert LINEITEM
    @staticmethod
    def insert_LINEITEM(self):
        pass

    # ----Insert NATION
    @staticmethod
    def insert_NATION(self):
        pass

    # ----Insert REGION
    @staticmethod
    def insert_REGION(self):
            pass

    #-------------------------------------------
    # @staticmethod
    # def generate_identifiers(value):
    #     keys = []
    #     for i in range(value):
    #         keys.append(i)
    #     return keys

    @staticmethod
    def generate_random_string_data(string_length):
        # generating random data..
        res = ''.join(choice(ascii_lowercase) for i in range(string_length))
        return res

    def insert_to_tables(self):
        InsertData.insert_PART(self)
        InsertData.insert_SUPPLIER(self)
        InsertData.insert_PARTSUPP(self)



