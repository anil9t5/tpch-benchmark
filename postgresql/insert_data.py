import random
from random import choice
from string import ascii_lowercase
import psycopg2
from postgresql.config import config

class InsertData:

    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor

    @staticmethod
    def insert_PART(self):
        conn = None

        try:
            # read the connection parameters
            params = config()

            # connect to the PostgreSQL server
            conn = psycopg2.connect(**params)

            # create a cursor to connect to database only once..
            cur = conn.cursor()

            data_size = int(self.scale_factor * 200000)
            keys = InsertData.generate_identifiers(self.scale_factor * 200000)
            part_names = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
                          "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral",
                          "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick",
                          "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew",
                          "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen",
                          "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo",
                          "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder",
                          "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell",
                          "sienna",
                          "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise",
                          "violet",
                          "wheat", "white", "yellow"]
            len_part_names = len(part_names)

            types_syllable_1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
            types_syllable_2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
            types_syllable_3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]

            containers_syllable_1=["SM", "LG", "MED", "JUMBO", "WRAP"]
            containers_syllable_2 = ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]


            for i in range(data_size):

                P_PARTKEY= keys.index(i)


                random_names=[]
                while (len(random_names)<5):
                    random_index = random.randint(0,len_part_names)
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

                one_PART_row = []

                cur.execute('''INSERT INTO PART(P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT) VALUES ({0}, '{1}', '{2}', '{3}', '{4}', {5}, '{6}', {7}, '{8}')''').format(P_PARTKEY, P_NAME, P_MFGR, P_BRAND,  P_TYPE,  P_SIZE,  P_CONTAINER,  P_RETAILPRICE,  P_COMMENT)

            cur.close()
            conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def generate_identifiers(value):
        keys = []
        for i in range(value):
            keys.append(i)
        return keys

    @staticmethod
    def generate_random_string_data(string_length):
        # generating random data..
        res = ''.join(choice(ascii_lowercase) for i in range(string_length))
        return res

