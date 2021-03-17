from mongodb.initialize_db import InitilizeDB
import string
import random
from faker import Faker


class InsertData:
    retail_prices = []

    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor
        self.retail_prices = self.scale_factor * 200000

    @staticmethod
    def generate_random_data(max_length):
        # generating random data..
        res = ''.join(random.choice(string.ascii_lowercase)
                      for x in range(max_length))
        return res

    @staticmethod
    def region():
        region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
        region_collection_values = []

        for i, val in enumerate(region_names):
            value = {
                "region_key": i,
                "name": val,
                "comment": InsertData.generate_random_data(random.randint(1, 100))
            }
            region_collection_values.append(value)
        InitilizeDB.insert(
            InitilizeDB.DATABASE["region"].name, region_collection_values)

    @staticmethod
    def insert_nation():
        nations = {"ALGERIA,0", "ARGENTINA,1", "BRAZIL,1", "CANADA,1", "EGYPT,4", "ETHIOPIA,0",
                                "FRANCE,3", "GERMANY,3", "INDIA,2", "INDONESIA,2", "IRAN,4", "IRAQ,4", "JAPAN,2", "JORDAN,4", "KENYA,0",
                                "MOROCCO,0", "MOZAMBIQUE,0", "PERU,1", "CHINA,2", "ROMANIA,3", "SAUDI ARABIA,4", "VIETNAM,2",
                                "RUSSIA,3", "UNITED KINGDOM,3", "UNITED STATES,1"}
        nations_collection_values = []

        for i, val in enumerate(nations):
            value = {
                "nation_key": i,
                "name": val,
                "region_key": random.randint(0, 25),
                "comment": InsertData.generate_random_data(random.randint(1, 100))
            }
            nations_collection_values.append(value)
        InitilizeDB.insert(
            InitilizeDB.DATABASE["nation"].name, nations_collection_values)

    @staticmethod
    def generate_identifiers(value):
        keys = []
        for i in range(value):
            keys.append(i)

        return keys

    @staticmethod
    def insert_supplier(self):
        data_size = int(self.scale_factor * 10000)
        keys = InsertData.generate_identifiers(data_size)
        comments_list = []
        commented_size = int(10 * self.scale_factor)

        while(len(comments_list) < commented_size):
            index = random.randint(0, data_size)
            if(not comments_list.__contains__(index)):
                comments_list.append(index)

        fake = Faker("en_CA")
        for i in range(data_size):
            key = keys.index(i)
            formatted_key = '{:09d}'.format(key)
            address_data = InsertData.generate_random_data(
                random.randint(1, 30))
            phone = fake.phone_number()
            balance = '{:.2f}'.format(
                (random.randint(0, 1099999)-99999) / 100.0)
            comments = "No comments..."

            comments_index = comments_list.index(
                key) if key in comments_list else -1

            complaints = InsertData.generate_random_data(
                random.randint(1, 124))
            recommends = InsertData.generate_random_data(
                random.randint(1, 124))
            if(comments_index != -1):
                if(comments_index < 5 * self.scale_factor):
                    comments = "Customer " + complaints + "Complaints"
                else:
                    comments = "Customer " + recommends + "Recommends"

            supplier_collection_values = []

            value = {
                "supplier_key": key,
                "name": "Supplier#r"+formatted_key,
                "address": address_data,
                "nation_key": random.randint(0, 25),
                "phone": phone,
                "acct_bal": balance,
                "comment": InsertData.generate_random_data(random.randint(1, 100))
            }
            supplier_collection_values.append(value)
            InitilizeDB.insert(
                InitilizeDB.DATABASE["supplier"].name, supplier_collection_values)

    @staticmethod
    def insert_part(self):
        part_names = ["almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue",
                      "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower",
                      "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest",
                      "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory",
                      "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium",
                      "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid",
                      "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy",
                      "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring",
                      "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"]

        type_1 = ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
        type_2 = ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
        type_3 = ["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
        container_1 = ["SM", "LG", "MED", "JUMBO", "WRAP"]
        container_2 = ["CASE", "BOX", "BAG",
                       "JAR", "PKG", "PACK", "CAN", "DRUM"]
        data_size = int(self.scale_factor * 200000)
        keys = InsertData.generate_identifiers(200000)

        for i in range(data_size):
            key = keys.index(i)
            selected_names = []
            length = part_names.__len__()
            while(len(selected_names) < 5):
                index = random.randint(0, 91)
                if(not selected_names.__contains__(part_names[index])):
                    selected_names.append(part_names[index])

            space = " "
            name_string = space.join(selected_names)
            M = random.randint(0, 5)
            mfgr_string = f'Manufacturer#{M}'
            brand = "Brand#" + str(M) + str(random.randint(0, 5))
            type_string = type_1[random.randint(
                0, 5)] + " " + type_2[random.randint(0, 4)] + " " + type_3[random.randint(0, 4)]

            size_string = str(random.randint(0, 50))
            container_string = container_1[random.randint(
                0, 4)] + " " + container_2[random.randint(0, 7)]

            price = (90000 + (key/10) % 20001 + 100 * key % 1000) / 100.0
            # self.retail_prices
            retail_price_string = '{:.2f}'.format(price)
            p_comments = InsertData.generate_random_data(
                random.randint(1, 124))

            part_collection_values = []
            value = {
                "part_key": key,
                "name": name_string,
                "mfgr": mfgr_string,
                "brand": brand,
                "type": type_string,
                "size": size_string,
                "container": container_string,
                "retail_price": retail_price_string,
                "comment": p_comments
            }
            part_collection_values.append(value)
            InitilizeDB.insert(
                InitilizeDB.DATABASE["part"].name, part_collection_values)

    def insert_to_collections(self):
        InsertData.region()
        InsertData.insert_nation()
        InsertData.insert_supplier(self)
        InsertData.insert_part(self)
