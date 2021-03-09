from mongodb.initialize_db import InitilizeDB
import string
import random
from faker import Faker


class InsertData:
    def __init__(self, scale_factor):
        super().__init__()
        self.scale_factor = scale_factor

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

    def insert_to_collections(self):
        InsertData.region()
        InsertData.insert_nation()
        InsertData.insert_supplier(self)
