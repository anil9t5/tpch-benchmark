from mongodb.initialize_db import InitilizeDB
import string
import random
from datetime import datetime, timedelta
import numpy as np


class InsertData:

    def __init__(self, scale_factor, db):
        super().__init__()
        self.db = db
        self.scale_factor = scale_factor
        self.retail_prices = np.array(
            [self.scale_factor * 200000], dtype='float')

    @staticmethod
    def generate_random_data(max_length):
        # generating random data..
        res = ''.join(random.choice(string.ascii_lowercase)
                      for x in range(max_length))
        return res

    @staticmethod
    def region(self):
        region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
        region_collection_values = []

        for i, val in enumerate(region_names):
            value = {
                "region_key": i,
                "name": val,
                "comment": InsertData.generate_random_data(random.randint(31, 115))
            }
            region_collection_values.append(value)

        self.db["region"].insert(
            region_collection_values)

    @staticmethod
    def insert_nation(self):
        # nations = {"ALGERIA,0", "ARGENTINA,1", "BRAZIL,1", "CANADA,1", "EGYPT,4", "ETHIOPIA,0",
        #                         "FRANCE,3", "GERMANY,3", "INDIA,2", "INDONESIA,2", "IRAN,4", "IRAQ,4", "JAPAN,2", "JORDAN,4", "KENYA,0",
        #                         "MOROCCO,0", "MOZAMBIQUE,0", "PERU,1", "CHINA,2", "ROMANIA,3", "SAUDI ARABIA,4", "VIETNAM,2",
        #                         "RUSSIA,3", "UNITED KINGDOM,3", "UNITED STATES,1"}
        nations = {0: "ALGERIA", 1: "ARGENTINA", 2: "BRAZIL", 3: "CANADA",
                   4: "EGYPT", 5: "ETHIOPIA", 6: "FRANCE", 7: "GERMANY",
                   8: "INDIA", 9: "INDONESIA", 10: "IRAN", 11: "IRAQ",
                   12: "JAPAN", 13: "JORDAN", 14: "KENYA", 15: "MOROCCO",
                   16: "MOZAMBIQUE", 17: "PERU", 18: "CHINA", 19: "ROMANIA",
                   20: "SAUDI ARABIA", 21: "VIETNAM", 22: "RUSSIA", 23: "UNITED KINGDOM",
                   24: "UNITED STATES"}

        region_key = {"ALGERIA": 0, "ARGENTINA": 1, "BRAZIL": 1, "CANADA": 1, "EGYPT": 4, "ETHIOPIA": 0,
                      "FRANCE": 3, "GERMANY": 3, "INDIA": 2, "INDONESIA": 2, "IRAN": 4, "IRAQ": 4,
                      "JAPAN": 2, "JORDAN": 4, "KENYA": 0, "MOROCCO": 0, "MOZAMBIQUE": 0, "PERU": 1,
                      "CHINA": 2, "ROMANIA": 3, "SAUDI ARABIA": 4, "VIETNAM": 2,
                      "RUSSIA": 3, "UNITED KINGDOM": 3, "UNITED STATES": 1}
        nations_collection_values = []
        data_size = 25
        for nation_key in range(data_size):
            n_name = nations[nation_key]
            n_regionkey = region_key[n_name]
            value = {
                "nation_key": nation_key,
                "name": n_name,
                "region_key": n_regionkey,
                "comment": InsertData.generate_random_data(random.randint(31, 114))
            }
            nations_collection_values.append(value)

        self.db["nation"].insert(
            nations_collection_values)

    @staticmethod
    def generate_identifiers(value):
        keys = []
        for i in range(value):
            keys.append(i)

        return keys

    @staticmethod
    def insert_supplier(self):
        data_size = int(self.scale_factor * 10000)
        keys = keys = list(range(data_size))
        random.shuffle(keys)
        comments_list = []
        commented_size = int(5 * self.scale_factor)

        while(len(comments_list) < commented_size):
            index = random.randint(0, data_size)
            if(not comments_list.__contains__(index)):
                comments_list.append(index)

        for i in range(data_size):
            key = keys.index(i)
            formatted_key = '{:09d}'.format(key)
            address_data = InsertData.generate_random_data(
                random.randint(10, 40))
            nation_key = random.randint(0, 24)
            country_code = nation_key+10
            local_number1 = random.randint(100, 999)
            local_number2 = random.randint(100, 999)
            local_number3 = random.randint(1000, 9999)
            phone = str(country_code) + "-" + str(local_number1) + \
                "-"+str(local_number2)+"-"+str(local_number3)
            balance = '{:.2f}'.format(random.uniform(-999.99, 9999.99))
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
                "nation_key": nation_key,
                "phone": phone,
                "acct_bal": balance,
                "comment": InsertData.generate_random_data(random.randint(25, 100))
            }
            supplier_collection_values.append(value)

            self.db["supplier"].insert(
                supplier_collection_values)

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

        keys = list(range(data_size))
        random.shuffle(keys)

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
            M = random.randint(1, 5)
            mfgr_string = f'Manufacturer#{M}'
            brand = "Brand#" + str(M) + str(random.randint(1, 5))
            type_string = type_1[random.randint(
                0, 5)] + " " + type_2[random.randint(0, 4)] + " " + type_3[random.randint(0, 4)]

            size_string = str(random.randint(1, 50))
            container_string = container_1[random.randint(
                0, 4)] + " " + container_2[random.randint(0, 7)]

            price = (90000 + (key/10) % 20001 + 100 * key % 1000) / 100.0

            # self.retail_prices[key] = price
            retail_price_string = '{:.2f}'.format(price)
            p_comments = InsertData.generate_random_data(
                random.randint(5, 22))
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

            self.db["part"].insert(
                part_collection_values)

    @staticmethod
    def insert_part_supplier(self):
        S = int(self.scale_factor * 10000)
        data_size = int(self.scale_factor * 200000)
        keys = list(range(data_size))

        for i in range(data_size):
            key = keys.index(i)
            for j in range(4):
                part_supplier_key = (key+(i*((S/4)+(key-1)/S))) % S
                part_supplier_avail_qty = random.randint(1, 9999)
                length = random.randint(0, 150)
                part_supplier_cost = random.uniform(1.0, 1000.0)
                part_supplier_comment = InsertData.generate_random_data(
                    random.randint(49, 198))
                part_supplier_collection_values = []
                value = {
                    "part_key": key,
                    "supp_key": part_supplier_key,
                    "avail_qty": part_supplier_avail_qty,
                    "supply_cost": part_supplier_cost,
                    "comment": part_supplier_comment
                }
                part_supplier_collection_values.append(value)

                self.db["partsupp"].insert(
                    part_supplier_collection_values)

    @staticmethod
    def insert_customer(self):
        segments = ["AUTOMOBILE", "BUILDING",
                    "FURNITURE", "MACHINERY", "HOUSEHOLD"]
        data_size = int(self.scale_factor * 150000)
        keys = list(range(data_size))
        random.shuffle(keys)

        for i in range(data_size):
            key = keys.index(i)
            customer_name = "Customer#"+'{:09d}'.format(key)
            length = random.randint(0, 30)
            address = InsertData.generate_random_data(
                random.randint(1, 30))
            nation_key = random.randint(0, 24)
            country_code = nation_key + 10
            local_number1 = random.randint(100, 999)
            local_number2 = random.randint(100, 999)
            local_number3 = random.randint(1000, 9999)
            phone = str(country_code) + "-" + str(local_number1) + \
                "-" + str(local_number2) + "-" + str(local_number3)
            acc_balance = random.uniform(-999.99, 9999.99)
            mkt_segment = segments[random.randint(0, 4)]
            count = random.randint(0, 88)
            comment_string = InsertData.generate_random_data(
                random.randint(29, 116))

            customer_collection_values = []
            values = {
                "cust_key": key,
                "name": customer_name,
                "address": address,
                "nation_key": nation_key,
                "phone": phone,
                "acct_balance": acc_balance,
                "mkt_segment": mkt_segment,
                "comment": comment_string
            }
            customer_collection_values.append(values)
            self.db["customer"].insert(
                customer_collection_values)

    @staticmethod
    def insert_order_line_item(self):
        priorities = ["1-URGENT", "2-HIGH",
                      "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
        instructs = ["DELIVER IN PERSON",
                     "COLLECT COD", "NONE", "TAKE BACK RETURN"]
        modes = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
        start_date = datetime.strptime("1992-1-1", "%Y-%m-%d")
        current_date = datetime.strptime("1995-6-17", "%Y-%m-%d")
        end_date = datetime.strptime("1998-12-31", "%Y-%m-%d")
        order_size = int(self.scale_factor * 150000 * 4)
        keys = list(range(order_size))
        random.shuffle(keys)

        days_range = 2405
        for i in range(order_size):
            key = keys.index(i)
            if (key % 4) != 0:
                continue
            customer_key = 0
            while (customer_key % 3) == 0:
                customer_key = random.randint(
                    0, int(self.scale_factor * 150000))
            order_status = "P"
            total_price = 0.0
            order_date = start_date + timedelta(random.randint(1, days_range))
            priority_string = priorities[random.randint(0, 4)]
            clerk_string = "Clerk#" + \
                '{:09d}'.format(random.randint(
                    1, int(self.scale_factor * 1000)))
            ship_priority = "0"
            order_comment = InsertData.generate_random_data(
                random.randint(19, 78))

            line_item_rows = random.randint(1, 7)
            F_number = 0
            O_number = 0

            for j in range(line_item_rows):
                L_order_key = key
                part_key = random.randint(
                    0, int(self.scale_factor * 200000))
                S = int(self.scale_factor * 10000)
                i = random.randint(0, 4)
                supplier_key = (part_key+(i*((S/4)+(part_key-1)/S)))
                line_number = j
                quantity = random.randint(0, 50)

                extended_price = 0.0

                if (part_key < self.retail_prices.size):
                    extended_price = quantity * self.retail_prices[part_key]
                else:
                    extended_price = quantity * self.retail_prices[0]

                extended_price_str = '{:.2f}'.format(extended_price)
                l_discount = random.uniform(0.0, 0.10)
                discount_str = str(l_discount)
                tax = random.uniform(0.0, 0.08)
                tax_str = '{:.2f}'.format(tax)
                return_flag_str = "N"
                line_status_str = "F"
                ship_date = order_date + timedelta(random.randint(1, 121))
                commit_date = order_date + timedelta(random.randint(30, 90))
                receipt_date = ship_date + timedelta(random.randint(1, 30))
                ship_instruct_str = instructs[random.randint(0, 3)]
                ship_mode_str = modes[random.randint(0, 6)]
                line_comment_str = InsertData.generate_random_data(
                    random.randint(10, 43))

                if(receipt_date > current_date):
                    return_flag_str = "R" if (
                        random.randint(0, 1) == 0) else "A"
                if(ship_date < current_date):
                    line_status_str = "O"

                if(line_status_str == "O"):
                    O_number = O_number + 1
                else:
                    F_number = F_number + 1

                total_price += extended_price * (1+tax)*(1-l_discount)

                line_items_collection_values = []

                values = {
                    "l_orderkey": L_order_key,
                    "l_partkey": part_key,
                    "l_suppkey": supplier_key,
                    "l_linenumber": line_number,
                    "l_quantity": quantity,
                    "l_extendedprice": extended_price,
                    "l_discount": l_discount,
                    "l_tax": tax,
                    "l_returnflag": return_flag_str,
                    "l_linestatus": line_status_str,
                    "l_shipdate": ship_date,
                    "l_commitdate": commit_date,
                    "l_receiptdate": receipt_date,
                    "l_shipinstruct": ship_instruct_str,
                    "l_shipmode": ship_mode_str,
                    "l_comment": line_comment_str
                }
                line_items_collection_values.append(values)

                self.db["lineitem"].insert(
                    line_items_collection_values)

            if(F_number == line_item_rows):
                order_status_str = "F"
            if(O_number == line_item_rows):
                order_status_str = "O"

            total_price_str = '{:.2f}'.format(total_price)

            line_items_collection_additional_values = []

            additional_values = {
                "order_key": key,
                "cust_key": customer_key,
                "order_status": order_status_str,
                "total_price": total_price_str,
                "order_date": order_date,
                "order_priority": priority_string,
                "clerk": clerk_string,
                "ship_priority": ship_priority,
                "comment": order_comment
            }

            line_items_collection_additional_values.append(additional_values)
            self.db["orders"].insert(
                line_items_collection_additional_values)

    def insert_to_collections(self):
        InsertData.region(self)
        InsertData.insert_nation(self)
        InsertData.insert_supplier(self)
        InsertData.insert_part(self)
        InsertData.insert_part_supplier(self)
        InsertData.insert_customer(self)
        InsertData.insert_order_line_item(self)
