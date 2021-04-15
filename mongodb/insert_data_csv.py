from mongodb.initialize_db import InitilizeDB
import string
import json
import random
import csv
import pandas as pd

from datetime import datetime, timedelta


class InsertDataCsv:
    csv_path = '/Users/anilhitang/Documents/winter-2021/CS6545/tpch-benchmark/mongodb/data_csv/'

    def __init__(self, scale_factor, db):
        super().__init__()
        self.db = db
        self.scale_factor = scale_factor
        self.retail_prices = {}

    @staticmethod
    def csv_to_json(filename, header=None):
        data = pd.read_csv(filename, header=header,
                           error_bad_lines=False, warn_bad_lines=False, delimiter='|')
        return data.to_dict('records')

    @staticmethod
    def region(self):
        self.db["region"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'region.csv', header=0))

    @ staticmethod
    def insert_nation(self):
        self.db["nation"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'nation.csv', header=0))

    @ staticmethod
    def insert_supplier(self):
        self.db["supplier"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'supplier.csv', header=0))

    @ staticmethod
    def insert_part(self):
        self.db["part"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'part.csv', header=0))

    @ staticmethod
    def insert_part_supplier(self):
        self.db["partsupp"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'partsupp.csv', header=0))

    @ staticmethod
    def insert_customer(self):
        self.db["customer"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'customer.csv', header=0))

    @ staticmethod
    def insert_line_item(self):
        self.db["lineitem"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'lineitem.csv', header=0))

    @ staticmethod
    def insert_orders(self):
        self.db["orders"].insert_many(InsertDataCsv.csv_to_json(
            InsertDataCsv.csv_path+'orders.csv', header=0))

    def insert_to_collections(self):
        InsertDataCsv.region(self)
        InsertDataCsv.insert_nation(self)
        InsertDataCsv.insert_supplier(self)
        InsertDataCsv.insert_part(self)
        InsertDataCsv.insert_part_supplier(self)
        InsertDataCsv.insert_customer(self)
        InsertDataCsv.insert_line_item(self)
        InsertDataCsv.insert_orders(self)
