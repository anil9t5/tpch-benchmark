# -*- coding: utf-8 -*-
import sys
from pymongo import MongoClient

# Init Mongo Client

client = MongoClient('localhost', 27017)

# Create New Database

db = client.db_tpch


def main():
    print("List of argument strings from mongodb module: %s" % sys.argv[1:])
