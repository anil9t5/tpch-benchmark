# -*- coding: utf-8 -*-
import sys
from mongodb.collections import Collections
from mongodb.insert_data import InsertData


def main():
    print("Mongo db commands running...")

    task = Collections()
    task.create_collections()

    insert_task = InsertData(0.05)
    insert_task.insert_to_collections()
