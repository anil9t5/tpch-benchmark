# -*- coding: utf-8 -*-
import sys
from neo4jp.initialize_db import InitilizeDB

def main():
    print("Neo4j db started...")
    db = InitilizeDB.init()






    #db.run()
    #session = db.session()

    #session.close()
    print("Successful!")

