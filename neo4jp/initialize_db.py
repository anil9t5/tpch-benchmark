import py2neo
from neo4j import GraphDatabase

class InitilizeDB(object):
    URI = "neo4j://localhost:7687"
    DRIVER =None

    try:
        def init():
            InitilizeDB.DRIVER = GraphDatabase.driver(InitilizeDB.URI, auth=("neo4j", "1"))
            return InitilizeDB.DRIVER

        def start_session():
            return InitilizeDB.DRIVER.session()
    except:
        print("py2neo ERROR:")