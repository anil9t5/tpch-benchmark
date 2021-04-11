from neo4j import GraphDatabase
from py2neo import Graph

class InitilizeDB(object):
    URI = "neo4j://localhost:7687"
    #DRIVER = None
    GRAPHS = None

    try:
        def init():
            # InitilizeDB.DRIVER = GraphDatabase.driver(InitilizeDB.URI, auth=("neo4j", "1"))
            # return InitilizeDB.DRIVER
            InitilizeDB.GRAPHS = Graph("bolt://localhost:7687", auth=("neo4j", "1"))
            return InitilizeDB.GRAPHS

        # def start_session():
        #     return InitilizeDB.DRIVER.session()
    except:
        print("py2neo ERROR:")