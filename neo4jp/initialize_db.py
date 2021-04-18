from neo4j import GraphDatabase
from py2neo import Graph


class InitilizeDB(object):
    DATABASE = 'tpch'
    USER = 'neo4j'
    PASSWORD = 'admin'
    #URI = "neo4j://localhost:7687"
    URI = "bolt://localhost:7687"
    GRAPHS = None
    #DRIVER = None

    @staticmethod
    def init():

        # InitilizeDB.DRIVER = GraphDatabase.driver(InitilizeDB.URI, auth=("neo4j", "1"))
        # return InitilizeDB.DRIVER
        InitilizeDB.GRAPHS = Graph(InitilizeDB.URI, auth=(
            InitilizeDB.USER, InitilizeDB.PASSWORD))
        return InitilizeDB.GRAPHS

    # def start_session():
    #     return InitilizeDB.DRIVER.session()
