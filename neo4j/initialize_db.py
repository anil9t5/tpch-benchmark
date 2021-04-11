import py2neo
from neo4j import GraphDatabase, basic_auth


class InitilizeDB(object):
    URI = "bolt://localhost:7687"
    DRIVER = GraphDatabase.driver(URI, auth=("neo4j", "1"))
    session = driver.session()