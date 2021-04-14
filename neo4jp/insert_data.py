from neo4jp.initialize_db import InitilizeDB


class InsertData:
    def __init__(self,db):
        super().__init__()
        self.db = db

    @staticmethod
    def insert_nation_nodes(self):

        graphDB = InitilizeDB.init()
        graphDB.run(
            'CREATE CONSTRAINT ON (n:NATION) ASSERT n.id IS UNIQUE; '
        )
        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS '
            'FROM "file:///nation.csv" AS line FIELDTERMINATOR "|" '
            'CREATE (nation:NATION { id: TOINTEGER(line.N_NATIONKEY) }) '
            'SET nation.N_NAME = line.N_NAME, '
            'nation.N_REGIONKEY = line.N_REGIONKEY, '
            'nation.N_COMMENT = line.N_COMMENT; ')

    @staticmethod
    def insert_customer_nodes(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'CREATE CONSTRAINT ON (c:CUSTOMER) ASSERT c.id IS UNIQUE;')

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS '
            'FROM "file:///customer.csv" AS line FIELDTERMINATOR "|" '
            'CREATE (customer:CUSTOMER { id: TOINTEGER(line.C_CUSTKEY) } ) '
            'SET customer.C_NAME = line.C_NAME, '
            'customer.C_ADDRESS = line.C_ADDRESS, '
            'customer.C_NATIONKEY = line.C_NATIONKEY, '
            'customer.C_PHONE = line.C_PHONE, '
            'customer.C_ACCTBAL = toFloat(line.C_ACCTBAL), '
            'customer.C_MKTSEGMENT = line.C_MKTSEGMENT, '
            'customer.C_COMMENT = line.C_COMMENT; ')

    def insert_nodes(self):
        #InsertData.insert_nation_nodes(self)
        InsertData.insert_customer_nodes(self)

    def insert_relations(self):
        pass