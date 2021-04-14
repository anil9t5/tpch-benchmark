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
#=======================================================
    @staticmethod
    def insert_relation_customer_nation(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT'
            'LOAD CSV WITH HEADERS FROM "file:///rel_customer_nation.csv" AS row FIELDTERMINATOR "|"'
            'MATCH (customer:CUSTOMER {id: toInteger(row.C_CUSTKEY)})'
            'MATCH (nation:NATION {id: toInteger(row.C_NATIONKEY)})'
            'MERGE (customer)-[:FROM_4]->(nation);')

    @staticmethod
    def insert_relation_lineitem_orders(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_lineitem_orders.csv" AS row FIELDTERMINATOR "|" ' 
            'MATCH (lineitem:LINEITEM {L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY:' 'toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY),L_LINENUMBER:' 'toInteger(row.L_LINENUMBER) }) '
            'MATCH (orders:ORDER {id: toInteger(row.L_ORDERKEY), O_CUSTKEY: toInteger(row.O_CUSTKEY)})'
            'MERGE (lineitem)-[:BELONGS_TO_7]->(orders);')

    @staticmethod
    def insert_relation_lineitem_part(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_lineitem_part.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (lineitem:LINEITEM {L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY: ' 
            'toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY),L_LINENUMBER: ' 
            'toInteger(row.L_LINENUMBER) }) '
            'MATCH (part:PART {id: toInteger(row.L_PARTKEY), P_NAME: row.P_NAME }) '
            'MERGE (lineitem)-[:COMPOSED_BY_8]->(part); ')

    def insert_nodes(self):
        #InsertData.insert_nation_nodes(self)
        #InsertData.insert_customer_nodes(self)
        InsertData.insert_customer_nodes(self)
        InsertData.insert_customer_nodes(self)
        InsertData.insert_customer_nodes(self)



    def insert_relations(self):
        InsertData.insert_relation_customer_nation(self)
        pass