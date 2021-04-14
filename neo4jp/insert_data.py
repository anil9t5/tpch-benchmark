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
            'CREATE CONSTRAINT ON (c:CUSTOMER) ASSERT c.id IS UNIQUE;'
        )

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
            'customer.C_COMMENT = line.C_COMMENT; '
        )

        @staticmethod
        def insert_lineItem_nodes(self):
            graphDB = InitilizeDB.init()
            graphDB.run(
                'USING PERIODIC COMMIT '
                'LOAD CSV WITH HEADERS '
                'FROM "file:///lineitem.csv" AS line FIELDTERMINATOR "|" '
                'SPLIT(line.L_SHIPDATE," / ") AS SHIPDATE, '
                'SPLIT(line.L_COMMITDATE," / ") AS COMMITDATE, '
                'SPLIT(line.L_RECEIPTDATE," / ") AS RECEIPTDATE '
                
                'CREATE (lineItem:LINEITEM) '
                'SET lineItem.L_ORDERKEY = toInteger(line.L_ORDERKEY), '
                '    lineItem.L_PARTKEY = toInteger(line.L_PARTKEY),	'
                '    lineItem.L_SUPPKEY = toInteger(line.L_SUPPKEY),'
                '    lineItem.L_LINENUMBER = toInteger(line.L_LINENUMBER),'
                '    lineItem.L_QUANTITY = toFloat(line.L_QUANTITY),'
                '    lineItem.L_EXTENDEDPRICE = toFloat(line.L_EXTENDEDPRICE),'
                '    lineItem.L_DISCOUNT = toFloat(line.L_DISCOUNT),'
                '    lineItem.L_TAX = toFloat(line.L_TAX),'
                '    lineItem.L_RETURNFLAG = line.L_RETURNFLAG,'
                '    lineItem.L_LINESTATUS = line.L_LINESTATUS,'
                '    lineItem.L_DISCOUNT = line.L_DISCOUNT,'
                '    lineItem.L_SHIPDATE = line.L_SHIPDATE,'
                '    lineItem.L_COMMITDATE = line.L_COMMITDATE,'
                '    lineItem.L_DISCOUNT = line.L_DISCOUNT,'
                '    lineItem.L_RECEIPTDATE = line.L_RECEIPTDATE,'
                '    lineItem.L_SHIPINSTRUCT = line.L_SHIPINSTRUCT,'
                '    lineItem.L_SHIPMODE = line.L_SHIPMODE,'
                '    lineItem.L_COMMENT = line.L_COMMENT,'
                '    lineItem.L_SHIPDATE_DAY = TOINT(SHIPDATE[0]),'
                '    lineItem.L_SHIPDATE_MONTH = TOINT(SHIPDATE[1]),'
                '    lineItem.L_SHIPDATE_YEAR = TOINT(SHIPDATE[2]),'
                '    lineItem.L_COMMITDATE_DAY = TOINT(COMMITDATE[0]),'
                '    lineItem.L_COMMITDATE_MONTH = TOINT(COMMITDATE[1]),'
                '    lineItem.L_COMMITDATE_YEAR =TOINT(COMMITDATE[2]),'
                '    lineItem.L_RECEIPTDATE_DAY = TOINT(RECEIPTDATE[0]),'
                '    lineItem.L_RECEIPTDATE_MONTH = TOINT(RECEIPTDATE[1]),'
                '    lineItem.L_RECEIPTDATE_YEAR = TOINT(RECEIPTDATE[2]), '
                'CREATE INDEX ON :LINEITEM(L_SUPPKEY) '
                'CREATE INDEX ON :LINEITEM(L_ORDERKEY) '
                'CREATE INDEX ON :LINEITEM(L_PARTKEY) '
                'CREATE INDEX ON :LINEITEM(L_LINENUMBER); '
            )

    @staticmethod
    def insert_region_nodes(self):
        graphDB = InitilizeDB.init()
        graphDB.run(
            'CREATE CONSTRAINT ON (r:REGION) ASSERT r.id IS UNIQUE;'
        )
        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS '
            'FROM "file:///region.csv" AS line FIELDTERMINATOR "|"'
            'CREATE (region:REGION { id: TOINTEGER(line.R_REGIONKEY) }) '
            'SET region.R_NAME = line.R_NAME,	'
            '    region.R_COMMENT = line.R_COMMENT; '
            )

#=======================================================
    @staticmethod
    def insert_relation_customer_nation(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT'
            'LOAD CSV WITH HEADERS FROM "file:///rel_customer_nation.csv" AS row FIELDTERMINATOR "|"'
            'MATCH (customer:CUSTOMER {id: toInteger(row.C_CUSTKEY)})'
            'MATCH (nation:NATION {id: toInteger(row.C_NATIONKEY)})'
            'MERGE (customer)-[:FROM_4]->(nation);'
        )

    @staticmethod
    def insert_relation_lineitem_orders(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_lineitem_orders.csv" AS row FIELDTERMINATOR "|" ' 
            'MATCH (lineitem:LINEITEM {L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY:' 'toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY),L_LINENUMBER:' 'toInteger(row.L_LINENUMBER) }) '
            'MATCH (orders:ORDER {id: toInteger(row.L_ORDERKEY), O_CUSTKEY: toInteger(row.O_CUSTKEY)})'
            'MERGE (lineitem)-[:BELONGS_TO_7]->(orders);'
        )

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
            'MERGE (lineitem)-[:COMPOSED_BY_8]->(part); '
        )

    @staticmethod
    def insert_relation_lineitem_partsupp(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_lineitem_partsupp.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (lineitem:LINEITEM {L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY: ' 'toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY),L_LINENUMBER: ' 'toInteger(row.L_LINENUMBER) }) '
            'MATCH (partsupp:PARTSUPP {PS_PARTKEY:toInteger(row.PS_PARTKEY) , PS_SUPPKEY: ' 'toInteger(row.PS_SUPPKEY)}) '
            'MERGE (lineitem)-[:COMPOSED_BY_6]->(partsupp); '
        )

    @staticmethod
    def insert_relation_lineitem_supplier(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_lineitem_supplier.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (lineitem:LINEITEM {L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY:' 'toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY),L_LINENUMBER:' 'toInteger(row.L_LINENUMBER)  })'
            'MATCH (supplier:SUPPLIER {id: toInteger(row.L_SUPPKEY)})'
            'MERGE (lineitem)-[:SUPPLIED_BY_9]->(supplier);'
        )

    @staticmethod
    def insert_relation_nation_region(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_nation_region.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (nation:NATION {id: toInteger(row.N_NATIONKEY)}) '
            'MATCH (region:REGION {id: toInteger(row.N_REGIONKEY)}) '
            'MERGE (nation)-[:FROM_10]->(region);'
        )





#---------------------------------------------------
    def insert_nodes(self):
        #InsertData.insert_nation_nodes(self)
        #InsertData.insert_customer_nodes(self)
        InsertData.insert_customer_nodes(self)
        InsertData.insert_customer_nodes(self)
        InsertData.insert_customer_nodes(self)



    def insert_relations(self):
        InsertData.insert_relation_customer_nation(self)
        pass