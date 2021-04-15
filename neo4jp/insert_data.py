from neo4jp.initialize_db import InitilizeDB


class InsertData:
    def __init__(self,db):
        super().__init__()
        self.db = db

    @staticmethod
    def insert_nodes_nation(self):

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
    def insert_nodes_customer(self):
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
        def insert_nodes_lineItem(self):
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
    def insert_nodes_region(self):
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

        @staticmethod
        def insert_nodes_supplier(self):
            graphDB = InitilizeDB.init()

            graphDB.run(
                'CREATE CONSTRAINT ON (s:SUPPLIER) ASSERT s.id IS UNIQUE; '
            )

            graphDB.run(
                'USING PERIODIC COMMIT '
                'LOAD CSV WITH HEADERS  '
                'FROM "file:///supplier.csv" AS line FIELDTERMINATOR "|" '
                'CREATE (supplier:SUPPLIER { id: TOINTEGER(line.S_SUPPKEY) }) '
                'SET supplier.S_NAME = line.S_NAME, '
                '	supplier.S_ADDRESS = line.S_ADDRESS, '
                '    supplier.S_NATIONKEY = line.S_NATIONKEY, '
                '    supplier.S_PHONE = line.S_PHONE, '
                '    supplier.S_ACCTBAL = line.S_ACCTBAL, '
                '    supplier.S_COMMENT = line.S_COMMENT; '
            )

        @staticmethod
        def insert_nodes_orders(self):
            graphDB = InitilizeDB.init()
            graphDB.run(
                'CREATE CONSTRAINT ON (o:ORDER) ASSERT o.id IS UNIQUE;'
            )
            graphDB.run(
                'USING PERIODIC COMMIT '
                'LOAD CSV WITH HEADERS '
                'FROM "file:///orders.csv" AS line FIELDTERMINATOR "|" '
                'WITH DISTINCT line, SPLIT(line.O_ORDERDATE, " / ") AS date '
                
                'CREATE (order:ORDER { id: TOINTEGER(line.O_ORDERKEY) }) '
                'SET order.O_CUSTKEY = TOINTEGER(line.O_CUSTKEY),'
                '    order.O_ORDERSTATUS = line.O_ORDERSTATUS,	'
                '    order.O_TOTALPRICE = TOFLOAT(line.O_TOTALPRICE),'
                '    order.O_ORDERDATE = line.O_ORDERDATE,'
                '    order.O_ORDERPRIORITY = line.O_ORDERPRIORITY,'
                '    order.O_CLERK = line.O_CLERK,'
                '    order.O_SHIPPRIORITY = line.O_SHIPPRIORITY,'
                '    order.O_COMMENT = line.O_COMMENT,'
                '    order.O_YEAR = TOINTEGER(date[2]),'
                '    order.O_MONTH = TOINTEGER(date[1]),'
                '    order.O_DAY = TOINTEGER(date[0]) '
            )

    @staticmethod
    def insert_nodes_partsupp(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS '
            'FROM "file:///partsupp.csv" AS line FIELDTERMINATOR "|" '
            'CREATE (partsupp:PARTSUPP { PS_PARTKEY: TOINTEGER(line.PS_PARTKEY) }) '
            'SET partsupp.PS_SUPPKEY = TOINTEGER(line.PS_SUPPKEY), '
            '    partsupp.PS_AVAILQTY = TOINTEGER(line.PS_AVAILQTY), '
            '    partsupp.PS_SUPPLYCOST = TOFLOAT(line.PS_SUPPLYCOST), '
            '    partsupp.PS_COMMENT = line.PS_COMMENT,'
            
            'CREATE INDEX ON :PARTSUPP(PS_PARTKEY);'
            'CREATE INDEX ON :PARTSUPP(PS_SUPPKEY) '
        )

    @staticmethod
    def insert_nodes_part(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'CREATE CONSTRAINT ON (p:PART) ASSERT p.id IS UNIQUE;'
        )

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS '
            'FROM "file:///part.csv" AS line FIELDTERMINATOR "|" '

            'CREATE (part:PART { id: TOINTEGER(line.P_PARTKEY) }) '
            'SET part.P_NAME =line.P_NAME,'
            '    part.P_MFGR = line.P_MFGR,'
            '    part.P_BRAND = line.P_BRAND,'
            '    part.P_TYPE = line.P_TYPE,'
            '    part.P_SIZE = TOINTEGER(line.P_SIZE),'
            '    part.P_CONTAINER = line.P_CONTAINER,'
            '    part.P_RETAILPRICE =TOFLOAT( line.P_RETAILPRICE),'
            '    part.P_COMMENT = line.P_COMMENT; '

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
            'MATCH (lineitem:LINEITEM {L_ORDERKEY: toInteger(row.L_ORDERKEY), L_PARTKEY: ' 
            'toInteger(row.L_PARTKEY), L_SUPPKEY: toInteger(row.L_SUPPKEY),L_LINENUMBER: ' 
            'toInteger(row.L_LINENUMBER) }) '
            'MATCH (orders:ORDER {id: toInteger(row.L_ORDERKEY), O_CUSTKEY: toInteger(row.O_CUSTKEY)}) '
            'MERGE (lineitem)-[:BELONGS_TO_7]->(orders); '
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

    @staticmethod
    def insert_relation_nation_supplier(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_nation_supplier.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (supplier:SUPPLIER {id: toInteger(row.S_SUPPKEY)}) '
            'MATCH (nation:NATION {id: toInteger(row.S_NATIONKEY)}) '
            'MERGE (supplier)-[:BELONGS_TO_1]->(nation);'
        )

    @staticmethod
    def insert_relation_orders_customer(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_orders_customer.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (orders:ORDER {id: toInteger(row.O_ORDERKEY)}) '
            'MATCH (customer:CUSTOMER {id: toInteger(row.O_CUSTKEY)}) '
            'MERGE (orders)-[:BY_5]->(customer);'
        )

    @staticmethod
    def insert_relation_part_partsupp(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_part_partsupp.csv" AS row FIELDTERMINATOR  "|" '
            'MATCH (partsupp:PARTSUPP {PS_PARTKEY: toInteger(row.P_PARTKEY)}) '
            'MATCH (part:PART {id: toInteger(row.P_PARTKEY)}) '
            'MERGE (partsupp)-[:COMPOSED_BY_2]->(part); '
        )


    @staticmethod
    def insert_relation_supplier_partsupp(self):
        graphDB = InitilizeDB.init()

        graphDB.run(
            'USING PERIODIC COMMIT '
            'LOAD CSV WITH HEADERS FROM "file:///rel_supplier_partsupp.csv" AS row FIELDTERMINATOR "|" '
            'MATCH (partsupp:PARTSUPP {PS_PARTKEY:toInteger(row.PS_PARTKEY) , PS_SUPPKEY: ' 'toInteger(row.PS_SUPPKEY)}) '
            'MATCH (supplier:SUPPLIER {id: toInteger(row.PS_SUPPKEY)}) '
            'MERGE (partsupp)-[:SUPPLIED_BY_3]->(supplier);'
        )

#---------------------------------------------------
    def insert_nodes(self):
        #InsertData.insert_nodes_nation(self)
        #InsertData.insert_nodes_customer(self)
        pass



    def insert_relations(self):
        InsertData.insert_relation_customer_nation(self)
        pass