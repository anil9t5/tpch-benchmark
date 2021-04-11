-- CREATE CUSTOMER (id: C_CUSTKEY)

CREATE CONSTRAINT ON (c:CUSTOMER) ASSERT c.id IS UNIQUE;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS
FROM "file:///C:/datacsv/customer.csv" AS line FIELDTERMINATOR ';'

CREATE (customer:CUSTOMER { id: TOINTEGER(line.C_CUSTKEY) })
SET customer.C_NAME = line.C_NAME,
	customer.C_ADDRESS = line.C_ADDRESS,
    customer.C_NATIONKEY = line.C_NATIONKEY,
    customer.C_PHONE = line.C_PHONE,
    customer.C_ACCTBAL = toFloat(line.C_ACCTBAL),
    customer.C_MKTSEGMENT = line.C_MKTSEGMENT,
    customer.C_COMMENT = line.C_COMMENT


-- CUSTOMER (C_NATIONKEY) -> NATION (N_NATIONKEY)

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///C:/datacsv/customer_innerjoin_nation.csv" AS row FIELDTERMINATOR ';'
MATCH (customer:CUSTOMER {id: toInteger(row.C_CUSTKEY)})
MATCH (nation:NATION {id: toInteger(row.C_NATIONKEY)})
MERGE (customer)-[:FROM_4]->(nation);