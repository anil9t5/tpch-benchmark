from neo4jp.initialize_db import InitilizeDB
import time


class Query2:
    def __init__(self):
        super().__init__()

    def execute(self):
        try:
            graphDB= InitilizeDB.init()
            start_time = time.time()

            result =graphDB.run(
                "MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]->(r:REGION) " 
                "where r.R_NAME = 'EUROPE' with p, min(p.PS_SUPPLYCOST) as minvalue "
                "MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART) "
                "MATCH (ps:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]->(r:REGION) "
                "where p.P_SIZE = 15 and p.P_TYPE =~ '.*BRASS.*' and r.R_NAME = 'EUROPE' and p.PS_SUPPLYCOST = minvalue "
                "return p.id,p.P_MFGR,s.S_ACCTBAL,s.S_NAME,s.S_ADDRESS,s.S_PHONE,s.S_COMMENT,n.N_NAME "
                "order by s.S_ACCTBAL desc,n.N_NAME,s.S_NAME,p.id; ")

            print(list(result))
            end_time = time.time()
            print("---------------Query 2-------------")
            print("Start time: " + str(start_time))
            print("End time: " + str(end_time))
            print("In seconds: " + str("{:.7f}".format(end_time - start_time)))
        except:
            print("py2neo ERROR:")

