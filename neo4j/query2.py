MATCH (p:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]->(r:REGION)
where r.R_NAME = 'EUROPE' with p, min(p.PS_SUPPLYCOST) as minvalue
MATCH (ps:PARTSUPP)-[:COMPOSED_BY]->(p:PART)
MATCH (ps:PARTSUPP)-[:SUPPLIED_BY]->(s:SUPPLIER)-[:BELONG_TO]->(n:NATION)-[:IS_FROM]->(r:REGION)
where p.P_SIZE = 15 and p.P_TYPE =~ '.*BRASS.*' and r.R_NAME = 'EUROPE' and p.PS_SUPPLYCOST = minvalue
return p.id,p.P_MFGR,s.S_ACCTBAL,s.S_NAME,s.S_ADDRESS,s.S_PHONE,s.S_COMMENT,n.N_NAME
order by s.S_ACCTBAL desc,n.N_NAME,s.S_NAME,p.id;