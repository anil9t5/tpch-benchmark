
******Prequisites installed on the system******

1. MongoDB
2. PostgreSQL
3. HBase
4. Neo4j
5. Apache Pheonix docker

The project also contains a JS script "mongo-join.js", that creates a single joined collection, required to perform join operations in MongoDB.

******Libraries required for the module to Run******

1. psycopg2
2. pheonixdb
3. csv
4. py2neo
5. pymongo
6. datetime
7. pandas
8. json
9. glob

******Run individual module on terminal******

python3 -m <module name> // module name => mongodb, postgresql, hbase, neo4j

The above script executes, the main.py file inside the selected module, which is the initial execution point. The boolean values in the main.py could be toggled in order to perform data insert and 
query execution tasks. The program is platform independent and could run on any machine with all the necessary setup.



******TPC-H DBGen data for different Scale Factors******

Scale Factor 0.01: https://unbcloud-my.sharepoint.com/:f:/g/personal/skhosra1_unb_ca/EoW82ZKXreBFq2v2pEu4KfkBmP4U3rslBsEFnj-XotShPQ?e=4EBhXE

Scale Factor 0.03: https://unbcloud-my.sharepoint.com/:f:/g/personal/skhosra1_unb_ca/EjlH-lPQ0DBKivusd17GrssB-5LUzD9BlprMVvWOxuBU3A?e=rO5JKI

Scale Factor 0.5:  https://unbcloud-my.sharepoint.com/:f:/g/personal/skhosra1_unb_ca/ElDOfRoKIQ5Dn8BgjeCE43oBtO35ROw1i7vAFBT9BhyyGQ?e=y3yj3u

Scale Factor 1:    https://unbcloud-my.sharepoint.com/:f:/g/personal/skhosra1_unb_ca/EnxFDtseUFZAoe_lQNoWSrABbGU8LdaT074XNblv1wSihA?e=Hvi7ar
