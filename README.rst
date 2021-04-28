
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

python3 -m <module name> # module name => mongodb, postgresql, hbase, neo4j

The above script executes, the main.py file inside the selected module, which is the initial execution point. The boolean values in the main.py could be toggled in order to perform data insert and 
query execution tasks. The program is platform independent and could run on any machine with all the necessary setup.



******TPC-H DBGen data for different Scale Factors******

Scale Factor 0.01: https://unbcloud-my.sharepoint.com/:f:/r/personal/skhosra1_unb_ca/Documents/BigData/DBGEN%20s0.01?csf=1&web=1&e=UYB7m6

Scale Factor 0.03: https://unbcloud-my.sharepoint.com/:f:/r/personal/skhosra1_unb_ca/Documents/BigData/DBGEN%20s0.03?csf=1&web=1&e=GQlCks

Scale Factor 0.5:  https://unbcloud-my.sharepoint.com/:f:/r/personal/skhosra1_unb_ca/Documents/BigData/DBGEN%20s0.5?csf=1&web=1&e=C7AgSX

Scale Factor 1:    https://unbcloud-my.sharepoint.com/:f:/r/personal/skhosra1_unb_ca/Documents/BigData/DBGEN%20s1?csf=1&web=1&e=bAxqla

*****************************************************************

******Running code in PostgreSQL ******

Install PostgreSQL, then run it: 

> sudo -u postgres psql

In PostgreSQL create a database: 

CREATE DATABASE tpch; 

Add the name of database to the database.ini and initialization module. 

In postresql folder main file set the following to True for a full run

| create_table = True
| insert_data = True
| read_from_csv = True
| run_queries = True

After inserting the data into postgresql if you wanted to export relations for Neo4j make the following true: 

export_csv_data = True

run the code by 

> python3 -m postgresql
From within the project folder tpch-benchmark-master

