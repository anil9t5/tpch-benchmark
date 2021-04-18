from neo4jp.initialize_db import InitilizeDB
import numpy as np


class DeleteData:
    # path is /var/lib/neo4j/import
    def __init__(self, db):
        super().__init__()
        self.db = db

    @staticmethod
    def delete_node(self):
        graphDB = InitilizeDB.init()


       # [LINEITEM | CUSTOMER | NATION | REGION | SUPPLIER | ORDER | PARTSUPP | PART]

        for i in range(5):
            graphDB.run(
                "MATCH (n) "
                "WITH n LIMIT 100000 "
                "DETACH DELETE n; "

            )

        # graphDB.run(
        #     'MATCH (n:LINEITEM) WITH n LIMIT 400000 DETACH DELETE n; '
        # )
        print("Deleted node")


    def delete_nodes(self):

        DeleteData.delete_node(self)
        pass