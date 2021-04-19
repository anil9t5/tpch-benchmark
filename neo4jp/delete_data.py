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


        graphDB.run(
            'MATCH (n:PARTSUPP) WITH n LIMIT 400000 DETACH DELETE n; '
        )
        print("Deleted node")


    def delete_nodes(self):

        # DeleteData.delete_node(self)
        pass