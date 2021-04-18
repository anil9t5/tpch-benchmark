from neo4jp.initialize_db import InitilizeDB
import numpy as np


class DeleteData:
    # path is /var/lib/neo4j/import
    def __init__(self, db):
        super().__init__()
        self.db = db

    @staticmethod
    def delete_lineitem(self):
        names=[ "AIR", "TRUCK" , "MAIL" ,"REG AIR" , "RAIL" ,   "FOB" ,"SHIP"]
        graphDB = InitilizeDB.init()
        for name in names:
            graphDB.run(
                'MATCH (n:LINEITEM) where n.L_SHIPMODE="{0}" DETACH DELETE n; '.format(name)
            )
        print("Deleted lineitem")

    @staticmethod
    def delete_customer(self):

        numbs=(np.linspace(10000, 10))
        print(numbs)
        graphDB = InitilizeDB.init()
        for numb in numbs:
            graphDB.run(
                'MATCH (n:CUSTOMER) where n.id<{0} DETACH DELETE n; '.format(numb)
            )
        print("Deleted customer")


    def delete_nodes(self):
        #DeleteData.delete_lineitem(self)
        #DeleteData.delete_customer(self)
        pass