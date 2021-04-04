import psycopg2
import time
import random

class Query2:
    def __init__(self, conn):
        super().__init__()
        self.conn = conn
    def execute(self):
        try:
            cur = self.conn.cursor()
            #
            type_syllable3=["TIN", "NICKEL", "BRASS", "STEEL", "COPPER"]
            region_name=["AFRICA","AMERICA","ASIA","EUROPE","MIDDLE EAST"]

            size=random.randint(1, 50)
            type3=type_syllable3[random.randint(0, 4)]
            region= region_name[random.randint(0, 4)]

            # #Query Validation:
            # size = 15
            # type = "BRASS"
            # region = "EUROPE"

            command = '''select
                        s_acctbal,
                        s_name,
                        n_name,
                        p_partkey,
                        p_mfgr,
                        s_address,
                        s_phone,
                        s_comment
                        from
                        part,
                        supplier,
                        partsupp,
                        nation,
                        region
                        where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and p_size = {0}
                        and p_type like '%{1}'
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = '{2}'
                        and ps_supplycost = (
                        select min(ps_supplycost)
                        from
                        partsupp, supplier,
                        nation, region
                        where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = '{2}'
                        )
                        order by
                        s_acctbal desc,
                        n_name,
                        s_name,
                        p_partkey;'''.format(size,type3, region)
            ts = time.time()
            cur.execute(command)
            resultAll = cur.fetchall()
            cur.close()
            self.conn.commit()
            te = time.time()
            print("---------------Query 2-------------")
            print("Start time: " + str(ts))
            print("End time: " + str(te))
            print("In seconds: " + str("{:.7f}".format(te - ts)))
            #print(resultAll)

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)