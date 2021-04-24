import psycopg2


class InitializeDB(object):
    DATABASE = 'tpch3'
    USER = 'postgres'
    PASSWORD = 'postgres'
    HOST = '127.0.0.1'
    PORT = '5432'

    @staticmethod
    def init():
        client = psycopg2.connect(InitializeDB.DATABASE, InitializeDB.USER, InitializeDB.PASSWORD, InitializeDB.HOST, InitializeDB.PORT)


