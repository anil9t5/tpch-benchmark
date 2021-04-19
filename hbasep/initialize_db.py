import phoenixdb

class InitializeDB(object):
    database_url = 'http://localhost:8765/'
    autocommit = True

    @staticmethod
    def init():
        conn = phoenixdb.connect('http://localhost:8765/', autocommit=True)





