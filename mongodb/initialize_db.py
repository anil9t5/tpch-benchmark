from pymongo import MongoClient


class InitilizeDB():
    URI = "mongodb://127.0.0.1:27017"
    DATABASE = None

    @staticmethod
    def init():
        # Creating a pymongo client
        client = MongoClient('localhost', 27017)

        # Getting the database instance
        db = client['mydb']
        print("Database created........")

        # Verification
        print("List of databases after creating new one")
        print(client.list_database_names())
