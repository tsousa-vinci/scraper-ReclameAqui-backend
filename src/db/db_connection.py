import os

class mongoDBConnection:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.connection_string = os.getenv("CONNECTION_URL")
    
    def get_connection(self):
        from pymongo import MongoClient
        client = MongoClient(self.connection_string)
        return client[self.db_name]