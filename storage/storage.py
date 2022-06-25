import sqlite3


class Storage:
    def __init__(self, sqlite_path: str):
        self.__connection = sqlite3.connect(sqlite_path)

    def get_connection(self):
        return self.__connection

    def get_cursor(self):
        return self.__connection.cursor()

    def close_connection(self):
        self.__connection.close()

    def execute(self, query: str):
        self.__connection.execute(query)

    def commit(self):
        self.__connection.commit()

    def __delete__(self, instance):
        self.close_connection()
