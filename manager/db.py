from config import Config
from dateutil.parser import parse
import os
import psycopg2

class DBManager:

    __postgres_store = None

    @classmethod
    def __init__(cls):
        conn = cls.getPostgres()

    @classmethod
    def __init_postgres(cls):
        dbname = Config.postgres['dbname']
        user = Config.postgres['user']
        host = Config.postgres['host']
        password = Config.postgres['password']

        user = 'postgres'

        connect_str = "dbname='{dbname}' user='{user}' host='{host}' password='{password}'"\
            .format(dbname=dbname, user=user, host=host, password=password)

        cls.__postgres_store = psycopg2.connect(connect_str)

    @classmethod
    def getPostgres(cls):
        if cls.__postgres_store == None:
            cls.__init_postgres()
        
        return cls.__postgres_store

    @classmethod
    def closeConnection(cls):
        cls.__postgres_store.close()
        return True