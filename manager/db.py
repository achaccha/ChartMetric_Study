from config import Config
import os
import psycopg2

class DBManager:

    __postgres_store = None

    @classmethod
    def __init__(cls):
        conn = cls.get_postgres()
        #return conn
        #cls.insert_db(conn, result)

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
    def get_postgres(cls):
        if cls.__postgres_store == None:
            cls.__init_postgres()
        return cls.__postgres_store

    @classmethod
    def insert(cls, result):
        conn = cls.__postgres_store
        cursor = conn.cursor()

        for item in result: 
            spotify_track_id = item[0]
            rank = item[1]
            timestp = item[2]
            country = item[3]
            chart_type = item[4]
            duration = item[5]
            
            insert_data = (spotify_track_id, rank, timestp, country, chart_type, duration,)

            insert_sql = "INSERT INTO spotify_chart (spotify_track_id, rank, timestp, country, chart_type, duration) \
                VALUES (%s, %s, %s, %s, %s, %s);"
            
            cursor.execute(insert_sql, insert_data)
            conn.commit()
            '''
            query_data = (rank, timestp, country, chart_type, duration,)
            query_sql = "SELECT COUNT(*) FROM spotify_chart \
                WHERE rank = %s and timestp = %s and country = %s and chart_type = %s and duration = %s;"
            
            # search duplicate
            cursor.execute(query_sql, query_data)
            records = cursor.fetchone()
             
            if records[0] == 0:
                cursor.execute(insert_sql, insert_data)
                conn.commit()
            '''
        cursor.close()

    @classmethod
    def initialize_table(cls, conn):
        cursor = conn.cursor()
        initialize_sql = "TRUNCATE TABLE spotify_chart restart identity;"
        cursor.execute(initialize_sql)

        return True

    @classmethod
    def close_connection(cls):
        cls.__postgres_store.close()
        return True
