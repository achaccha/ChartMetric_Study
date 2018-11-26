from config import Config
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
    def initializeTable(cls, conn):
        cursor = conn.cursor()
        initialize_sql = "TRUNCATE TABLE spotify_chart restart identity;"
        cursor.execute(initialize_sql)

        return True

    @classmethod
    def getPostgres(cls):
        if cls.__postgres_store == None:
            cls.__init_postgres()
        
        return cls.__postgres_store

    @classmethod
    def getDateList(cls, chart_type, country, duration):
        conn = cls.__postgres_store
        cursor = conn.cursor()
       
        query_sql = "SELECT timestp FROM spotify_chart \
            WHERE chart_type=%s and country=%s and duration=%s \
            GROUP BY timestp;"

        query_data = (chart_type, country, duration,)
        cursor.execute(query_sql, query_data)

        records = cursor.fetchall()
        update_date_list = [str(record[0]) for record in records]
        
        cursor.close()
        return update_date_list

    @classmethod
    def getLatestDate(cls, chart_type, country, duration):
        conn = cls.__postgres_store
        cursor = conn.cursor()

        query_sql = "SELECT DISTINCT timestp FROM spotify_chart \
            WHERE country=%s and chart_type=%s and duration=%s"

        query_data = (country, chart_type, duration,)
        cursor.execute(query_sql, query_data)
        records = cursor.fetchall()
        #date_list = [str(record[0]) for record in records]
        for record in records:
            print(record[0])
            print(type(record[0]))
            
        date_list.sort(reverse=True)
        if date_list:
            date = date_list[0]
        else:
            date = -1
        
        cursor.close()
        return date

    @classmethod
    def getTotalData(cls):
        conn = cls.__postgres_store
        cursor = conn.cursor()

        query_sql = "SELECT id FROM spotify_chart\
                ORDER BY id DESC LIMIT 1;"
        
        cursor.execute(query_sql)
        records = cursor.fetchone()
        total = records[0]
        
        cursor.close()
        return total

    @classmethod
    def insertData(cls, result):
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

        cursor.close()

    @classmethod
    def closeConnection(cls):
        cls.__postgres_store.close()
        return True
    
    
    @classmethod
    def isDuplicate(cls):
        conn = cls.__postgres_store
        cursor = conn.cursor()
        
        query_sql = "SELECT spotify_track_id, rank, timestp, country, chart_type, duration, count(*) \
            from spotify_chart \
            group by spotify_track_id, rank, timestp, country, chart_type, duration \
            having count(*) > 1;"

        cursor.execute(query_sql)
        records = cursor.fetchone()

        conn.commit()
        cursor.close()

        if records == None:
            return False
        else:
            return True
        
