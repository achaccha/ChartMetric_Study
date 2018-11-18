import psycopg2

class DBManager:

    __postgres_store = None

    @classmethod
    def __init_postgres(cls):
        connect_str = "dbname='spotify_chart' user='postgres' host='localhost' " + \
                      "password='min12378'"
        cls.__postgres_store = psycopg2.connect(connect_str)

    @classmethod
    def get_postgres(cls):
        if cls.__postgres_store == None:
            cls.__init_postgres()
        return cls.__postgres_store

    '''
    try:
        connect_str = "dbname='spotify_chart' user='postgres' host='localhost' " + \
                      "password='min12378'"
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)
        # create a psycopg2 cursor that can execute queries
        cursor = conn.cursor()
        # create a new table with a single column called "name"
        cursor.execute("""CREATE TABLE tutorials (name char(40));""")
        # run a SELECT statement - no data in there, but we can try it
        cursor.execute("""SELECT * from tutorials""")
        rows = cursor.fetchall()
        print(rows)
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)
    '''