import psycopg2

from resources.dev import config


class Postgres:

    def __init__(self,dbname,dbuser,dbpassword,host,port):
        self.dbname = dbname
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.host = host
        self.port = port

    def get_postgres_connection(self):
        dbname = config.database_name
        properties = config.properties
        connection = psycopg2.connect(dbname=self.dbname,
                                      user=self.dbuser, password=self.dbpassword,
                                      host=self.host, port=self.port)
        return connection