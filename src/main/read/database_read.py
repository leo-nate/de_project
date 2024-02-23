import logging


class DatabaseReader:
    def __init__(self,url,properties):
        self.url = url
        self.properties = properties

    def created_dataframe(self,spark,table_name):
        logging.info("url: {}".format(self.url))
        df = spark.read.jdbc(url= self.url, table= table_name, properties= self.properties)
        return df
