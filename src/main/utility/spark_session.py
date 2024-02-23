import findspark
from src.main.utility.logging_config import logger
from pyspark.sql import SparkSession
findspark.init()

def spark_session():
    JARS_PATH = 'C:\\Users\\LPC\\Documents\\project\\gcp-connector-jar\\gcs-connector-hadoop3-latest.jar,C:\\Users\\LPC\\Documents\\project\\postgres-jar\\postgresql-42.7.1.jar'
    spark = SparkSession.builder.master("local[*]")\
            .appName("spark")\
            .config('spark.jars',JARS_PATH)\
            .getOrCreate()
    spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
    spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile",'your_service_account_path')
    spark._jsc.hadoopConfiguration().set('fs.gs.auth.service.account.enable', 'true')
    spark._jsc.hadoopConfiguration().set('fs.gs.project.id','your_gcp_project_id')
    spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.gs.impl","com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    logger.info("spark session %s",spark)


    return spark

# .config("spark.driver.extraClassPath","C:\\Users\\LPC\\Documents\\project\\postgres-jar\\postgresql-42.7.1.jar")\
