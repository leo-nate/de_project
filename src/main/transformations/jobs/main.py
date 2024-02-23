import datetime
import os
import random

from google.cloud import storage
from google.oauth2 import service_account
from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import *

from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_team_data_sql_transform import sales_team_calculation
from src.main.utility.Helper import *
from resources.dev import config
from src.main.utility.logging_config import *
from src.main.utility.spark_session import *
from src.main.utility.postgres_session import Postgres
from src.main.write.parquet_writer import DataFrameWriter

credentials = service_account.Credentials.from_service_account_file('your_service_account_path')
client = storage.Client(credentials=credentials)

bucket_name = config.bucket_name
folder_path = "files"
gcp_client_provider = Helper(client)
csv_files = gcp_client_provider.list_files_in_folder(bucket_name, folder_path)
logger.info(f"Total files in bucket: {csv_files}")

db_client = Postgres(config.database_name,config.properties["user"],
                     config.properties["password"],config.host,config.port)

connection = db_client.get_postgres_connection()
cursor = connection.cursor()


csv_files_check = [os.path.basename(file) for file in csv_files if file.endswith(".csv")]
if csv_files_check:
    csv_string = ",".join(f"'{file}'" for file in csv_files_check)
    query = f"""
    select distinct file_name from {config.project}.{config.product_staging_table}
    where status = 'Active' and file_name in ({csv_string});
    """

    logger.info(f"runtime checking query: {query}")

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        logger.info("last run was not successful still last files are there in local folder")
    else:
        logger.info("No record match")

    connection.close()
else:
    logger.info("last run was successful")

logger.info("************* Starting spark Session*************")

spark = spark_session()


logger.info("**************** Spark session created*************")

correct_files = []
for file_name in csv_files:
    data_columns = spark.read.format("csv")\
              .option("inferSchema", "true")\
              .option("mode", "permissive")\
              .option("header", "true")\
              .load(file_name).columns

    missing_columns = set(config.mandatory_columns) - set(data_columns)
    if missing_columns:
        logger.info(f"some mandatory columns is missing in file: {file_name}")
        gcp_client_provider.move_files_from_one_bucket_another_bucket(bucket_name, "files", bucket_name, "error_files", file_name, file_name)

    else:
        correct_files.append(file_name)

# Generate a random integer between a specified range (inclusive)
random_number = random.randint(1, 100000)
print(random_number)

insert_statements = []
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        insert_state = f"""
        INSERT INTO {config.project}.{config.product_staging_table}
        (file_name,file_location,created_date,status,job_id)
        values('{file_name}','{file}','{formatted_date}','Active',{random_number})
        """
        insert_statements.append(insert_state)
else:
    logger.info("No correct files found for to process")

connection = db_client.get_postgres_connection()
cursor = connection.cursor()
for statement in insert_statements:
    cursor.execute(statement)
    connection.commit()

cursor.close()
connection.close()

logger.info("******************Staging table updated successfully***************")

logger.info("***********************fixing extra columns in files******************")

schema = StructType([
    StructField("customer_id",IntegerType(), True),
    StructField("store_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("sales_date",DateType(),True),
    StructField("sales_person_id",IntegerType(),True),
    StructField("price",FloatType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("total_cost",FloatType(),True),
    StructField("additional_column",StringType(),True),
])

# final_df_to_process = spark.createDataFrame(data=[], schema=schema)
# for above getting this error: 'JavaPackage' object is not callable

database_client = DatabaseReader(config.url,config.properties)
final_df_to_process = database_client.created_dataframe(spark,"empty_df_create_table")
if correct_files:
    for file_name in correct_files:
        df = spark.read.format("csv")\
                  .option("inferSchema", "true")\
                  .option("header", "true")\
                  .load(file_name)

        extra_columns = list(set(df.columns) - set(config.mandatory_columns))

        logger.info("Extra columns at source: {}".format(extra_columns))

        if extra_columns:
            df = df.withColumn("additional_column", concat_ws(",",*extra_columns))\
                    .select("customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost","additional_column")

        else:
            df = df.withColumn("additional_column",lit(None)) \
                .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                        "total_cost", "additional_column")

        final_df_to_process = final_df_to_process.union(df)

final_df_to_process.show()

logger.info("************* creating all database dataframe's")
customer_table_df = database_client.created_dataframe(spark,config.customer_table_name)
product_staging_table_df = database_client.created_dataframe(spark,config.product_staging_table)
product_table_df = database_client.created_dataframe(spark,config.product_table)
sales_team_table_df = database_client.created_dataframe(spark,config.sales_team_table)
store_table_df = database_client.created_dataframe(spark,config.store_table)
customer_data_mart_table_df = database_client.created_dataframe(spark,config.customer_data_mart_table)
sales_team_data_mart_table_df = database_client.created_dataframe(spark,config.sales_team_data_mart_table)

logger.info("**************** completed creating all database dataframe's")

final_enriched_data = dimesions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)

logger.info("******************** Final Enriched data")
final_enriched_data.show()

final_customer_data_mart_df = final_enriched_data\
                .select("ct.customer_id","ct.first_name","ct.last_name","ct.address","ct.phone_number","ct.pincode","sales_date","total_cost")

logger.info("****************** Final customer data_mart")
final_customer_data_mart_df.show()

parquet_client = DataFrameWriter("overwrite","parquet")

logger.info("*********** writing customer data_mart to gcp bucket")
parquet_client.dataframe_writer(final_customer_data_mart_df,f"gs://{bucket_name}/{config.gcp_customer_datamart_directory}")


final_store_data_mart_df = final_enriched_data\
                .select("store_id","sales_person_id","sales_person_first_name","sales_person_last_name",
                        "store_manager_name","manager_id","is_manager",
                        "sales_person_address","sales_person_pincode",
                        expr("substring(sales_date,1,7) as sales_month"))

logger.info("****************** Final sales data_mart")
final_store_data_mart_df.show()

parquet_client = DataFrameWriter("overwrite","parquet")

logger.info("*********** writing sales data_mart to gcp bucket")
parquet_client.dataframe_writer(final_store_data_mart_df,f"gs://{bucket_name}/{config.gcp_sales_datamart_directory}")

logger.info("*************** writing data into partitions")
formatted_date_part = current_date.strftime("%Y-%m-%d")
final_store_data_mart_df.write.format("parquet")\
                    .option("header","true")\
                    .mode("overwrite")\
                    .partitionBy("sales_month","store_id")\
                    .option("path",f"gs://{bucket_name}/{config.gcp_sales_datamart_directory}/{formatted_date_part}")\
                    .save()

logger.info("**************** making customer sales data")
customer_mart_calculation_table_write(final_customer_data_mart_df)

logger.info("**************** making sales person data")
sales_team_calculation(final_enriched_data)

logger.info("************ updating stating table")
update_date = datetime.datetime.now()
formatted_update_date = update_date.strftime("%Y-%m-%d %H:%M:%S")
update_query = f"""
   update {config.product_staging_table}
   set status = 'InActive' and updated_date = {formatted_update_date}
   where job_id = {random_number}
"""

connection = db_client.get_postgres_connection()
cursor = connection.cursor()
cursor.execute(update_query)
connection.commit()
cursor.close()
connection.close()














