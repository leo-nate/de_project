from pyspark.sql import Window
from pyspark.sql.functions import *

from resources.dev import config
from src.main.utility.logging_config import logger
from src.main.write.database_write import DatabaseWriter


def sales_team_calculation(sales_df):
    sales_final_df = sales_df.withColumn("sales_date_month",
                                           substring(col("sales_date"),1,7))\
                    .groupBy(col("store_id"),col("sales_person_id"),
                             concat("sales_person_first_name",lit(" "),col("sales_person_last_name")).alias("full_name"),
                             col("sales_date_month"))\
                    .agg(sum(col("total_cost")).alias("total_sales"))

    sales_final_df.show()

    window = Window.partitionBy("store_id","full_name","sales_date_month").orderBy(desc("total_sales"))

    sales_data_df = sales_final_df.withColumn("rank",rank().over(window))\
                    .withColumn("incentive",when(col("rank") == 1,col("total_sales") * 0.01).otherwise(lit(0)))\
                    .withColumn("sales_month",col("sales_date_month").alias("sales_month"))\
                    .select("store_id","sales_person_id","full_name","sales_month","total_sales","incentive")

    logger.info("*********** sales by sales person")
    sales_data_df.show()

    logger.info("********** writing sales dataframe to postgres")
    database_client = DatabaseWriter(config.url,config.properties)
    database_client.write_dataframe(sales_data_df,config.sales_team_data_mart_table)

