import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
# aws_access_key = "your_encrypted_access_key"
# aws_secret_key = "your_encrypted_secret_key"
# bucket_name = "youtube-project-testing"
# s3_customer_datamart_directory = "customer_data_mart"
# s3_sales_datamart_directory = "sales_data_mart"
# s3_source_directory = "sales_data/"
# s3_error_directory = "sales_data_error/"
# s3_processed_directory = "sales_data_processed/"

#GCP VARIABLES
bucket_name = "your_bucket_name"
gcp_customer_datamart_directory = "customer_data_mart"
gcp_sales_datamart_directory = "sales_data_mart"
gcp_source_directory = "sales_data/"
gcp_error_directory = "sales_data_error/"
gcp_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
database_name = "project"
project = "public"
url = f"jdbc:postgresql://localhost:5433/{database_name}"
host = "localhost"
port = "5433"
properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}


#Dataset name
dataset_name = "public"

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "C:\\Users\\LPC\\Documents\\data\\file_from_gcp\\"
customer_data_mart_local_file = "C:\\Users\\LPC\\Documents\\data\\customer_data_mart\\"
sales_team_data_mart_local_file = "C:\\Users\\LPC\\Documents\\data\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\LPC\\Documents\\data\\sales_partition_data\\"
error_folder_path_local = "C:\\Users\\LPC\\Documents\\data\\error_files\\"

sales_data_to_gcp = "C:\\Users\\LPC\\Documents\\data\\sales_data_to_gcp"
