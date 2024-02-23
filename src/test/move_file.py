from resources.dev import config
from src.main.transformations.jobs.main import client
from src.main.utility.Helper import Helper
from src.main.utility.logging_config import logger

bucket_name = config.bucket_name
folder_path = "files"
gcp_client_provider = Helper(client)
source_bucket = client.get_bucket(bucket_name)
dest_bucket = client.get_bucket(bucket_name)
source_blob_name = f' files/sales_data.csv'
dest_blob_name = f'error_files/sales_data.csv'
source_blob = source_bucket.blob(source_blob_name)
# dest_blob = source_blob.copy_blob(source_blob,dest_bucket,dest_blob_name)
dest_blob = dest_bucket.copy_blob(source_blob,dest_bucket,dest_blob_name)
source_blob.delete()
logger.info(f"file: sales_data.csv moved from source_bucket: {source_bucket} to dest_bucket: {dest_bucket}")