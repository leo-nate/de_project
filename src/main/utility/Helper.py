import os.path
from io import BytesIO
import pandas as pd
from src.main.utility.logging_config import *

class Helper:
    def __init__(self, client):
        self.client = client

    def read_file_from_gcs(self, bucket_name, file_name):
        # get bucket
        bucket = self.client.get_bucket(bucket_name)

        # get files
        blob = bucket.blob(file_name)

        # print content
        content = blob.download_as_text()

        return content

    def list_files_in_bucket(self, bucket_name):
        # get bucket
        bucket = self.client.get_bucket(bucket_name)
        # get files
        blobs = bucket.list_blobs(prefix='')

        file_names = []
        for blob in blobs:
            file_names.append(blob.name)

        return file_names

    # def list_files_in_folder(self, bucket_name, folder_path):
    #     try:
    #         bucket = self.client.get_bucket(bucket_name)
    #         blobs = bucket.list_blobs(prefix=folder_path)
    #
    #         result_dict = {}
    #         for blob in blobs:
    #             name = str(blob.name)
    #             file_name = os.path.basename(name)
    #
    #             if not name.endswith(".csv"):
    #                 continue  # Skip non-CSV files
    #
    #             content = BytesIO(blob.download_as_text().encode('utf-8'))
    #             df = pd.read_csv(content)
    #             result_dict[file_name] = df
    #
    #         if len(result_dict) == 0:
    #             logger.info(f"No Files found at folder: {folder_path} in bucket: {bucket_name}")
    #             raise Exception("No Data Available")
    #         else:
    #             return result_dict
    #
    #     except Exception as e:
    #         logger.info(e)
    #         raise e

    def list_files_in_folder(self, bucket_name, folder_path):
        try:
            bucket = self.client.get_bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=folder_path)

            file_names = []
            for blob in blobs:
                name = str(blob.name)

                if not name.endswith(".csv"):
                    continue  # Skip non-CSV files

                file_name = f"gs://{bucket_name}/{name}"

                file_names.append(file_name)

            if len(file_names) == 0:
                logger.info(f"No Files found at folder: {folder_path} in bucket: {bucket_name}")
                raise Exception("No Data Available")
            else:
                return file_names

        except Exception as e:
            logger.info(e)
            raise e


    def upload_files_to_bucket(self,bucket_name,folder_path,local_folder_path):
        bucket = self.client.get_bucket(bucket_name)

        for file_name in os.listdir(local_folder_path):
            local_file_path = os.path.join(local_folder_path,file_name)

            if os.path.isdir(local_file_path):
                continue

            destination_blob_name = folder_path+"/"+file_name

            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(local_file_path)

            logger.info(f"file: {file_name} is created in this path: {destination_blob_name}")


    def move_files_from_one_bucket_another_bucket(self,source_bucket_name,source_file_path,dest_bucket_name,
                                                  dest_file_path,source_file_name,dest_file_name):

        source_bucket = self.client.get_bucket(source_bucket_name)

        dest_bucket = self.client.get_bucket(dest_bucket_name)

        source_blob_name = f'{source_file_path}/{os.path.basename(source_file_name)}'

        dest_blob_name = f'{dest_file_path}/{os.path.basename(dest_file_name)}'

        source_blob = source_bucket.blob(source_blob_name)

        dest_blob = source_bucket.copy_blob(source_blob,dest_bucket,dest_blob_name)

        source_blob.delete()

        logger.info(f"file: {os.path.basename(source_file_name)} moved from source_bucket: {source_bucket_name} to dest_bucket: {dest_bucket_name}")


