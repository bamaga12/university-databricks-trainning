#Databricks notebook source
import pyspark.sql.functions as F
import requests
import os
import baseFunction.baseFunction as base

def run_testcase(spark, basePath, numQuestion):
    extension = "parquet"
    github_folder = f"data/result/{basePath}/{numQuestion}"
    dbfs_base_path = "/mnt/github_files/" + github_folder
    repo_owner = "bamaga12"
    repo_name = "university-databricks-trainning"
    # Get list of files and download them
    file_list = base.list_files_in_github_folder(repo_owner, repo_name, github_folder)

    for file_path in file_list:
        save_path = f"{dbfs_base_path}/{os.path.basename(file_path)}"
        base.download_github_file(repo_owner, repo_name, file_path, save_path)

    base_path = f"""file:///dbfs{dbfs_base_path}"""
    check_data_path = f"file:///dbfs/{basePath}/{numQuestion}"
    if base.check_path_exists(check_data_path):
        # if base.check_file_type(check_data_path, extension=extension):
            if base.is_partitioned_path(check_data_path, spark, extension=extension):
                if base.check_schema_is_correct(check_data_path, base_path, spark, extension=extension, partition_cols=["paymentMethod"]):
                    base.check_content_files(check_data_path, base_path, spark, extension=extension, partition_cols=["paymentMethod"])

# COMMAND ----------