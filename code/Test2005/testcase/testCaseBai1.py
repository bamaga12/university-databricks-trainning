#Databricks notebook source
import pyspark.sql.functions as F
import requests
import os
import baseFunction as base

def list_files_in_github_folder(repo_owner, repo_name, folder_path, branch="main"):
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{folder_path}?ref={branch}"
    response = requests.get(api_url)
    
    if response.status_code == 200:
        contents = response.json()
        return [item['path'] for item in contents if item['type'] == 'file']
    else:
        print(f"Failed to list files in {folder_path}, Status Code: {response.status_code}")
        return []

def download_github_file(repo_owner, repo_name, file_path, save_path, branch="main"):
    raw_url = f"https://raw.githubusercontent.com/{repo_owner}/{repo_name}/{branch}/{file_path}"
    response = requests.get(raw_url)

    if response.status_code == 200:
        dbfs_path = f"/dbfs{save_path}"
        os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)
        with open(dbfs_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {file_path} to {save_path}")
    else:
        print(f"Failed to download {file_path}, Status Code: {response.status_code}")


def run_testcase(spark, basePath, numQuestion):
    github_folder = f"data/result/{basePath}/{numQuestion}"
    dbfs_base_path = "/mnt/github_files/" + github_folder
    repo_owner = "bamaga12"
    repo_name = "university-databricks-trainning"
    # Get list of files and download them
    file_list = list_files_in_github_folder(repo_owner, repo_name, github_folder)

    for file_path in file_list:
        save_path = f"{dbfs_base_path}/{os.path.basename(file_path)}"
        download_github_file(repo_owner, repo_name, file_path, save_path)

    base_path = f"""file:///dbfs{dbfs_base_path}"""
    check_data_path = f"file:///dbfs/{basePath}/{numQuestion}"
    if base.check_path_exists(check_data_path):
        if base.check_file_type(check_data_path, extension="csv"):
            if base.check_schema_is_correct(check_data_path, base_path, spark, extension="csv"):
                    base.check_content_files(check_data_path, base_path, spark, extension="csv")

# COMMAND ----------