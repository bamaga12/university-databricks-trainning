#Databricks notebook source
import pyspark.sql.functions as F
import requests
import os

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


def run_testcase(spark):
    github_folder_bai3 = "data/result/Test2904/Bai3"
    dbfs_base_path_bai3 = "/mnt/github_files/" + github_folder_bai3
    repo_owner = "bamaga12"
    repo_name = "university-databricks-trainning"
    # Get list of files and download them
    file_list = list_files_in_github_folder(repo_owner, repo_name, github_folder_bai3)

    for file_path in file_list:
        save_path = f"{dbfs_base_path_bai3}/{os.path.basename(file_path)}"
        download_github_file(repo_owner, repo_name, file_path, save_path)

    base_path = f"""file:///dbfs{dbfs_base_path_bai3}"""
    check_data_path = "file:///dbfs/KiemTra2904/Bai3"
    # Test Case 1: Kiểm tra đường dẫn có tồn tại hay không ?
    if check_path_exists(check_data_path):
        if check_csv_gz(check_data_path):
            if check_schema_is_correct(check_data_path, base_path, spark):
                    check_content_files(check_data_path, base_path, spark)


def check_path_exists(path):
    """
    Kiểm tra đường dẫn có tồn tại hay không
    """
    dbfs_path = path.replace("file://", "") 
    if os.path.exists(dbfs_path):
        print(f"✅ Đường dẫn {path} tồn tại!")
        return True
    else:
        print(f"❌ Đường dẫn {path} không tồn tại!")
        return False

def check_csv_gz(path):
    """
    Kiểm tra tất cả file .csv.gz trong path có đang sử dụng Snappy compression hay không
    """
    dbfs_path = path.replace("file://", "") 
    files = os.listdir(dbfs_path)
    parquet_files = [f for f in files if f.endswith(".csv.gz")]
    if not parquet_files:
        print(f"❌ Không có file định dạng .csv.gz trong {path}")
        return False
    else:
        print(f"✅ Có file định dạng .csv.gz trong {path}!")
        return True

def check_schema_is_correct(path, base_path, spark):
    base_data = spark.read.parquet(base_path)
    check_data = spark.read.parquet(path)
    base_schema = base_data.schema
    check_schema = check_data.schema

    if base_schema == check_schema:
        print(f"✅ Schema của file tại {path} giống schema của file mẫu.")
    else:
        print(f"❌ Schema không khớp!")


def check_content_files(path, base_path, spark):
    base_data = spark.read.parquet(base_path)
    check_data = spark.read.parquet(path)
    diff = check_data.union(base_data).distinct().count() - base_data.count()
    if diff == 0:
        print("✅ Dữ liệu chính xác!")
    else:
        print("❌ Dữ liệu không chính xác!")    
    
# COMMAND ----------