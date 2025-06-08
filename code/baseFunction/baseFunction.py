import re
import os
import pyspark.sql.functions as F
import requests
from databricks.sdk.runtime import *

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

def check_file_type(path, extension, compression=None):
    """
    Kiểm tra:
    - Thư mục có chứa ít nhất 1 file với đuôi mở rộng mong muốn không?
    - Nếu là parquet và có truyền expected_compression => kiểm tra codec nén
    
    Args:
        path (str): Đường dẫn thư mục chứa file (nên là file:///dbfs/...)
        extension (str): ví dụ ".parquet", ".csv", ".json"
        spark (SparkSession): bắt buộc nếu dùng kiểm tra codec parquet
        expected_compression (str): "snappy", "gzip", "uncompressed", ...
    """
    dbfs_path = path.replace("file://", "") 
    files = os.listdir(dbfs_path)
    check_partten = extension

    if compression is not None:
        if compression == "gzip":
            check_partten = f"gz.{check_partten}"
        else:
            check_partten = f"{compression}.{check_partten}"

    files = [f for f in files if f.endswith(check_partten)]
    if not files:
        print(f"❌ Không có file định dạng {check_partten} trong {path}")
        return False
    else:
        print(f"✅ Có file định dạng {check_partten} trong {path}!")
        return True

def read_data(spark, path, extension, rowTag, compression):
    reader = spark.read

    if compression is not None:
        reader = reader.option("compression", compression)

    if extension == "parquet":
        return reader.parquet(path)
    elif extension == "csv":
        return reader.option("header", "true").csv(path)
    elif extension == "json":
        return reader.json(path)
    elif extension == "xml":
        if rowTag is None:
            print("❌ Định dạng XML yêu cầu truyền vào rowTag.")
            return None
        return reader.format("xml").option("rowTag", rowTag).load(path)
    else:
        print(f"❌ Extension '{extension}' không được hỗ trợ.")
        return None


def split_schema(schema, partition_col_names):
    """
    Trả về:
    - data_columns: list các StructField không phải partition
    - partition_columns: list các StructField là partition
    """
    data_columns = [f for f in schema.fields if f.name not in partition_col_names]
    partition_columns = [f for f in schema.fields if f.name in partition_col_names]
    return data_columns, partition_columns


def check_schema_is_correct(path, base_path, spark, extension, rowTag=None, compression=None, partition_cols=[]):
    base_data = read_data(spark, base_path, extension, rowTag, compression)
    check_data = read_data(spark, path, extension, rowTag, compression)

    if base_data is None or check_data is None:
        return False

    # Tách schema
    base_data_cols, base_part_cols = split_schema(base_data.schema, partition_cols)
    check_data_cols, check_part_cols = split_schema(check_data.schema, partition_cols)

    # So sánh phần dữ liệu chính (theo thứ tự)
    if base_data_cols != check_data_cols:
        print("❌ Schema data columns không khớp (phân biệt thứ tự)")
        print("Base:", base_data_cols)
        print("Check:", check_data_cols)
        return False

    # So sánh partition columns (không phân biệt thứ tự)
    base_part_set = set((f.name, f.dataType.simpleString()) for f in base_part_cols)
    check_part_set = set((f.name, f.dataType.simpleString()) for f in check_part_cols)

    if base_part_set != check_part_set:
        print("❌ Schema partition columns không khớp (không phân biệt thứ tự)")
        print("Base:", base_part_set)
        print("Check:", check_part_set)
        return False

    print(f"✅ Schema của file tại {path} đúng (đã kiểm tra partition logic).")
    return True



def is_partitioned_path(path, spark, extension, rowTag=None, compression=None):
    input_files = read_data(spark, path, extension, rowTag, compression).inputFiles()
    
    for file_path in input_files:
        # Nếu trong đường dẫn có ký tự '=' (dạng cột=giá_trị) thì là partition
        if "=" in file_path:
            print(f"✅ Folder {path} có sử dụng partition (phát hiện trong đường dẫn file):")
            # print(file_path)
            return True

    print(f"❌ Folder {path} KHÔNG sử dụng partition.")
    return False

def check_number_of_files(path, extension, rowTag=None, compression=None, desired_number_of_files=None):
    dbfs_path = f"dbfs:/FileStore/{path}"
    
    # List only files in the current directory (non-recursive)
    files = dbutils.fs.ls(dbfs_path)

    # Filter files by extension and optional compression
    matched_files = [
        f for f in files
        if f.name.endswith(extension) and
           (compression is None or f.name.endswith(compression + extension))
    ]

    actual_count = len(matched_files)
    print(f"Found {actual_count} file(s) with extension '{extension}' (expected: {desired_number_of_files})")

    if actual_count == desired_number_of_files:
        print("✅ File count matches.")
        return True
    else:
        print("❌ File count does not match.")
        return False


def check_sorted_descending(path, column_name, spark):
    df = spark.read.parquet(path)
    values = [row[column_name] for row in df.select(column_name).collect()]
    if all(values[i] >= values[i+1] for i in range(len(values)-1)):
        print(f"✅ Dữ liệu đang được sắp xếp giảm dần!")
        return True
    else:
        print(f"❌ Dữ liệu đang KHÔNG sắp xếp giảm dần!")
        return False

import re

def check_content_files(path, base_path, spark, extension, rowTag=None, compression=None, partition_cols=[]):
    try:
        # Đọc dữ liệu
        base_data = read_data(spark, base_path, extension, rowTag, compression)
        check_data = read_data(spark, path, extension, rowTag, compression)

        # Xây dựng danh sách cột chuẩn: giữ nguyên thứ tự cột gốc (không có partition)
        base_cols = [f.name for f in base_data.schema.fields]
        # Thêm partition cols nếu thiếu (Spark có thể tự thêm vào cuối)
        for pcol in partition_cols:
            if pcol not in base_cols:
                base_cols.append(pcol)

        # Reorder các dataframe theo cùng thứ tự cột
        base_data = base_data.select(base_cols)
        check_data = check_data.select(base_cols)

        # So sánh nội dung
        missing_from_check = base_data.exceptAll(check_data)
        extra_in_check = check_data.exceptAll(base_data)

        missing_count = missing_from_check.count()
        extra_count = extra_in_check.count()

        if missing_count == 0 and extra_count == 0:
            print("✅ Dữ liệu chính xác!")
            return True
        else:
            print("❌ Dữ liệu không chính xác!")
            if missing_count > 0:
                print(f"⚠️ Thiếu {missing_count} dòng so với dữ liệu gốc.")
            if extra_count > 0:
                print(f"⚠️ Thừa {extra_count} dòng so với dữ liệu gốc.")
            
            return False

    except Exception as e:
        print(f"⚠️ Lỗi trong quá trình xử lý: {e}")

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

