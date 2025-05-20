import re
import os
import pyspark.sql.functions as F
import requests

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

def read_data(spark, path, extension, rowTag=None, compression=None):
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

def check_schema_is_correct(path, base_path, spark, extension, rowTag=None, compression=None):
    
    base_data = read_data(path, base_path, spark, extension, rowTag, compression)
    check_data = read_data(path, base_path, spark, extension, rowTag, compression)

    if base_data is None or check_data is None:
        return False

    base_schema = base_data.schema
    check_schema = check_data.schema

    if base_schema == check_schema:
        print(f"✅ Schema của file tại {path} đúng.")
        return True
    else:
        print(f"❌ Schema không khớp!")
        print("Schema gốc:")
        print(base_schema)
        print("Schema kiểm tra:")
        print(check_schema)
        return False


def is_partitioned_path(path, spark):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    
    if not fs.exists(hadoop_path):
        print(f"❌ Đường dẫn không tồn tại: {path}")
        return False
    
    # Lấy danh sách thư mục con
    status = fs.listStatus(hadoop_path)
    dirs = [f.getPath().getName() for f in status if f.isDirectory()]

    # Kiểm tra xem có thư mục dạng 'key=value' không
    for d in dirs:
        if re.match(r"[^=]+=[^=]+", d):
            print(f"✅ Đường dẫn chứa partitionBy: {d}")
            return True

    print("❌ Đường dẫn KHÔNG chứa partitionBy.")
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

def check_content_files(path, base_path, spark, extension, rowTag=None, compression=None):
    try:
        # Đọc dữ liệu
        base_data = read_data(path, base_path, spark, extension, rowTag, compression)
        check_data = read_data(path, base_path, spark, extension, rowTag, compression)

        # Kiểm tra schema
        if base_data.schema != check_data.schema:
            print("❌ Schema không khớp, không thể so sánh nội dung!")
            print("🔧 Schema file gốc:")
            base_data.printSchema()
            print("🔍 Schema file kiểm tra:")
            check_data.printSchema()
            return

        # So sánh nội dung
        missing_from_check = base_data.exceptAll(check_data)
        extra_in_check = check_data.exceptAll(base_data)

        missing_count = missing_from_check.count()
        extra_count = extra_in_check.count()

        if missing_count == 0 and extra_count == 0:
            print("✅ Dữ liệu chính xác!")
        else:
            print("❌ Dữ liệu không chính xác!")
            if missing_count > 0:
                print(f"⚠️ Thiếu {missing_count} dòng so với dữ liệu gốc.")
            if extra_count > 0:
                print(f"⚠️ Thừa {extra_count} dòng so với dữ liệu gốc.")

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

