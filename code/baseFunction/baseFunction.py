import re
import os

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

def check_file_type(path, extension):
    """
    Kiểm tra tất cả file parquet trong path có đang sử dụng Snappy compression hay không
    """
    dbfs_path = path.replace("file://", "") 
    files = os.listdir(dbfs_path)
    parquet_files = [f for f in files if f.endswith(extension)]
    if not parquet_files:
        print(f"❌ Không có file định dạng {extension} trong {path}")
        return False
    else:
        print(f"✅ Có file định dạng {extension} trong {path}!")
        return True

def check_schema_is_correct(path, base_path, spark, extension, rowTag=None):
    if extension == "parquet":
        base_data = spark.read.parquet(base_path)
        check_data = spark.read.parquet(path)
    elif extension == "csv":
        base_data = spark.read.option("header", "true").csv(base_path)
        check_data = spark.read.option("header", "true").csv(path)
    elif extension == "json":
        base_data = spark.read.json(base_path)
        check_data = spark.read.json(path)
    elif extension == "xml":
        if rowTag is None:
            print("❌ Định dạng XML yêu cầu truyền vào rowTag.")
            return False
        base_data = spark.read.format("xml").option("rowTag", rowTag).load(base_path)
        check_data = spark.read.format("xml").option("rowTag", rowTag).load(path)
    else:
        print(f"❌ Extension '{extension}' không được hỗ trợ.")
        return False

    base_schema = base_data.schema
    check_schema = check_data.schema

    if base_schema == check_schema:
        print(f"✅ Schema của file tại {path} đúng.")
        return True
    else:
        print(f"❌ Schema không khớp!")
        print("Schema gốc:")
        base_schema.printTreeString()
        print("Schema kiểm tra:")
        check_schema.printTreeString()
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

def check_content_files(path, base_path, spark, extension, rowTag=None):
    def read_data(p):
        if extension == "parquet":
            return spark.read.parquet(p)
        elif extension == "csv":
            return spark.read.option("header", "true").csv(p)
        elif extension == "json":
            return spark.read.json(p)
        elif extension == "xml":
            if rowTag is None:
                raise ValueError("Với định dạng XML, cần truyền vào rowTag.")
            return spark.read.format("xml").option("rowTag", rowTag).load(p)
        else:
            raise ValueError(f"❌ Định dạng '{extension}' không được hỗ trợ.")

    try:
        # Đọc dữ liệu
        base_data = read_data(base_path)
        check_data = read_data(path)

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
