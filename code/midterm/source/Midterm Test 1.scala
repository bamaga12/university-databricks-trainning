// Databricks notebook source
// MAGIC %md-sandbox
// MAGIC
// MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
// MAGIC   <img src="https://cct.ued.vnu.edu.vn/admin//lienketlinkphai/brtunv-7_87_anhlienket.png" alt="Databricks Learning" style="width: 400px">
// MAGIC </div>
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC **Chạy phần cell dưới đây để nạp dữ liệu vào notebook (Chỉ cần chạy, không cần sửa gì ở đây)**

// COMMAND ----------

// DBTITLE 1,Download dữ liệu từ Internet về Cluster
// MAGIC %python
// MAGIC # Download dữ liệu từ Internet
// MAGIC import requests
// MAGIC import os
// MAGIC from pyspark.sql import SparkSession
// MAGIC
// MAGIC
// MAGIC def download_github_file(repo_url, file_path, save_path):
// MAGIC     """
// MAGIC     Downloads a file from GitHub and saves it to the specified location in DBFS.
// MAGIC     """
// MAGIC     file_url = f"{repo_url}/raw/main/{file_path}"
// MAGIC     response = requests.get(file_url)
// MAGIC
// MAGIC     if response.status_code == 200:
// MAGIC         dbfs_path = f"/dbfs{save_path}"
// MAGIC         os.makedirs(os.path.dirname(dbfs_path), exist_ok=True)
// MAGIC
// MAGIC         with open(dbfs_path, "wb") as file:
// MAGIC             file.write(response.content)
// MAGIC
// MAGIC         print(f"File saved to {save_path}")
// MAGIC     else:
// MAGIC         print(f"Failed to download {file_path}, Status Code: {response.status_code}")
// MAGIC
// MAGIC
// MAGIC # Example usage
// MAGIC repo_url = "https://github.com/bamaga12/university-databricks-trainning"
// MAGIC files_to_download = [
// MAGIC     ("hr_records","/data/hr_records/part-00000-1b36056b-eec9-4910-aae1-38aa70078ecc-c000.gz.parquet"),
// MAGIC     ("gp_address","/data/gp_address/part-00000-1590efac-b3c6-44fd-a2d9-6e0090f07b75-c000.gz.parquet"),
// MAGIC     ("practice_demographics", "/data/practice_demographics/part-00000-7ee96c14-ecc3-41a1-ad4f-df6bdd3f7561-c000.gz.parquet")
// MAGIC ]
// MAGIC
// MAGIC dbfs_save_dir = "/mnt/github_files"
// MAGIC
// MAGIC for name, file_path in files_to_download:
// MAGIC     download_github_file(repo_url, file_path, dbfs_save_dir + file_path)
// MAGIC     dbutils.widgets.text(name, f"file:///dbfs{dbfs_save_dir + file_path}")

// COMMAND ----------

// MAGIC %md
// MAGIC **Đọc dữ liệu (Chỉ cần chạy, không cần sửa gì ở đây)**

// COMMAND ----------

val hr_records = spark.read.parquet(dbutils.widgets.get("hr_records"))
val gp_address = spark.read.parquet(dbutils.widgets.get("gp_address"))
val practice_demographics = spark.read.parquet(dbutils.widgets.get("practice_demographics"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Bài 1:
// MAGIC #### YÊU CẦU:
// MAGIC • Sử dụng dữ liệu **'hr_records'** để tạo báo cáo về tất cả nhân viên đã gia nhập công ty trong năm 2015.
// MAGIC
// MAGIC #### MÔ TẢ DỮ LIỆU:
// MAGIC • Sử dụng Dataframe chứa dữ liệu **'hr_records'**
// MAGIC
// MAGIC #### YÊU CẦU ĐẦU RA:
// MAGIC
// MAGIC • Kết quả đầu ra cần có 4 cột:
// MAGIC `first_name`, `last_name`, tháng gia nhập `month_of_joining`
// MAGIC và năm gia nhập `year_of_joining`.
// MAGIC
// MAGIC • Sắp xếp kết quả theo thứ tự năm gia nhập, tháng gia nhập theo tăng dần.

// COMMAND ----------

val lesson1 =

// COMMAND ----------

// MAGIC %md
// MAGIC ## BÀI 2
// MAGIC #### YÊU CẦU:
// MAGIC Bộ phận bảo mật dữ liệu muốn tạo bí danh (alias) cho nhân viên để sử dụng khi đăng nhập.
// MAGIC
// MAGIC Nhiệm vụ của bạn là tạo alias tùy chỉnh theo cấu trúc sau:
// MAGIC
// MAGIC Lấy 3 chữ số đầu tiên từ số an sinh xã hội (cột 'ssn'), mã vùng của nhân viên (cột 'zip'), và 3 chữ số đầu tiên từ số điện thoại (cột 'phone_nbr'). Các phần này được nối với nhau bằng dấu hai chấm (':').
// MAGIC
// MAGIC Ví dụ alias: 275:72453:479
// MAGIC
// MAGIC #### MÔ TẢ DỮ LIỆU:
// MAGIC • Sử dụng Dataframe chứa dữ liệu **'hr_records'**
// MAGIC
// MAGIC #### YÊU CẦU ĐẦU RA:
// MAGIC
// MAGIC • Kết quả chỉ cần chứa 3 cột:
// MAGIC first_name, last_name, và alias
// MAGIC
// MAGIC #### KẾT QUẢ MẪU:
// MAGIC
// MAGIC | first_name | last_name | alias         |
// MAGIC |------------|-----------|---------------|
// MAGIC | Hermila    | Suhr      | 275:72453:479 |
// MAGIC | Antonio    | Joy       | 646:30455:229 |
// MAGIC | Sebastian  | Moores    | 499:13608:212 |
// MAGIC

// COMMAND ----------

val lesson2 =

// COMMAND ----------

// MAGIC %md
// MAGIC ## BÀI 3
// MAGIC #### YÊU CẦU:
// MAGIC Ban lãnh đạo muốn xác định những nhân viên xuất sắc trong công ty và đã yêu cầu bạn tìm các nhân viên có thu nhập cao nhất và dưới 30 tuổi.
// MAGIC
// MAGIC • Sử dụng ngày hiện tại là 01/12/2020 (2020-12-01)
// MAGIC
// MAGIC • Giả định mỗi năm có 365 ngày
// MAGIC
// MAGIC #### MÔ TẢ DỮ LIỆU:
// MAGIC • Sử dụng Dataframe chứa dữ liệu **'hr_records'**
// MAGIC
// MAGIC • Kết quả đầu ra gồm 5 cột sau:
// MAGIC first_name, last_name, tuổi hiện tại (làm tròn xuống), salary, và rank
// MAGIC
// MAGIC • Sắp xếp kết quả theo salary, từ cao đến thấp
// MAGIC
// MAGIC #### KẾT QUẢ MẪU:
// MAGIC | first_name | last_name | current_age | salary | rank |
// MAGIC |------------|-----------|--------------|--------|------|
// MAGIC | Morton     | Seaborn   | 25           | 199997 | 1    |
// MAGIC | Albertina  | Lipford   | 27           | 199995 | 2    |
// MAGIC | Francisco  | Cassano   | 28           | 199993 | 3    |

// COMMAND ----------

val lesson3 =

// COMMAND ----------

// MAGIC %md
// MAGIC ## BÀI 4
// MAGIC #### YÊU CẦU:
// MAGIC Tìm số lượng nhân viên đang sinh sống tại từng quận (county) trong dữ liệu hr_records.
// MAGIC
// MAGIC #### MÔ TẢ DỮ LIỆU:
// MAGIC • Sử dụng Dataframe chứa dữ liệu **'hr_records'**
// MAGIC
// MAGIC #### YÊU CẦU ĐẦU RA:
// MAGIC
// MAGIC • Ký tự phân cách cột là tab (\t)
// MAGIC
// MAGIC • Kết quả chỉ nên chứa một dòng cho mỗi quận (county)
// MAGIC
// MAGIC • Mỗi dòng phải hiển thị tên county và tổng số nhân viên đang sống ở đó
// MAGIC
// MAGIC #### KẾT QUẢ MẪU
// MAGIC
// MAGIC |Chelan 162 | |Putnam 986 | |Bayfield 119|
// MAGIC
// MAGIC

// COMMAND ----------

val lesson4 =

// COMMAND ----------

// MAGIC %md
// MAGIC ## BÀI 5
// MAGIC #### YÊU CẦU
// MAGIC Tạo một báo cáo hiển thị số lượng bệnh nhân đăng ký tại mỗi phòng khám GP (bác sĩ gia đình).
// MAGIC
// MAGIC #### MÔ TẢ DỮ LIỆU:
// MAGIC Thông tin về phòng khám GP được lưu trữ trong bảng metastore **gp_address** thuộc cơ sở dữ liệu **gp_db**.
// MAGIC
// MAGIC Các cột của bảng **gp_address**:
// MAGIC
// MAGIC • date: string
// MAGIC
// MAGIC • practice_code: string
// MAGIC
// MAGIC • surgery_name: string
// MAGIC
// MAGIC • address_1: string
// MAGIC
// MAGIC • address_2: string
// MAGIC
// MAGIC • address_3: string
// MAGIC
// MAGIC • address_4: string
// MAGIC
// MAGIC • postcode: string
// MAGIC
// MAGIC Thông tin nhân khẩu học của phòng khám được lưu trong bảng **practice_demographics** thuộc cơ sở dữ liệu **gp_db**.
// MAGIC
// MAGIC Cột practice_code trong bảng **practice_demographics** là khóa ngoại, tham chiếu đến khóa chính practice_code trong bảng **gp_address**.
// MAGIC
// MAGIC Các cột của bảng **practice_demographics**:
// MAGIC
// MAGIC • practice_code: string
// MAGIC
// MAGIC • postcode: string
// MAGIC
// MAGIC • nbr_of_patients: integer
// MAGIC
// MAGIC #### YÊU CẦU ĐẦU RA:
// MAGIC
// MAGIC • Kết quả cần hiển thị:
// MAGIC
// MAGIC surgery_name
// MAGIC
// MAGIC nbr_of_patients (số lượng bệnh nhân đã đăng ký)
// MAGIC
// MAGIC #### DỮ LIỆU MẪU:
// MAGIC | surgery_name                  | nbr_of_patients |
// MAGIC |------------------------------|------------------|
// MAGIC | THE DENSHAM SURGERY          | 4234             |
// MAGIC | QUEENS PARK MEDICAL CENTRE   | 19691            |
// MAGIC | VICTORIA MEDICAL PRACTICE    | 3357             |

// COMMAND ----------

val lesson5 =