from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, to_timestamp, regexp_replace, when, from_json, lpad, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


import requests

def get_exchange_rate(base_currency, target_currency):
    # URL của Exchange Rates API
    url = f"https://api.exchangerate-api.com/v4/latest/{base_currency}"
    
    try:
        # Gửi yêu cầu GET đến API
        response = requests.get(url)
        response.raise_for_status()  # Nếu có lỗi HTTP, ném ra ngoại lệ
        
        # Chuyển đổi dữ liệu JSON thành dictionary
        data = response.json()
        
        # Lấy tỉ giá từ dữ liệu
        exchange_rate = data['rates'].get(target_currency)

        
        
        if exchange_rate:
            print(f"1 {base_currency} = {exchange_rate} {target_currency}")
            return exchange_rate
        else:
            print(f"Không tìm thấy tỉ giá cho {target_currency}.")
    
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi kết nối đến API: {e}")

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

# Thiết lập mức độ log chỉ in ra các warning và error
spark.sparkContext.setLogLevel("ERROR")

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# # Lấy giá trị dữ liệu từ Kafka
data = df.select(col("value").cast(StringType()))

# # Tỷ giá hối đoái (giả sử cố định ở đây, thực tế sẽ lấy từ API hoặc cập nhật hàng ngày)
exchange_rate = get_exchange_rate("USD", "VND")

schema = StructType([
        StructField("User", StringType(), True),
        StructField("Card", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Month",StringType(), True),
        StructField("Day", StringType(), True),
        StructField("Time", StringType(), True),
        StructField("Amount", StringType(), True),
        StructField("Use Chip", StringType(), True),
        StructField("Merchant Name", StringType(), True),
        StructField("Merchant City", StringType(), True),
        StructField("Merchant State", StringType(), True),
        StructField("Zip", StringType(), True),
        StructField("MCC", StringType(), True),
        StructField("Errors?", StringType(), True),
        StructField("Is Fraud?", StringType(), True)
    ])

# Parse JSON từ Kafka
def parse_json(data,schema):
    
    # Use from_json with the schema directly
    return data.select(from_json("value", schema).alias("parsed")).select("parsed.*")

# Xử lý dữ liệu
def process_data(df, exchange_rate):
    return df \
        .filter(col("Is Fraud?") == "No") \
        .filter(col("Use Chip") != "Online Transaction") \
        .withColumn("Amount", regexp_replace(col("Amount"), "\\$", "").cast(DoubleType())) \
        .withColumn("Amount VND", col("Amount") * lit(exchange_rate)) \
        .withColumn(
            "Transaction Date", 
            concat(
                lpad(col("Day"), 2, "0"), lit("/"),
                lpad(col("Month"), 2, "0"), lit("/"),
                col("Year")
            )
        ) \
        .withColumn(
            "Transaction Time", 
            when(col("Time").rlike("^\\d{2}:\\d{2}$"), concat(col("Time"), lit(":00")))
            .otherwise(col("Time"))
        ) \
        .select(
            col("Card"),
            col("Transaction Date"),
            col("Transaction Time"),
            col("Merchant Name"),
            col("Merchant City"),
            col("Amount VND")
        )

# parsed_data = parse_json(data,schema)
# processed_stream = process_data(parsed_data, exchange_rate)

# # Lưu trữ dữ liệu đã xử lý
# # output_path = "output_1"
# output_path = "hdfs://localhost:9000/odap/new"
# query = processed_stream.writeStream \
#     .outputMode("append") \
#     .format("csv") \
#     .option("path", output_path) \
#     .option("checkpointLocation", "new_checkpoint") \
#     .start()
# query.awaitTermination()




# Hàm xử lý từng batch
def count_and_save_batch(batch_df, batch_id):
    # Đếm số dòng trong batch
    row_count = batch_df.count()
    print(f"Batch ID: {batch_id}, Row Count: {row_count}")
    #in ra batch
    batch_df.show()
    # Lưu batch vào HDFS
    output_path = "hdfs://localhost:9000/odap/new"
    batch_df.write.mode("append").csv(output_path)

parsed_data = parse_json(data,schema)
processed_stream = process_data(parsed_data, exchange_rate)

# Lưu trữ dữ liệu đã xử lý và đếm số dòng
query = processed_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(count_and_save_batch) \
    .option("checkpointLocation", "new_checkpoint") \
    .start()

query.awaitTermination()


# query = processed_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 --conf spark.pyspark.python=python Consumer.py

