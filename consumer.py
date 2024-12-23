from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, lit, to_timestamp, regexp_replace, when, from_json, lpad, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

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
exchange_rate = 24000  # 1 USD = 24,000 VND

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
        .withColumn("Transaction Time", col("Time")) \
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

parsed_data = parse_json(data,schema)
processed_stream = process_data(parsed_data, exchange_rate)

# Lưu trữ dữ liệu đã xử lý
# output_path = "output_1"
output_path = "hdfs://localhost:9000/aida/output"
query = processed_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", "new_checkpoint") \
    .start()
query.awaitTermination()


# query = processed_stream.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()