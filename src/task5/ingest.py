import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 1. Khai báo các biến cấu hình môi trường
# Chú ý: Dùng 127.0.0.1 thay vì localhost để ép WSL dùng IPv4 kết nối tới Docker Windows
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_TOPIC = "cpg-metadata"  # Topic nhận metadata sự kiện từ cpg_parser.py

# Cấu hình MongoDB — MongoDB Spark Connector v10.x yêu cầu tách riêng:
#   - Base URI (chỉ host:port)
#   - Database và Collection khai báo riêng khi writeStream
MONGO_BASE_URI   = "mongodb://127.0.0.1:27017"
MONGO_DATABASE   = "peft_db"
MONGO_COLLECTION = "source_metadata"

# Đường dẫn tuyệt đối cho checkpoint (relative path gây lỗi trên Windows)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CHECKPOINT_LOCATION = str(_PROJECT_ROOT / "checkpoints" / "task5_metadata")
os.makedirs(CHECKPOINT_LOCATION, exist_ok=True)

# 2. Khởi tạo Spark Session
print("Đang khởi tạo Spark Session...")
spark = SparkSession.builder \
    .appName("Task5_MetadataIngestion") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
    .config("spark.mongodb.write.connection.uri", MONGO_BASE_URI) \
    .config("spark.mongodb.write.database",       MONGO_DATABASE) \
    .config("spark.mongodb.write.collection",     MONGO_COLLECTION) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Định nghĩa Schema cho Metadata Events dựa trên JSON mẫu từ cpg_parser.py
metadata_schema = StructType([
    StructField("id",             StringType(),    True),
    StructField("file_path",      StringType(),    True),
    StructField("size_bytes",     IntegerType(),   True),
    StructField("sha256",         StringType(),    True),
    StructField("num_lines",      IntegerType(),   True),
    StructField("processed_at",   TimestampType(), True),
    StructField("status",         StringType(),    True),
    StructField("schema_version", StringType(),    True),
    StructField("event_time",     TimestampType(), True),
])

# 4. Đọc luồng dữ liệu từ Kafka topic cpg-metadata
print(f"Đang kết nối tới Kafka topic: {KAFKA_TOPIC}...")
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe",               KAFKA_TOPIC) \
    .option("startingOffsets",         "earliest") \
    .load()

# Giải mã dữ liệu JSON từ cột "value" của Kafka
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), metadata_schema).alias("data")) \
    .select("data.*")

# 5. Ghi luồng dữ liệu vào MongoDB
print(f"Đang bắt đầu ghi dữ liệu vào MongoDB: {MONGO_BASE_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}...")
query = parsed_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation",                CHECKPOINT_LOCATION) \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .option("spark.mongodb.write.connection.uri",  MONGO_BASE_URI) \
    .option("spark.mongodb.write.database",        MONGO_DATABASE) \
    .option("spark.mongodb.write.collection",      MONGO_COLLECTION) \
    .option("spark.mongodb.write.operationType",   "update") \
    .option("spark.mongodb.write.idFieldList",     "file_path") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("[OK] Streaming query đã khởi động. Đang chờ dữ liệu từ Kafka...")
print(f"     Topic     : {KAFKA_TOPIC}")
print(f"     MongoDB   : {MONGO_BASE_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}")
print(f"     Checkpoint: {CHECKPOINT_LOCATION}")

# Giữ cho luồng chạy liên tục cho đến khi bị dừng thủ công (Ctrl+C)
query.awaitTermination()
