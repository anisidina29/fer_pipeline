from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64
from pyspark.sql.types import StructType, StringType
import base64
import cv2
import numpy as np
from fer import FER
from utils.mongo_writer import write_metadata_to_mongo
from utils.minio_upload import upload_image_to_minio

# Create Spark session
spark = SparkSession.builder \
    .appName("Spark-FER-Pipeline") \
    .getOrCreate()

# Kafka message schema
schema = StructType() \
    .add("frame_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("image_b64", StringType())

# Read from Kafka
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "fer-frames") \
    .option("startingOffsets", "earliest") \
    .load()

# Decode Kafka JSON message
df_json = df_kafka.selectExpr("CAST(value AS STRING)")
df_parsed = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")
df_with_bytes = df_parsed.withColumn("image_bytes", unbase64("image_b64"))

# Define batch processing logic
def process_batch(df, epoch_id):
    # Load entire batch into memory (driver)
    rows = df.select("frame_id", "timestamp", "image_bytes").collect()
    detector = FER(mtcnn=True)

    for row in rows:
        try:
            frame_id = row["frame_id"]
            timestamp = row["timestamp"]
            image_bytes = row["image_bytes"]

            arr = np.frombuffer(image_bytes, dtype=np.uint8)
            img = cv2.imdecode(arr, cv2.IMREAD_COLOR)

            result = detector.top_emotion(img)
            emotion = result[0] if result else "unknown"

            write_metadata_to_mongo(frame_id, timestamp, emotion)
            upload_image_to_minio(img, frame_id, emotion)

            print(f"[✓] {frame_id} - {emotion}")

        except Exception as e:
            print(f"[✗] Lỗi xử lý {row['frame_id']}: {e}")

# Run stream
df_with_bytes.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "/tmp/spark_fer_checkpoint") \
    .start() \
    .awaitTermination()
