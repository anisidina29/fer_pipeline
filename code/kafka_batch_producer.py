import os
import base64
import json
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

# === Cấu hình Kafka ===
try:
    producer = KafkaProducer(
        bootstrap_servers='172.25.195.86:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print("Lỗi khi kết nối Kafka:", e)
    exit(1)

# === Thư mục chứa ảnh ===
image_folder = os.path.join(os.path.dirname(__file__), "..", "Dataset", "angry")
image_folder = os.path.abspath(image_folder)

# === Gửi từng ảnh vào Kafka ===
for filename in os.listdir(image_folder):
    if filename.endswith(".jpg"):
        path = os.path.join(image_folder, filename)
        frame_id = os.path.splitext(filename)[0]

        with open(path, "rb") as img_file:
            image_bytes = img_file.read()
            image_b64 = base64.b64encode(image_bytes).decode('utf-8')

        message = {
            "frame_id": frame_id,
            "timestamp": datetime.now().isoformat(),
            "image_b64": image_b64
        }

        try:
            producer.send("fer-frames", value=message)
            print(f"Đã gửi ảnh {filename}")
        except KafkaTimeoutError as e:
            print(f"Lỗi gửi ảnh {filename}: {e}")

producer.flush()
