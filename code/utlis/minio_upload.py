from minio import Minio
from minio.error import S3Error
import cv2
import os

client = Minio(
    "172.25.195.86:9000",
    access_key="minio",
    secret_key="minio123",
    secure=False
)

bucket_name = "annotated"

# Ensure the bucket exists
if not client.bucket_exists(bucket_name):
    client.make_bucket(bucket_name)
    print(f"[INFO] Bucket '{bucket_name}' created in MinIO.")

def upload_image_to_minio(image, frame_id, emotion):
    filename = f"{frame_id}_{emotion}.jpg"
    filepath = f"/tmp/{filename}"
    cv2.imwrite(filepath, image)
    try:
        client.fput_object(bucket_name, filename, filepath)
        print(f"[INFO] Image uploaded to MinIO: {filename}")
    except S3Error as e:
        print(f"[ERROR] Failed to upload {filename} to MinIO: {e}")
    finally:
        os.remove(filepath)
