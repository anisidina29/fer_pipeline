from pymongo import MongoClient

client = MongoClient("mongodb://172.25.195.86:27017")
db = client["fer_db"]
collection = db["emotions"]

def write_metadata_to_mongo(frame_id, timestamp, emotion):
    collection.insert_one({
        "frame_id": frame_id,
        "timestamp": timestamp,
        "emotion": emotion
    })
