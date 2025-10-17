import os
from pathlib import Path

import certifi
from dotenv import load_dotenv
from pymongo import MongoClient

from .utils import read_csv_records
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DEFAULT_DATABASE = "energyd2"
DEFAULT_COLLECTION = "test3"

BASE_DIR = Path(__file__).resolve().parent.parent
STAGING_DIR = BASE_DIR / "staging_data"
ELECTRICITY_FILENAME = "africa_electricity_data.csv"

def load_electrical_data(collection):
    try:
        csv_path = STAGING_DIR / ELECTRICITY_FILENAME
        records = read_csv_records(csv_path)
        if records:
            collection.insert_many(records)
            print(f"Loaded {collection.count_documents({})} documents into {collection.name} successfully!")
        else:
            print(f"No data found in {csv_path}, skipping insert.")
    except Exception as e:
        print(f"Error loading data to collection: {e}")

def get_default_collection():
    if not MONGO_URI:
        raise ValueError("MONGO_URI is not set. Please configure the MongoDB connection string.")
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return client.get_database(DEFAULT_DATABASE).get_collection(DEFAULT_COLLECTION)

if __name__ == "__main__":
    load_electrical_data(get_default_collection())
