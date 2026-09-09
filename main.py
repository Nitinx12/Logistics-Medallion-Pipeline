from pathlib import Path
import os

import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("data")

FILES = [
    "delivery_events.csv",
    "safety_incidents.csv",
    "maintenance_records.csv",
]

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "freight_lake"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

for filename in FILES:
    path = DATA_DIR / filename

    if not path.exists():
        print(f"SKIP: {filename}")
        continue

    collection_name = path.stem

    df = pd.read_csv(path)

    # Convert NaN to None
    df = df.where(pd.notna(df), None)

    documents = df.to_dict("records")

    if documents:
        db[collection_name].insert_many(documents)

    print(f"✓ {collection_name}: {len(documents)} documents")

client.close()

print(f"\nDone. MongoDB database: {DB_NAME}")