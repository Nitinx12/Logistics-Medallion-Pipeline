"""Bronze extract — Mongo tracking feed via watermark incremental."""

from __future__ import annotations

import json
import pathlib
from datetime import UTC, datetime

import pandas as pd
from pymongo import MongoClient

from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__, "bronze")

COLLECTIONS = ["delivery_events", "safety_incidents", "maintenance_records"]
BRONZE_DIR = pathlib.Path("delta/bronze")
WATERMARK_FILE = pathlib.Path("watermarks.json")


def watermark_get(coll: str) -> str:
    if WATERMARK_FILE.exists():
        try:
            return json.loads(WATERMARK_FILE.read_text()).get(
                f"mongo:{coll}", "1970-01-01T00:00:00"
            )
        except (json.JSONDecodeError, OSError):
            return "1970-01-01T00:00:00"
    return "1970-01-01T00:00:00"


def watermark_set(coll: str, ts: str) -> None:
    data = json.loads(WATERMARK_FILE.read_text()) if WATERMARK_FILE.exists() else {}
    data[f"mongo:{coll}"] = ts
    WATERMARK_FILE.write_text(json.dumps(data, indent=2))


def extract_collection(name: str) -> None:
    wm = watermark_get(name)
    logger.info("Extracting mongo collection=%s watermark %s", name, wm)
    client = None
    for uri in [
        "mongodb://root:changeme@localhost:27017/freightlake_tracking?authSource=admin",
        "mongodb://localhost:27017",
    ]:
        try:
            c = MongoClient(uri, serverSelectionTimeoutMS=2000)
            c.list_database_names()
            client = c
            break
        except Exception:  # noqa: BLE001, S112
            continue
    if client is None:
        logger.warning("Mongo not reachable for %s", name)
        return
    db = client["freight_lake"]
    # try watermark on event_ts else updated_at
    docs = list(db[name].find({"event_ts": {"$gt": wm}}, {"_id": 0}))
    if not docs:
        docs = list(db[name].find({"updated_at": {"$gt": wm}}, {"_id": 0}))
    if not docs:
        # first run fallback: all docs
        if not WATERMARK_FILE.exists() or f"mongo:{name}" not in json.loads(
            WATERMARK_FILE.read_text() if WATERMARK_FILE.exists() else "{}"
        ):
            docs = list(db[name].find({}, {"_id": 0}))
        else:
            logger.info("  %s 0 new docs", name)
            client.close()
            return
    df = pd.DataFrame(docs)
    df.columns = [c.strip() for c in df.columns]
    df["_loaded_at"] = datetime.now(UTC)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    out = BRONZE_DIR / f"{name}.parquet"
    # MERGE on event_id / incident_id / maintenance_id
    pk = df.columns[0]
    if out.exists() and not df.empty:
        existing = pd.read_parquet(out)
        combined = pd.concat([existing, df], ignore_index=True).drop_duplicates(
            subset=[pk], keep="last"
        )
        combined.to_parquet(out, index=False)
        logger.info("  %s MERGE %s docs (upsert on %s) -> %s", name, len(df), pk, out)
    else:
        df.to_parquet(out, index=False)
        logger.info("  %s %s docs -> %s", name, len(df), out)
    if "event_ts" in df.columns:
        watermark_set(name, str(df["event_ts"].max()))
    elif "updated_at" in df.columns:
        watermark_set(name, str(df["updated_at"].max()))
    client.close()


def main() -> None:
    logger.info("Bronze Mongo extract start collections=%s", COLLECTIONS)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    for c in COLLECTIONS:
        extract_collection(c)
    logger.info("Bronze Mongo extract complete")


if __name__ == "__main__":
    main()
