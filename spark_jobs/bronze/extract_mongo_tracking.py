"""Bronze extract — Mongo tracking feed via watermark incremental.

Auth: reads MONGO_URI from the environment (set in .env).  If authentication
fails the step exits non-zero — there is no silent fallback to an
unauthenticated connection.  A silent fallback would allow a credential
mismatch to go undetected for the entire pipeline run.
"""

from __future__ import annotations

import json
import os
import pathlib
from datetime import UTC, datetime

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

from spark_jobs.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__, "bronze")

COLLECTIONS = ["delivery_events", "safety_incidents", "maintenance_records"]
BRONZE_DIR = pathlib.Path("delta/bronze")
WATERMARK_FILE = pathlib.Path("watermarks.json")

# Read from env; fail loudly if not set — no default that could mask a
# misconfigured environment.
_MONGO_URI = os.environ.get("MONGO_URI")
if not _MONGO_URI:
    raise RuntimeError(
        "MONGO_URI is not set. "
        "Copy .env.example to .env and fill in the Mongo credentials."
    )


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


def _connect() -> MongoClient:
    """Connect to MongoDB using the configured URI.  Raises on auth failure."""
    client: MongoClient = MongoClient(_MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        # list_database_names forces authentication; raises OperationFailure on
        # bad credentials and ServerSelectionTimeoutError when unreachable.
        client.list_database_names()
    except OperationFailure as exc:
        raise RuntimeError(
            f"MongoDB authentication failed for URI {_MONGO_URI!r}. "
            "Check MONGO_URI credentials in .env and confirm they match "
            "MONGO_INITDB_ROOT_USERNAME / MONGO_INITDB_ROOT_PASSWORD used "
            "when the container volume was first created. "
            "If the volume was initialised with a different password, run: "
            "  docker compose -f docker/compose.yml down -v  (data loss!)"
            "  docker compose -f docker/compose.yml up -d mongo"
        ) from exc
    except ServerSelectionTimeoutError as exc:
        raise RuntimeError(
            "MongoDB is not reachable. Start the container with: make docker-up"
        ) from exc
    return client


def extract_collection(name: str) -> None:
    wm = watermark_get(name)
    logger.info("Extracting mongo collection=%s watermark %s", name, wm)
    client = _connect()
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
