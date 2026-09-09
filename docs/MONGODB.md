# MongoDB in FreightLake

MongoDB 7 simulates a high-volume, semi-structured tracking and IoT feed. It runs in Docker as `freightlake-mongo` on port `27017`.

---

## Why MongoDB?

Nearly every real logistics platform has this exact split: a relational system of record for orders and master data, plus a high-volume, loosely structured event stream for tracking. Modeling both sources is the core design exercise of this project. MongoDB's document model allows variable fields per document, which deliberately creates schema evolution scenarios handled by Delta's native schema merge.

---

## Database: `freight_lake`

Three collections hold the simulated tracking data. All are seeded from CSV files in `data/` by `scripts/seed.py`.

### Collections

| Collection | Source CSV | Description | Approximate Docs |
|---|---|---|---|
| `delivery_events` | `data/delivery_events.csv` | GPS pings and status transitions per shipment | ~300,000 |
| `safety_incidents` | `data/safety_incidents.csv` | Damage, delay, failed delivery, and safety reports | ~5,000 |
| `maintenance_records` | `data/maintenance_records.csv` | Truck maintenance and inspection logs | ~10,000 |

> **Note on naming:** The plan in `docs/PROJECT_PLAN.md` names these `tracking_events`, `delivery_exceptions`, and `driver_app_events`. The actual implementation uses `delivery_events`, `safety_incidents`, and `maintenance_records`. The mapping is documented in `scripts/seed.py`.

### Watermark Field

The Bronze extractor queries by `event_ts` first, falling back to `updated_at`:

```python
# From spark_jobs/bronze/extract_mongo_tracking.py
docs = list(db[name].find({"event_ts": {"$gt": wm}}, {"_id": 0}))
if not docs:
    docs = list(db[name].find({"updated_at": {"$gt": wm}}, {"_id": 0}))
```

On first run with no watermark entry in `watermarks.json`, the extractor loads all documents as a bootstrap.

---

## Connection

The extractor tries two URIs in sequence — authenticated first, then unauthenticated fallback for local no-auth setups:

```python
for uri in [
    "mongodb://root:changeme@localhost:27017/freightlake_tracking?authSource=admin",
    "mongodb://localhost:27017",
]:
```

The `connection.py` helper exposes: `mongo_uri()` which reads `MONGO_URI` from env.

```bash
# .env.example
MONGO_URI=mongodb://root:changeme@localhost:27017/freightlake_tracking?authSource=admin
MONGO_INITDB_ROOT_USERNAME=root
MONGO_INITDB_ROOT_PASSWORD=changeme
```

---

## Seed Behavior

`scripts/seed.py` seeds MongoDB by:
1. Connecting (tries auth then no-auth)
2. For each collection: calling `db[coll].drop()` then `insert_many(docs)` from CSV

This is fully **idempotent**: dropping before inserting means reruns produce the same result.

---

## Verify Collections

```bash
# Connect via mongosh
mongosh "mongodb://root:changeme@localhost:27017/freight_lake?authSource=admin"

# Check counts
use freight_lake
db.delivery_events.countDocuments()
db.safety_incidents.countDocuments()
db.maintenance_records.countDocuments()
```
