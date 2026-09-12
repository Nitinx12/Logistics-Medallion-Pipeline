# FreightLake Architecture

Logistics medallion lakehouse: Postgres OLTP and MongoDB as simulated source
systems, a bronze/silver/gold medallion on Databricks Delta, a Postgres
serving mart for BI, orchestrated by Airflow and quality gated by dbt tests
plus Great Expectations. This document explains the four views of the system
and the design decision behind each layer. Step by step pipeline detail lives
in `docs/PIPELINE.md`; the column level reference lives in
`docs/DATA_DICTIONARY.md`.

## 1. System context

```mermaid
flowchart LR
    PG[("Postgres OLTP<br/>freightlake_oltp<br/>9 ERP tables")]
    MG[("MongoDB<br/>freight_lake<br/>3 tracking collections")]
    BR[("bronze<br/>raw landing, append log")]
    SV[("silver<br/>cleaned, deduped, merged")]
    GD[("gold<br/>star schema")]
    MART[("Postgres mart<br/>freightlake_mart<br/>schema gold")]
    BI[["Power BI"]]
    AF{{"Airflow 3<br/>three chained DAGs"}}

    PG -->|"PySpark JDBC<br/>watermark on updated_at"| BR
    MG -->|"Spark Mongo connector<br/>watermark on updated_at"| BR
    BR -->|"dbt build tag:silver"| SV
    SV -->|"dbt build tag:silver passed<br/>dbt build tag:gold"| GD
    GD -->|"PySpark JDBC<br/>full overwrite per table"| MART
    MART --> BI
    AF -.->|"bronze DAG"| BR
    AF -.->|"silver DAG + GX gate"| SV
    AF -.->|"gold DAG + GX gate + publish"| GD

    classDef sourcepg fill:#336791,stroke:#1e4a6e,color:#fff
    classDef sourcemongo fill:#4db33d,stroke:#2e7d1f,color:#fff
    classDef bronze fill:#cd7f32,stroke:#8b5a2b,color:#fff
    classDef silver fill:#d8dee9,stroke:#7b8894,color:#222
    classDef gold fill:#ffd700,stroke:#b8860b,color:#222
    classDef mart fill:#8e44ad,stroke:#5b2c6f,color:#fff
    classDef bi fill:#e74c3c,stroke:#922b21,color:#fff
    classDef airflow fill:#017cee,stroke:#01579b,color:#fff

    class PG sourcepg
    class MG sourcemongo
    class BR bronze
    class SV silver
    class GD gold
    class MART mart
    class BI bi
    class AF airflow
```

## 2. Medallion layers

Each layer answers one question and hands a strictly better dataset to the
next. The colors below match the layer colors used across all diagrams.

```mermaid
flowchart TB
    subgraph L1["bronze: what did the source say, exactly"]
        B1["type cast for Delta compatibility"]
        B2["append only landing, no cleaning"]
        B3["watermark = MAX of updated_at<br/>read from the target table itself"]
    end
    subgraph L2["silver: what is the current clean truth"]
        S1["dedup with ROW_NUMBER<br/>PARTITION BY primary key<br/>ORDER BY updated_at DESC"]
        S2["incremental merge on primary key"]
        S3["three day lookback window<br/>to survive late updates"]
    end
    subgraph L3["gold: what does the business analyze"]
        G1["star schema, dim_ and fact_"]
        G2["SHA2 surrogate keys<br/>plus UNKNOWN member rows"]
        G3["SCD Type 2 on dim_customers<br/>via dbt snapshot"]
        G4["fact_operations daily aggregate<br/>with weighted average MPG"]
    end
    B1 --> S1
    B2 --> S2
    B3 --> S3

    classDef bronzebox fill:#cd7f32,stroke:#8b5a2b,color:#fff
    classDef silverbox fill:#d8dee9,stroke:#7b8894,color:#222
    classDef goldbox fill:#ffd700,stroke:#b8860b,color:#222
    class B1,B2,B3 bronzebox
    class S1,S2,S3 silverbox
    class G1,G2,G3,G4 goldbox
```

### Bronze design decisions

- **Watermark without a state table.** The watermark for each table is
  `MAX(updated_at)` read straight out of the target Delta table. A separate
  `etl_watermark` table would be one more thing to keep in sync and one more
  thing to lose; the target table already knows what it captured.
- **Append log, not upsert.** Bronze keeps every version of a row that the
  source ever exposed. Dedup and merge happen once, in silver, where the
  merge key is the primary key. This keeps reruns cheap and makes the raw
  layer replayable.
- **Inclusive versus exclusive bounds.** A full load includes its lower
  bound; an incremental run excludes it, because the watermark value was
  already captured inclusively last run. `build_windows()` in
  `src/jobs/pg_extract_incremental.py` encodes this; the tied timestamp edge
  case (every row sharing one `updated_at` on bulk seeded data) is handled
  and tested.
- **Chunking.** The `[start, end]` range is split into seven day windows,
  each read and written on its own, with four parallel JDBC connections per
  window and a fetch size of 50000. A failing chunk can be rerun without
  touching earlier ones.

### Silver design decisions

- **Incremental merge with a safety lookback.** Every silver model selects
  source rows at or newer than `MAX(updated_at)` from itself minus three
  days, then merges on the primary key. The lookback absorbs late arriving
  updates and clock skew; the merge makes overlap harmless.
- **Dedup before merge.** `ROW_NUMBER() OVER (PARTITION BY <primary key>
  ORDER BY updated_at DESC)` keeps only the newest version, so the merge
  never has to resolve conflicts.
- **Schema evolution.** `on_schema_change='sync_all_columns'` so a new
  source column flows through without a manual migration.

### Gold design decisions

- **Star schema only.** Seven dimensions, seven facts, one daily aggregate
  (`fact_operations`). No gold table outside the `dim_`/`fact_` shape.
- **Surrogate keys with UNKNOWN members.** Dimension keys are `SHA2` hashes.
  Every dimension carries an `UNKNOWN` row so fact foreign keys stay not null
  when a natural key has no match, with `is_unmatched_*` flags preserving the
  signal that something did not match.
- **SCD Type 2 on customers only.** `dbt/snapshots/customers_snapshot.sql`
  captures history with the timestamp strategy; `dim_customers` exposes
  `effective_from`, `effective_to`, `is_current` and a surrogate key that
  includes the validity start.
- **Weighted averages, never averages of ratios.** `fact_operations`
  computes daily MPG as total miles over total gallons, not `AVG(average_mpg)`,
  which would bias the daily figure toward short trips.

### Publish design decisions

- **Full overwrite per table.** Gold models are materialized as tables, so
  the mart publisher truncates and reinserts each table through Spark JDBC
  with a batch size of 10000 and a repartition to 4. BI tools query the
  mart, never Databricks directly.
- **Mart indexes.** `sql/serving_mart/indexes.sql` creates foreign key and
  date key indexes on every fact for the BI access patterns.

## 3. Orchestration

Three Airflow DAGs, chained with `ExternalTaskSensor`. Bronze extracts both
sources in parallel; silver and gold each run their dbt build, then a Great
Expectations gate that fails the DAG on error severity. Publish runs only
after the gold gate passes.

```mermaid
flowchart LR
    subgraph BRZ["freightlake_bronze, daily"]
        BP["bronze_pg<br/>JDBC watermark extract"]
        BM["bronze_mongo<br/>connector watermark extract"]
    end
    subgraph SLV["freightlake_silver, daily"]
        WS["wait_for_bronze<br/>ExternalTaskSensor"]
        SV["silver<br/>dbt build tag:silver"]
        GXS["gx_silver<br/>GX gate, layer silver"]
    end
    subgraph GLD["freightlake_gold, daily"]
        WG["wait_for_silver<br/>ExternalTaskSensor"]
        GD["gold<br/>dbt build tag:gold"]
        GXG["gx_gold<br/>GX gate, layer gold"]
        MP["mart_publish<br/>publish_gold_to_postgres"]
    end

    BP --> WS
    BM --> WS
    WS --> SV --> GXS --> WG --> GD --> GXG --> MP

    classDef bronzetask fill:#cd7f32,stroke:#8b5a2b,color:#fff
    classDef silvertask fill:#d8dee9,stroke:#7b8894,color:#222
    classDef goldtask fill:#ffd700,stroke:#b8860b,color:#222
    classDef sensor fill:#017cee,stroke:#01579b,color:#fff

    class BP,BM bronzetask
    class WS,SV,GXS silvertask
    class WG,GD,GXG,MP goldtask
    class WS,WG sensor
```

All three DAGs share the same `@daily` schedule and start date, so sensor
logical dates line up without an execution delta. Task commands run through
`uv run` so they use the project virtual environment inside the Airflow
container, where the scheduler's own interpreter does not carry pyspark or
dbt.

## 4. Bronze write paths

Databricks blocks managed table creation and staging credentials from
outside its own compute (HTTP 403, ErrorCode 5108 and 5105). The write
strategy is therefore configurable per run, with a documented default that
works from a laptop.

```mermaid
flowchart TB
    START["chunk DataFrame ready to write"]
    Q1{"BRONZE_WRITE_MODE<br/>from .env or --write-mode"}
    LOCAL["local Delta files<br/>under BRONZE_LOCAL_PATH<br/>offline dev, no Unity Catalog"]
    WH["Databricks SQL warehouse<br/>CREATE TABLE + batched INSERT<br/>via databricks-sql-connector"]
    UCM["Unity Catalog managed<br/>UCSingleCatalog saveAsTable<br/>only inside Databricks compute"]
    UCE["Unity Catalog external<br/>DATABRICKS_EXTERNAL_LOCATION<br/>requires storage credential"]
    Q2{"write raised<br/>403 5108 / 5105?"}
    OK["rows landed in bronze"]

    START --> Q1
    Q1 -->|local| LOCAL --> OK
    Q1 -->|warehouse| WH --> OK
    Q1 -->|uc_managed or uc_external| UCM
    Q1 -->|uc_external| UCE
    UCM --> Q2
    UCE --> Q2
    Q2 -->|"yes"| WH
    Q2 -->|"no"| OK

    classDef decision fill:#017cee,stroke:#01579b,color:#fff
    classDef bronzebox fill:#cd7f32,stroke:#8b5a2b,color:#fff
    classDef fallback fill:#e67e22,stroke:#935116,color:#fff

    class Q1,Q2 decision
    class LOCAL,UCM,UCE bronzebox
    class WH fallback
    class OK bronzebox
```

The Postgres and Mongo extraction jobs share this routing, as does the gold
to mart publisher in its local variant. `warehouse` is the default because
it is the only path verified to work from outside Databricks compute.

## 5. Quality gates

Two independent gate systems cover the same contracts:

- **dbt tests** run as part of `dbt build` inside the silver and gold DAGs.
  Generic tests under `dbt/tests/generic/` (no empty strings, no white
  spaces, no future dates, no orphan rows, accepted range, matches regex)
  plus standard not null, unique, relationships and accepted values tests.
- **Great Expectations suites** in `gx/expectations/` mirror the dbt tests
  and run against the published mart tables in Postgres, so the same rules
  validate the lakehouse and the serving layer.

Severity semantics are identical in both: `error` fails the DAG and blocks
promotion, `warn` documents a known issue (see `docs/DATA_QUALITY.md` for
the current list). The DAG tasks have no demo fallback on purpose: an
unreachable validation database is a failed gate, not a skipped one.

## 6. Non goals

Deliberately out of scope, documented so nobody fills them in as gaps: no
streaming (Kafka, Flink, Spark Structured Streaming), no row level
security, no realtime serving layer, no multi region deployment. The
project demonstrates batch medallion patterns on a laptop sized stack.
