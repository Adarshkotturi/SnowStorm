# CityRide — Solution Architecture Document

> **Project:** Bike Rental Data Insights Hackathon — HCL Tech  
> **Version:** 1.0  
> **Date:** 2026-03-30  
> **Author:** Team SnowPro

---

## 1. Problem Statement

CityRide (bike-share operator) needs a **data engineering and analytics platform** built entirely on Snowflake to:

- Ingest station, bike, rental, and user data (CSV + NDJSON, master + incremental files)
- Apply data quality checks — validate emails, phones, GPS coordinates, battery levels, timestamps, and distance ranges
- Normalize denormalized source data into a clean relational schema with SCD Type-2 for dimensions
- Compute 5 business KPIs (Anomalous Rental Probability Score, Station Availability Score, Active Rider Engagement Ratio, Fleet Maintenance Health Index, Average Rental Revenue by Channel)
- Implement enterprise-grade security — RBAC, column-level PII masking, row-level filtering by region/city_zone
- Handle incremental loads automatically using CDC (Streams + Tasks)

All of this must be accomplished using **Snowflake-native capabilities only** — no external tools, no ETL frameworks, no orchestrators.

---

## 2. Solution Approach

We implement a **3-layer medallion architecture** inside a single Snowflake database (`CITYRIDE_DB`), with each layer serving a distinct purpose:

| Layer | Schema | Purpose | Key Objects |
|-------|--------|---------|-------------|
| **RAW** | `RAW` | Ingest source files exactly as received, zero transformation | Internal stages, file formats, COPY INTO, raw tables |
| **VALIDATED** | `VALIDATED` | Type-cast, validate, deduplicate, flag anomalies & DQ errors | JavaScript UDFs, MERGE, validation + anomaly flags |
| **CURATED** | `CURATED` | Normalize into relational tables with SCD2 and derived KPI fields | dim_stations, dim_bikes, dim_users, fact_rentals + views |

Two additional schemas provide governance and analytics:

| Schema | Purpose |
|--------|---------|
| `ANALYTICS` | 5 KPI views + anomaly rule catalog |
| `SECURITY` | Role mapping table, masking policies, row access policies |
| `AUDIT` | Append-only operation log for full traceability |

---

## 3. Architecture Diagram

```
  ┌──────────────────────────────────────────────────────────────────────────────┐
  │                         CITYRIDE_DB                                          │
  │                                                                              │
  │  ┌──────────────┐   ┌────────────┐   ┌────────────┐   ┌────────────────┐   │
  │  │  SOURCE FILES │──▶│    RAW     │──▶│ VALIDATED  │──▶│   CURATED      │   │
  │  │ CSV + NDJSON  │PUT│  Schema    │CDC│  Schema    │MRG│  Schema        │   │
  │  └──────────────┘CPY│            │   │            │   │ dim_stations   │   │
  │                 INTO│ raw_        │   │ val_        │   │ dim_bikes      │   │
  │                     │ stations   │   │ stations   │   │ dim_users      │   │
  │                     │ raw_bikes  │   │ val_bikes  │   │ fact_rentals   │   │
  │                     │ raw_rentals│   │ val_rentals│   │ + Views        │──┐│
  │                     │ raw_users  │   │ val_users  │   └────────────────┘  ││
  │                     └─────┬──────┘   └────────────┘                       ││
  │                           │                                                ││
  │                      STREAMS                           ┌────────────────┐  ││
  │                   (Append-Only)                        │   ANALYTICS    │◀─┘│
  │                           │                           │   Schema       │   │
  │                      ┌────▼────┐                      │ v_kpi1–v_kpi5  │   │
  │                      │  TASKS  │                      │ v_anomaly_rules│   │
  │                      │  (DAG)  │                      └────────────────┘   │
  │                      └─────────┘                                           │
  │  ┌─────────────────────────────────────────────────────────────────────┐   │
  │  │ SECURITY Schema                │  AUDIT Schema                      │   │
  │  │ • 3 Roles (RBAC)               │  • audit_log (append-only)         │   │
  │  │ • Masking: email, phone, GPS   │  • op type, table, rows, user, ts  │   │
  │  │ • Row Access by region/zone    │                                    │   │
  │  └─────────────────────────────────────────────────────────────────────┘   │
  └──────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Data Sources

| Source | Format | Volume | Content |
|--------|--------|--------|---------|
| `stations_master.csv` | CSV | ~200 rows | Station ID, name, lat/lon, capacity, city_zone, status |
| `stations_inc.csv` | CSV | ~20 rows | Incremental station updates |
| `bikes_master.csv` | CSV | ~300 rows | Bike ID, type, battery, odometer, firmware, service dates |
| `bikes_inc.csv` | CSV | ~30 rows | Incremental bike updates |
| `rentals_master.csv` / `rentals_master.ndjson` | CSV + NDJSON | ~500 rows | Rental events with GPS, channel, device info |
| `rentals_inc.csv` | CSV | ~50 rows | Incremental rental records |
| `users_master.csv` | CSV | ~300 rows | Rider PII, KYC, plan, corporate_id |
| `users_inc.csv` | CSV | ~30 rows | Incremental user updates |

**Key challenges:** Rentals contain nested GPS fields; bikes require battery/odometer range enforcement; users have PII requiring masking; anomaly detection requires multi-rule business logic.

---

## 5. Layer-by-Layer Design

### 5.1 RAW Layer — Zero-Touch Ingestion

**Goal:** Land files exactly as received. No transformation, no type casting.

- **Stages:** Four internal stages — `raw_stations_stage`, `raw_bikes_stage`, `raw_rentals_stage`, `raw_users_stage`
- **File Formats:** `csv_format` (skip header, handle NULLs) and `ndjson_format` (NDJSON line-by-line parsing)
- **Tables:**
  - `raw_stations` — all VARCHAR + 3 metadata columns (`_loaded_at`, `_source_file`, `_row_number`)
  - `raw_bikes` — all VARCHAR + 3 metadata columns
  - `raw_rentals` — all VARCHAR + 3 metadata columns (NDJSON flattened into columns via COPY INTO SELECT)
  - `raw_users` — all VARCHAR + 3 metadata columns
- **Loading:** `COPY INTO` with `ON_ERROR = 'CONTINUE'` (bad rows skipped), `FORCE = FALSE` (idempotent)
- **Streams:** Append-only streams on all four tables capture every new row for downstream processing

**Why this matters:** Raw data is immutable. Any downstream failure can always be reprocessed from RAW without re-uploading files.

### 5.2 VALIDATED Layer — Cleansing, Quality & Anomaly Flags

**Goal:** Type-cast, validate, standardize, flag every record, and apply all anomaly business rules.

- **Type Casting:** Strings → DATE, TIMESTAMP, NUMBER, BOOLEAN using `TRY_TO_*` safe functions
- **Standardization:** `INITCAP(names)`, `UPPER(status)`, `LOWER(email)`, `TRIM()` everywhere; timestamps normalized to UTC; GPS precision standardized to 6 decimal places
- **JavaScript UDFs** (6 functions):
  - `udf_validate_email` — regex email format check
  - `udf_validate_phone` — strips non-digits, checks 10/11 digit format
  - `udf_validate_gps` — checks lat/lon within valid global bounds
  - `udf_validate_battery` — enforces battery_level 0–100
  - `udf_is_ultra_short_trip` — flags trips with duration_sec < 60
  - `udf_gps_distance_km` — Haversine formula to cross-check distance_km vs GPS delta
- **Anomaly Flags:** Per rental record:
  - `flag_ultra_short_trip` — duration < 60 seconds
  - `flag_gps_mismatch` — GPS-derived distance vs reported distance > threshold
  - `flag_zero_battery_ebike` — e-bike with battery_level = 0
  - `flag_unrealistic_speed` — speed > 50 km/h
  - `flag_device_reuse` — same device_info across multiple user_ids in 24h window
  - `flag_outside_geofence` — start GPS > 500m from any known station
  - `anomaly_score` — count of active flags (0–6)
  - `is_high_risk` — TRUE when anomaly_score ≥ 2
- **Validation Flags:** `is_valid_email`, `is_valid_phone`, `is_valid_gps`, `is_valid_battery`, `is_valid_record`, `validation_errors`
- **SCD Type-2 Keys:** Surrogate key (`sk_*`) + `valid_from`, `valid_to`, `is_current` for stations, bikes, users
- **Loading:** MERGE (upsert for facts, SCD2 for dimensions) triggered by Tasks consuming Streams

### 5.3 CURATED Layer — Normalized Business Model

**Goal:** Deliver a clean star schema with dimension + fact tables, derived KPI fields, and convenience views.

| Table | PK | Source | What It Stores |
|-------|----|--------|----------------|
| `dim_stations` | `sk_station` (surrogate) | val_stations | Station identity, geo, capacity, SCD2 history |
| `dim_bikes` | `sk_bike` (surrogate) | val_bikes | Bike attributes, battery, odometer, SCD2 history |
| `dim_users` | `sk_user` (surrogate) | val_users | Rider PII, KYC, plan type, SCD2 history |
| `fact_rentals` | `rental_id` | val_rentals | All rental events + FK surrogate keys + derived metrics |

**Derived fields on fact_rentals:**
- `duration_min` — `duration_sec / 60.0`
- `speed_kmh` — `distance_km / (duration_sec / 3600.0)`
- `revenue_bucket` — LOW / MEDIUM / HIGH / PREMIUM based on price
- `day_of_week`, `hour_of_day`, `week_of_year` — time attributes from start_time

**Convenience Views:**
- `v_rentals_full` — fact_rentals + all dimension attributes joined
- `v_anomaly_rentals` — only high-risk rentals with all anomaly flags

---

## 6. Automation: Streams + Tasks (DAG)

```
task_raw_to_val_stations ──┐
task_raw_to_val_bikes ─────┤──▶ task_curate_dimensions ──▶ task_curate_rentals
task_raw_to_val_users ─────┘         (SCD2 upserts)           (fact MERGE)
task_raw_to_val_rentals ───────────────────────────────▶ task_curate_rentals
       (root, scheduled 5 min)
```

- **Root Tasks** fire every 5 minutes, only when `SYSTEM$STREAM_HAS_DATA(...)` returns TRUE
- **Child Tasks** use `AFTER` to chain in dependency order (dimensions before facts)
- **Every MERGE is idempotent** — re-running produces no duplicates

---

## 7. Security Architecture

### 7.1 Three Roles

| Role | Row Access | Column Access | Use Case |
|------|-----------|---------------|----------|
| `MANAGER_ROLE` | All city_zones / regions | Full PII visible (email, phone, GPS) | Ops manager, data engineer |
| `ANALYST_ROLE` | All city_zones | email → `a****@domain.com`, phone masked, GPS truncated | Business analyst |
| `OPS_ROLE` | Only their assigned region/city_zone | All PII masked | Field operations team |

### 7.2 Implementation

**Dynamic Data Masking:**
- `email` — ANALYST: `a****@domain.com`; OPS: `****`
- `phone` — ANALYST/OPS: `****XXXX`
- `start_gps` / `end_gps` — ANALYST: truncated to 2 decimal places; OPS: `0.000000,0.000000`
- `lat` / `lon` in dim_stations — OPS: `0.000000`

**Row Access Policy:**
- Applied to `fact_rentals` and `dim_stations` on `city_zone` / `region` columns
- `MANAGER_ROLE` → ALL rows; `ANALYST_ROLE` → ALL rows; `OPS_ROLE` → only their assigned zone

**Audit Trail:**
- `audit_log` captures: operation, target table, row count, status, user, role, timestamp
- Metadata columns (`_created_at`, `_updated_at`, `_source_file`) on every curated table

---

## 8. Snowflake Features Used

| Feature | Where Used | Why |
|---------|-----------|-----|
| Internal Stages | RAW layer | Landing area for files without external storage |
| File Formats | RAW layer | Reusable CSV/NDJSON parsing definitions |
| COPY INTO + SELECT | RAW (rentals NDJSON) | Flatten nested JSON at load time |
| VARIANT | Optional raw_rentals fallback | Schema-on-read for semi-structured JSON |
| JavaScript UDFs | VALIDATED layer | GPS validation, Haversine distance, anomaly rules |
| Streams | RAW → VALIDATED | Change data capture — detect new rows automatically |
| Tasks | All layers | Scheduled automation with DAG dependency chains |
| MERGE | All layers | Idempotent upsert — no duplicates on re-run |
| QUALIFY | VALIDATED/CURATED | Inline deduplication without subqueries |
| SCD Type-2 | CURATED dimensions | Track historical changes to stations, bikes, users |
| Dynamic Data Masking | SECURITY | Column-level PII protection (email, phone, GPS) |
| Row Access Policies | SECURITY | Row-level city_zone/region filtering |
| TRY_TO_* functions | VALIDATED | Safe type casting — returns NULL instead of error |
| METADATA$ columns | RAW | Automatic file/row tracking without manual input |
| Views | ANALYTICS | Logical KPI layer — no data duplication |

---

## 9. KPI Summary

| KPI | Definition | View |
|-----|-----------|------|
| KPI 1: Anomalous Rental Probability Score | (High-risk rentals / Total rentals) × 100 | `v_kpi1_anomaly_score` |
| KPI 2: Station Availability Score | % of time station has ≥1 bike AND ≥1 free dock | `v_kpi2_station_availability` |
| KPI 3: Active Rider Engagement Ratio | % of registered riders with ≥1 rental in last 30 days | `v_kpi3_rider_engagement` |
| KPI 4: Fleet Maintenance Health Index | % of bikes within all health thresholds | `v_kpi4_fleet_health` |
| KPI 5: Avg Rental Revenue (ARR) by Channel | Avg price per completed rental per channel | `v_kpi5_arr_by_channel` |

---

## 10. Project Structure

```
CityRide-HCLHackathon/
├── docs/
│   ├── 01_solution_architecture.md      ← This document
│   ├── 02_database_design.md            ← Full DDL (all tables, views, UDFs)
│   ├── 03_data_ingestion_pipeline.md    ← COPY INTO, MERGE, Streams, Tasks
│   └── 04_rbac_data_masking.md          ← Security policies and testing
├── sql/
│   ├── create_raw_tables.sql            ← RAW layer DDL + COPY INTO
│   ├── create_validated_tables.sql      ← VALIDATED layer DDL + UDFs + MERGE
│   ├── create_curated_tables.sql        ← CURATED layer DDL + SCD2 + MERGE
│   ├── create_analytics_views.sql       ← KPI1–KPI5 views
│   ├── create_pipeline.sql              ← Snowpipe + Streams + Tasks
│   ├── create_security.sql              ← RBAC + masking + row access
│   ├── data_quality_check.sql           ← DQ analysis queries (stations, bikes, users)
│   ├── data_quality_check_rentals.sql   ← DQ analysis queries (rentals)
│   └── verify_all_tables.sql            ← Post-load verification queries
└── README.md
```

---

## 11. What Makes This Solution Stand Out

1. **100% Snowflake-native** — no external tools, no Python scripts, no orchestrators
2. **Idempotent pipeline** — any step can be re-run without creating duplicates
3. **SCD Type-2 dimensions** — full historical tracking of station/bike/user attribute changes
4. **Multi-rule anomaly engine** — 6 independent anomaly flags + composite risk score per rental
5. **Incremental processing** — Streams + Tasks automatically handle new files
6. **Proper star schema** — 3 dimension tables + 1 fact table, not flat denormalized tables
7. **Full data lineage** — every row tracks its source file, load time, and validation status
8. **Enterprise security demo** — same query, 3 roles, 3 different results (masking + row filtering)

---

*End of Document*
