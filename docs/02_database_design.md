# CityRide — Database Design (DDL)

> **Project:** Bike Rental Data Insights Hackathon — HCL Tech  
> **Version:** 1.0  
> **Date:** 2026-03-30

---

## 1. Database & Schemas

```sql
CREATE DATABASE IF NOT EXISTS CITYRIDE_DB;
USE DATABASE CITYRIDE_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS VALIDATED;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS SECURITY;
CREATE SCHEMA IF NOT EXISTS AUDIT;
```

---

## 2. File Formats

```sql
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE                         = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER                  = 1
  NULL_IF                      = ('', 'NULL', 'null', 'N/A', 'NA')
  TRIM_SPACE                   = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  DATE_FORMAT                  = 'AUTO'
  TIMESTAMP_FORMAT             = 'AUTO';

CREATE OR REPLACE FILE FORMAT ndjson_format
  TYPE              = 'JSON'
  STRIP_OUTER_ARRAY = FALSE
  STRIP_NULL_VALUES = FALSE
  IGNORE_UTF8_ERRORS = TRUE;
```

---

## 3. Internal Stages

```sql
USE SCHEMA RAW;

CREATE OR REPLACE STAGE raw_stations_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for station CSV files';

CREATE OR REPLACE STAGE raw_bikes_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for bike CSV files';

CREATE OR REPLACE STAGE raw_rentals_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for rental CSV/NDJSON files';

CREATE OR REPLACE STAGE raw_users_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for user CSV files';
```

---

## 4. RAW Layer Tables

### 4.1 raw_stations

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_stations (
    station_id          VARCHAR(50),
    station_name        VARCHAR(200),
    latitude            VARCHAR(30),
    longitude           VARCHAR(30),
    capacity            VARCHAR(20),
    neighborhood        VARCHAR(200),
    city_zone           VARCHAR(100),
    install_date        VARCHAR(30),
    status              VARCHAR(30),

    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _row_number         NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
);
```

### 4.2 raw_bikes

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_bikes (
    bike_id             VARCHAR(50),
    bike_type           VARCHAR(30),
    status              VARCHAR(30),
    purchase_date       VARCHAR(30),
    last_service_date   VARCHAR(30),
    odometer_km         VARCHAR(30),
    battery_level       VARCHAR(20),
    firmware_version    VARCHAR(50),

    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _row_number         NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
);
```

### 4.3 raw_rentals

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_rentals (
    rental_id           VARCHAR(50),
    user_id             VARCHAR(50),
    bike_id             VARCHAR(50),
    start_station_id    VARCHAR(50),
    end_station_id      VARCHAR(50),
    start_time          VARCHAR(50),
    end_time            VARCHAR(50),
    duration_sec        VARCHAR(20),
    distance_km         VARCHAR(20),
    price               VARCHAR(20),
    plan_type           VARCHAR(50),
    channel             VARCHAR(30),
    device_info         VARCHAR(200),
    start_gps           VARCHAR(100),
    end_gps             VARCHAR(100),
    is_flagged          VARCHAR(10),

    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _row_number         NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
);
```

### 4.4 raw_users

```sql
USE SCHEMA RAW;

CREATE OR REPLACE TABLE raw_users (
    user_id             VARCHAR(50),
    customer_name       VARCHAR(200),
    dob                 VARCHAR(30),
    gender              VARCHAR(20),
    email               VARCHAR(255),
    phone               VARCHAR(30),
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(100),
    region              VARCHAR(100),
    kyc_status          VARCHAR(30),
    registration_date   VARCHAR(30),
    is_student          VARCHAR(10),
    corporate_id        VARCHAR(50),

    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500)  DEFAULT METADATA$FILENAME,
    _row_number         NUMBER        DEFAULT METADATA$FILE_ROW_NUMBER
);
```

---

## 5. VALIDATED Layer Tables

### 5.1 val_stations

```sql
USE SCHEMA VALIDATED;

CREATE OR REPLACE TABLE val_stations (
    station_id          VARCHAR(50)     NOT NULL,
    station_name        VARCHAR(200),
    latitude            NUMBER(10,6),
    longitude           NUMBER(10,6),
    capacity            NUMBER(5),
    neighborhood        VARCHAR(200),
    city_zone           VARCHAR(100),
    install_date        DATE,
    status              VARCHAR(30),

    is_valid_gps        BOOLEAN,
    is_valid_capacity   BOOLEAN,
    is_valid_record     BOOLEAN,
    validation_errors   VARCHAR(2000),

    _loaded_at          TIMESTAMP_NTZ,
    _validated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         NUMBER,

    CONSTRAINT pk_val_stations PRIMARY KEY (station_id)
);
```

### 5.2 val_bikes

```sql
USE SCHEMA VALIDATED;

CREATE OR REPLACE TABLE val_bikes (
    bike_id             VARCHAR(50)     NOT NULL,
    bike_type           VARCHAR(30),
    status              VARCHAR(30),
    purchase_date       DATE,
    last_service_date   DATE,
    odometer_km         NUMBER(10,2),
    battery_level       NUMBER(5,2),
    firmware_version    VARCHAR(50),

    is_valid_battery    BOOLEAN,
    is_valid_odometer   BOOLEAN,
    is_ebike            BOOLEAN,
    is_valid_record     BOOLEAN,
    validation_errors   VARCHAR(2000),

    _loaded_at          TIMESTAMP_NTZ,
    _validated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         NUMBER,

    CONSTRAINT pk_val_bikes PRIMARY KEY (bike_id)
);
```

### 5.3 val_rentals

```sql
USE SCHEMA VALIDATED;

CREATE OR REPLACE TABLE val_rentals (
    rental_id           VARCHAR(50)     NOT NULL,
    user_id             VARCHAR(50),
    bike_id             VARCHAR(50),
    start_station_id    VARCHAR(50),
    end_station_id      VARCHAR(50),
    start_time          TIMESTAMP_NTZ,
    end_time            TIMESTAMP_NTZ,
    duration_sec        NUMBER(10),
    distance_km         NUMBER(10,3),
    price               NUMBER(10,2),
    plan_type           VARCHAR(50),
    channel             VARCHAR(30),
    device_info         VARCHAR(200),
    start_gps           VARCHAR(100),
    end_gps             VARCHAR(100),
    is_flagged          BOOLEAN,

    -- Anomaly flags
    flag_ultra_short_trip   BOOLEAN,
    flag_gps_mismatch       BOOLEAN,
    flag_zero_battery_ebike BOOLEAN,
    flag_unrealistic_speed  BOOLEAN,
    flag_device_reuse       BOOLEAN,
    flag_outside_geofence   BOOLEAN,
    anomaly_score           NUMBER(2),
    is_high_risk            BOOLEAN,

    -- Validation flags
    is_valid_times          BOOLEAN,
    is_valid_distance       BOOLEAN,
    is_valid_price          BOOLEAN,
    has_matching_bike       BOOLEAN,
    has_matching_station    BOOLEAN,
    is_valid_record         BOOLEAN,
    validation_errors       VARCHAR(2000),

    _loaded_at              TIMESTAMP_NTZ,
    _validated_at           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),
    _row_number             NUMBER,

    CONSTRAINT pk_val_rentals PRIMARY KEY (rental_id)
);
```

### 5.4 val_users

```sql
USE SCHEMA VALIDATED;

CREATE OR REPLACE TABLE val_users (
    user_id             VARCHAR(50)     NOT NULL,
    customer_name       VARCHAR(200),
    dob                 DATE,
    gender              VARCHAR(20),
    email               VARCHAR(255),
    phone               VARCHAR(30),
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(100),
    region              VARCHAR(100),
    kyc_status          VARCHAR(30),
    registration_date   DATE,
    is_student          BOOLEAN,
    corporate_id        VARCHAR(50),

    is_valid_email      BOOLEAN,
    is_valid_phone      BOOLEAN,
    is_valid_dob        BOOLEAN,
    is_valid_record     BOOLEAN,
    validation_errors   VARCHAR(2000),

    _loaded_at          TIMESTAMP_NTZ,
    _validated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),
    _row_number         NUMBER,

    CONSTRAINT pk_val_users PRIMARY KEY (user_id)
);
```

---

## 6. CURATED Layer Tables (Star Schema — SCD Type-2 Dimensions + Fact)

### 6.1 dim_stations (SCD Type-2)

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE dim_stations (
    sk_station          NUMBER AUTOINCREMENT  NOT NULL,
    station_id          VARCHAR(50)           NOT NULL,
    station_name        VARCHAR(200),
    latitude            NUMBER(10,6),
    longitude           NUMBER(10,6),
    capacity            NUMBER(5),
    neighborhood        VARCHAR(200),
    city_zone           VARCHAR(100),
    install_date        DATE,
    status              VARCHAR(30),

    valid_from          TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    valid_to            TIMESTAMP_NTZ,
    is_current          BOOLEAN               DEFAULT TRUE,

    _created_at         TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    _updated_at         TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),

    CONSTRAINT pk_dim_stations PRIMARY KEY (sk_station)
);
```

### 6.2 dim_bikes (SCD Type-2)

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE dim_bikes (
    sk_bike             NUMBER AUTOINCREMENT  NOT NULL,
    bike_id             VARCHAR(50)           NOT NULL,
    bike_type           VARCHAR(30),
    status              VARCHAR(30),
    purchase_date       DATE,
    last_service_date   DATE,
    odometer_km         NUMBER(10,2),
    battery_level       NUMBER(5,2),
    firmware_version    VARCHAR(50),
    is_ebike            BOOLEAN,

    valid_from          TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    valid_to            TIMESTAMP_NTZ,
    is_current          BOOLEAN               DEFAULT TRUE,

    _created_at         TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    _updated_at         TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),

    CONSTRAINT pk_dim_bikes PRIMARY KEY (sk_bike)
);
```

### 6.3 dim_users (SCD Type-2)

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE dim_users (
    sk_user             NUMBER AUTOINCREMENT  NOT NULL,
    user_id             VARCHAR(50)           NOT NULL,
    customer_name       VARCHAR(200),
    dob                 DATE,
    gender              VARCHAR(20),
    email               VARCHAR(255),
    phone               VARCHAR(30),
    address             VARCHAR(500),
    city                VARCHAR(100),
    state               VARCHAR(100),
    region              VARCHAR(100),
    kyc_status          VARCHAR(30),
    registration_date   DATE,
    is_student          BOOLEAN,
    corporate_id        VARCHAR(50),

    valid_from          TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    valid_to            TIMESTAMP_NTZ,
    is_current          BOOLEAN               DEFAULT TRUE,

    _created_at         TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    _updated_at         TIMESTAMP_NTZ         DEFAULT CURRENT_TIMESTAMP(),
    _source_file        VARCHAR(500),

    CONSTRAINT pk_dim_users PRIMARY KEY (sk_user)
);
```

### 6.4 fact_rentals

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE fact_rentals (
    rental_id               VARCHAR(50)     NOT NULL,
    user_id                 VARCHAR(50),
    bike_id                 VARCHAR(50),
    start_station_id        VARCHAR(50),
    end_station_id          VARCHAR(50),
    sk_user                 NUMBER,
    sk_bike_start           NUMBER,
    sk_start_station        NUMBER,
    sk_end_station          NUMBER,

    start_time              TIMESTAMP_NTZ,
    end_time                TIMESTAMP_NTZ,
    duration_sec            NUMBER(10),
    duration_min            NUMBER(10,2),
    distance_km             NUMBER(10,3),
    price                   NUMBER(10,2),
    plan_type               VARCHAR(50),
    channel                 VARCHAR(30),
    device_info             VARCHAR(200),
    start_gps               VARCHAR(100),
    end_gps                 VARCHAR(100),
    is_flagged              BOOLEAN,

    -- Derived time attributes
    day_of_week             VARCHAR(10),
    hour_of_day             NUMBER(2),
    week_of_year            NUMBER(2),

    -- Derived metrics
    speed_kmh               NUMBER(10,2),
    revenue_bucket          VARCHAR(20),

    -- Anomaly fields
    flag_ultra_short_trip   BOOLEAN,
    flag_gps_mismatch       BOOLEAN,
    flag_zero_battery_ebike BOOLEAN,
    flag_unrealistic_speed  BOOLEAN,
    flag_device_reuse       BOOLEAN,
    flag_outside_geofence   BOOLEAN,
    anomaly_score           NUMBER(2),
    is_high_risk            BOOLEAN,

    _created_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _updated_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source_file            VARCHAR(500),

    CONSTRAINT pk_fact_rentals PRIMARY KEY (rental_id)
);
```

### 6.5 Convenience Views

```sql
USE SCHEMA CURATED;

CREATE OR REPLACE VIEW v_rentals_full AS
SELECT
    fr.rental_id,
    fr.user_id,
    fr.bike_id,
    fr.start_station_id,
    fr.end_station_id,
    du.customer_name,
    du.email,
    du.phone,
    du.region,
    du.kyc_status,
    du.is_student,
    du.corporate_id,
    db.bike_type,
    db.battery_level,
    db.is_ebike,
    db.odometer_km,
    ss.station_name                         AS start_station_name,
    ss.city_zone                            AS start_city_zone,
    ss.neighborhood                         AS start_neighborhood,
    es.station_name                         AS end_station_name,
    es.city_zone                            AS end_city_zone,
    fr.start_time,
    fr.end_time,
    fr.duration_sec,
    fr.duration_min,
    fr.distance_km,
    fr.speed_kmh,
    fr.price,
    fr.revenue_bucket,
    fr.plan_type,
    fr.channel,
    fr.device_info,
    fr.start_gps,
    fr.end_gps,
    fr.day_of_week,
    fr.hour_of_day,
    fr.week_of_year,
    fr.flag_ultra_short_trip,
    fr.flag_gps_mismatch,
    fr.flag_zero_battery_ebike,
    fr.flag_unrealistic_speed,
    fr.flag_device_reuse,
    fr.flag_outside_geofence,
    fr.anomaly_score,
    fr.is_high_risk,
    fr.is_flagged
FROM fact_rentals fr
LEFT JOIN dim_users    du ON fr.user_id          = du.user_id    AND du.is_current = TRUE
LEFT JOIN dim_bikes    db ON fr.bike_id           = db.bike_id    AND db.is_current = TRUE
LEFT JOIN dim_stations ss ON fr.start_station_id  = ss.station_id AND ss.is_current = TRUE
LEFT JOIN dim_stations es ON fr.end_station_id    = es.station_id AND es.is_current = TRUE;

CREATE OR REPLACE VIEW v_anomaly_rentals AS
SELECT *
FROM v_rentals_full
WHERE is_high_risk = TRUE;
```

---

## 7. SECURITY Schema

```sql
USE SCHEMA SECURITY;

CREATE OR REPLACE TABLE user_role_mapping (
    snowflake_user  VARCHAR(100)    NOT NULL,
    role_type       VARCHAR(30)     NOT NULL,
    mapped_region   VARCHAR(100),
    mapped_zone     VARCHAR(100),
    _created_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_user_role_mapping PRIMARY KEY (snowflake_user)
);

INSERT INTO user_role_mapping (snowflake_user, role_type, mapped_region, mapped_zone) VALUES
    ('MANAGER_USER1',   'MANAGER',  NULL,    NULL),
    ('ANALYST_USER1',   'ANALYST',  NULL,    NULL),
    ('OPS_USER_NORTH',  'OPS',      'North', 'North-Zone'),
    ('OPS_USER_SOUTH',  'OPS',      'South', 'South-Zone');

CREATE ROLE IF NOT EXISTS MANAGER_ROLE;
CREATE ROLE IF NOT EXISTS ANALYST_ROLE;
CREATE ROLE IF NOT EXISTS OPS_ROLE;

CREATE USER IF NOT EXISTS manager_user1  PASSWORD = 'Manager@123'  DEFAULT_ROLE = MANAGER_ROLE;
CREATE USER IF NOT EXISTS analyst_user1  PASSWORD = 'Analyst@123'  DEFAULT_ROLE = ANALYST_ROLE;
CREATE USER IF NOT EXISTS ops_user_north PASSWORD = 'Ops@North123' DEFAULT_ROLE = OPS_ROLE;

GRANT ROLE MANAGER_ROLE TO USER manager_user1;
GRANT ROLE ANALYST_ROLE TO USER analyst_user1;
GRANT ROLE OPS_ROLE     TO USER ops_user_north;
```

---

## 8. AUDIT Schema

```sql
USE SCHEMA AUDIT;

CREATE OR REPLACE TABLE audit_log (
    log_id          NUMBER AUTOINCREMENT,
    operation       VARCHAR(50),
    target_table    VARCHAR(200),
    rows_affected   NUMBER,
    status          VARCHAR(20),
    error_message   VARCHAR(4000),
    executed_by     VARCHAR(100)  DEFAULT CURRENT_USER(),
    executed_role   VARCHAR(100)  DEFAULT CURRENT_ROLE(),
    executed_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

---

## 9. Schema Diagram

```
┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│   dim_stations    │     │    dim_bikes       │     │    dim_users      │
├───────────────────┤     ├───────────────────┤     ├───────────────────┤
│ sk_station PK     │     │ sk_bike PK        │     │ sk_user PK        │
│ station_id NK     │     │ bike_id NK        │     │ user_id NK        │
│ station_name      │     │ bike_type         │     │ customer_name     │
│ latitude          │     │ battery_level     │     │ email             │
│ longitude         │     │ odometer_km       │     │ phone             │
│ capacity          │     │ is_ebike          │     │ region            │
│ city_zone         │     │ status            │     │ kyc_status        │
│ status            │     │ valid_from        │     │ is_student        │
│ valid_from        │     │ valid_to          │     │ corporate_id      │
│ valid_to          │     │ is_current        │     │ valid_from        │
│ is_current        │     └────────┬──────────┘     │ valid_to          │
└────────┬──────────┘              │                │ is_current        │
         │ 1:N                     │ 1:N            └────────┬──────────┘
         │                         │                         │ 1:N
         └─────────────────────────▼─────────────────────────▼
                            ┌───────────────────────┐
                            │      fact_rentals     │
                            ├───────────────────────┤
                            │ rental_id PK          │
                            │ user_id FK            │
                            │ bike_id FK            │
                            │ start_station_id FK   │
                            │ end_station_id FK     │
                            │ sk_user FK            │
                            │ sk_bike_start FK      │
                            │ sk_start_station FK   │
                            │ sk_end_station FK     │
                            │ start_time            │
                            │ duration_sec/min      │
                            │ distance_km           │
                            │ speed_kmh             │
                            │ price / revenue_bucket│
                            │ plan_type / channel   │
                            │ anomaly_score         │
                            │ is_high_risk          │
                            │ flag_* (6 flags)      │
                            └───────────────────────┘
```

---

*End of Document*
