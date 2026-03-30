USE DATABASE CITYRIDE_DB;
USE SCHEMA RAW;

-- ============================================
-- DATABASE & SCHEMAS
-- ============================================

CREATE DATABASE IF NOT EXISTS CITYRIDE_DB;
USE DATABASE CITYRIDE_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS VALIDATED;
CREATE SCHEMA IF NOT EXISTS CURATED;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS SECURITY;
CREATE SCHEMA IF NOT EXISTS AUDIT;

-- ============================================
-- FILE FORMATS
-- ============================================

USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE                           = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY   = '"'
  SKIP_HEADER                    = 1
  NULL_IF                        = ('', 'NULL', 'null', 'N/A', 'NA')
  TRIM_SPACE                     = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  DATE_FORMAT                    = 'AUTO'
  TIMESTAMP_FORMAT               = 'AUTO';

CREATE OR REPLACE FILE FORMAT ndjson_format
  TYPE               = 'JSON'
  STRIP_OUTER_ARRAY  = FALSE
  STRIP_NULL_VALUES  = FALSE
  IGNORE_UTF8_ERRORS = TRUE;

-- ============================================
-- INTERNAL STAGES
-- ============================================

CREATE OR REPLACE STAGE raw_stations_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for station CSV files (master + incremental)';

CREATE OR REPLACE STAGE raw_bikes_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for bike CSV files (master + incremental)';

CREATE OR REPLACE STAGE raw_rentals_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for rental CSV/NDJSON files (master + incremental)';

CREATE OR REPLACE STAGE raw_users_stage
  FILE_FORMAT = csv_format
  COMMENT = 'Stage for user CSV files (master + incremental)';

-- ============================================
-- RAW TABLES
-- ============================================

/* ============================================================
   1. RAW_STATIONS
   ============================================================ */

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

/* ============================================================
   2. RAW_BIKES
   ============================================================ */

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

/* ============================================================
   3. RAW_RENTALS
   ============================================================ */

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

/* ============================================================
   4. RAW_USERS
   ============================================================ */

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

-- ============================================
-- COPY INTO: LOAD RAW DATA
-- ============================================

-- Upload files first (run in SnowSQL CLI):
-- PUT file:///path/to/stations_master.csv @raw_stations_stage/master/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/stations_inc.csv    @raw_stations_stage/incremental/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/bikes_master.csv    @raw_bikes_stage/master/    AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/bikes_inc.csv       @raw_bikes_stage/incremental/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/rentals_master.csv  @raw_rentals_stage/master/  AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/rentals_inc.csv     @raw_rentals_stage/incremental/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/users_master.csv    @raw_users_stage/master/    AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- PUT file:///path/to/users_inc.csv       @raw_users_stage/incremental/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

COPY INTO raw_stations (
    station_id, station_name, latitude, longitude, capacity,
    neighborhood, city_zone, install_date, status
)
FROM @raw_stations_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

COPY INTO raw_bikes (
    bike_id, bike_type, status, purchase_date, last_service_date,
    odometer_km, battery_level, firmware_version
)
FROM @raw_bikes_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- CSV rentals
COPY INTO raw_rentals (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    start_time, end_time, duration_sec, distance_km, price,
    plan_type, channel, device_info, start_gps, end_gps, is_flagged
)
FROM @raw_rentals_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- NDJSON rentals (if present)
COPY INTO raw_rentals (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    start_time, end_time, duration_sec, distance_km, price,
    plan_type, channel, device_info, start_gps, end_gps, is_flagged
)
FROM (
    SELECT
        $1:rental_id::VARCHAR,
        $1:user_id::VARCHAR,
        $1:bike_id::VARCHAR,
        $1:start_station_id::VARCHAR,
        $1:end_station_id::VARCHAR,
        $1:start_time::VARCHAR,
        $1:end_time::VARCHAR,
        $1:duration_sec::VARCHAR,
        $1:distance_km::VARCHAR,
        $1:price::VARCHAR,
        $1:plan_type::VARCHAR,
        $1:channel::VARCHAR,
        $1:device_info::VARCHAR,
        $1:start_gps::VARCHAR,
        $1:end_gps::VARCHAR,
        $1:is_flagged::VARCHAR
    FROM @raw_rentals_stage (PATTERN => '.*\.ndjson.*|.*\.json.*')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.ndjson_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

COPY INTO raw_users (
    user_id, customer_name, dob, gender, email, phone,
    address, city, state, region, kyc_status,
    registration_date, is_student, corporate_id
)
FROM @raw_users_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- ============================================
-- VERIFY RAW COUNTS
-- ============================================

SELECT 'raw_stations' AS table_name, COUNT(*) AS row_count FROM raw_stations
UNION ALL SELECT 'raw_bikes',   COUNT(*) FROM raw_bikes
UNION ALL SELECT 'raw_rentals', COUNT(*) FROM raw_rentals
UNION ALL SELECT 'raw_users',   COUNT(*) FROM raw_users;

-- ============================================
-- AUDIT LOG
-- ============================================

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

INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_stations', COUNT(*), 'SUCCESS' FROM RAW.raw_stations;
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_bikes', COUNT(*), 'SUCCESS' FROM RAW.raw_bikes;
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_rentals', COUNT(*), 'SUCCESS' FROM RAW.raw_rentals;
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_users', COUNT(*), 'SUCCESS' FROM RAW.raw_users;
