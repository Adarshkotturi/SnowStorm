USE DATABASE CITYRIDE_DB;
USE SCHEMA CURATED;

-- ============================================
-- CURATED LAYER: STAR SCHEMA
-- ============================================

/* ============================================================
   1. DIM_STATIONS (SCD Type-2)
   ============================================================ */

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

-- SCD2 Step 1: Expire records where key attributes changed
UPDATE dim_stations tgt
SET
    valid_to    = CURRENT_TIMESTAMP(),
    is_current  = FALSE,
    _updated_at = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
  AND EXISTS (
      SELECT 1 FROM VALIDATED.val_stations src
      WHERE src.station_id    = tgt.station_id
        AND src.is_valid_record = TRUE
        AND (src.station_name  != tgt.station_name
          OR src.capacity      != tgt.capacity
          OR src.status        != tgt.status
          OR src.city_zone     != tgt.city_zone
          OR src.latitude      != tgt.latitude
          OR src.longitude     != tgt.longitude)
  );

-- SCD2 Step 2: Insert new current version
INSERT INTO dim_stations (
    station_id, station_name, latitude, longitude, capacity,
    neighborhood, city_zone, install_date, status,
    valid_from, valid_to, is_current, _source_file
)
SELECT
    src.station_id, src.station_name, src.latitude, src.longitude, src.capacity,
    src.neighborhood, src.city_zone, src.install_date, src.status,
    CURRENT_TIMESTAMP(), NULL, TRUE, src._source_file
FROM VALIDATED.val_stations src
WHERE src.is_valid_record = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM dim_stations tgt
      WHERE tgt.station_id = src.station_id AND tgt.is_current = TRUE
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.station_id ORDER BY src._validated_at DESC) = 1;


/* ============================================================
   2. DIM_BIKES (SCD Type-2)
   ============================================================ */

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

-- SCD2: Expire changed records
UPDATE dim_bikes tgt
SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE, _updated_at = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
  AND EXISTS (
      SELECT 1 FROM VALIDATED.val_bikes src
      WHERE src.bike_id = tgt.bike_id
        AND src.is_valid_record = TRUE
        AND (src.battery_level     != tgt.battery_level
          OR src.odometer_km       != tgt.odometer_km
          OR src.last_service_date != tgt.last_service_date
          OR src.firmware_version  != tgt.firmware_version
          OR src.status            != tgt.status)
  );

-- SCD2: Insert new current version
INSERT INTO dim_bikes (
    bike_id, bike_type, status, purchase_date, last_service_date,
    odometer_km, battery_level, firmware_version, is_ebike,
    valid_from, valid_to, is_current, _source_file
)
SELECT
    src.bike_id, src.bike_type, src.status, src.purchase_date, src.last_service_date,
    src.odometer_km, src.battery_level, src.firmware_version, src.is_ebike,
    CURRENT_TIMESTAMP(), NULL, TRUE, src._source_file
FROM VALIDATED.val_bikes src
WHERE src.is_valid_record = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM dim_bikes tgt
      WHERE tgt.bike_id = src.bike_id AND tgt.is_current = TRUE
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.bike_id ORDER BY src._validated_at DESC) = 1;


/* ============================================================
   3. DIM_USERS (SCD Type-2)
   ============================================================ */

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

-- SCD2: Expire changed records
UPDATE dim_users tgt
SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE, _updated_at = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
  AND EXISTS (
      SELECT 1 FROM VALIDATED.val_users src
      WHERE src.user_id = tgt.user_id
        AND src.is_valid_record = TRUE
        AND (src.email      != tgt.email
          OR src.phone      != tgt.phone
          OR src.kyc_status != tgt.kyc_status
          OR src.region     != tgt.region
          OR src.address    != tgt.address)
  );

-- SCD2: Insert new current version
INSERT INTO dim_users (
    user_id, customer_name, dob, gender, email, phone,
    address, city, state, region, kyc_status, registration_date,
    is_student, corporate_id,
    valid_from, valid_to, is_current, _source_file
)
SELECT
    src.user_id, src.customer_name, src.dob, src.gender, src.email, src.phone,
    src.address, src.city, src.state, src.region, src.kyc_status, src.registration_date,
    src.is_student, src.corporate_id,
    CURRENT_TIMESTAMP(), NULL, TRUE, src._source_file
FROM VALIDATED.val_users src
WHERE src.is_valid_record = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM dim_users tgt
      WHERE tgt.user_id = src.user_id AND tgt.is_current = TRUE
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.user_id ORDER BY src._validated_at DESC) = 1;


/* ============================================================
   4. FACT_RENTALS (MERGE with derived fields + SK lookups)
   ============================================================ */

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

MERGE INTO fact_rentals AS tgt
USING (
    SELECT
        vr.rental_id,
        vr.user_id,
        vr.bike_id,
        vr.start_station_id,
        vr.end_station_id,

        du.sk_user,
        db.sk_bike                                          AS sk_bike_start,
        ss.sk_station                                       AS sk_start_station,
        es.sk_station                                       AS sk_end_station,

        vr.start_time,
        vr.end_time,
        vr.duration_sec,
        ROUND(vr.duration_sec / 60.0, 2)                   AS duration_min,
        vr.distance_km,
        vr.price,
        vr.plan_type,
        vr.channel,
        vr.device_info,
        vr.start_gps,
        vr.end_gps,
        vr.is_flagged,

        DAYNAME(vr.start_time)                             AS day_of_week,
        HOUR(vr.start_time)                                AS hour_of_day,
        WEEKOFYEAR(vr.start_time)                          AS week_of_year,

        CASE
            WHEN vr.duration_sec > 0
            THEN ROUND(vr.distance_km / (vr.duration_sec / 3600.0), 2)
            ELSE NULL
        END                                                AS speed_kmh,

        CASE
            WHEN vr.price < 50   THEN 'LOW'
            WHEN vr.price < 150  THEN 'MEDIUM'
            WHEN vr.price < 500  THEN 'HIGH'
            ELSE 'PREMIUM'
        END                                                AS revenue_bucket,

        vr.flag_ultra_short_trip,
        vr.flag_gps_mismatch,
        vr.flag_zero_battery_ebike,
        vr.flag_unrealistic_speed,
        vr.flag_device_reuse,
        vr.flag_outside_geofence,
        vr.anomaly_score,
        vr.is_high_risk,
        vr._source_file

    FROM VALIDATED.val_rentals vr
    LEFT JOIN dim_users    du ON vr.user_id          = du.user_id    AND du.is_current = TRUE
    LEFT JOIN dim_bikes    db ON vr.bike_id           = db.bike_id    AND db.is_current = TRUE
    LEFT JOIN dim_stations ss ON vr.start_station_id  = ss.station_id AND ss.is_current = TRUE
    LEFT JOIN dim_stations es ON vr.end_station_id    = es.station_id AND es.is_current = TRUE
    WHERE vr.is_valid_record = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY vr.rental_id ORDER BY vr._validated_at DESC) = 1
) AS src
ON tgt.rental_id = src.rental_id
WHEN MATCHED THEN UPDATE SET
    tgt.user_id                 = src.user_id,
    tgt.bike_id                 = src.bike_id,
    tgt.start_station_id        = src.start_station_id,
    tgt.end_station_id          = src.end_station_id,
    tgt.sk_user                 = src.sk_user,
    tgt.sk_bike_start           = src.sk_bike_start,
    tgt.sk_start_station        = src.sk_start_station,
    tgt.sk_end_station          = src.sk_end_station,
    tgt.start_time              = src.start_time,
    tgt.end_time                = src.end_time,
    tgt.duration_sec            = src.duration_sec,
    tgt.duration_min            = src.duration_min,
    tgt.distance_km             = src.distance_km,
    tgt.price                   = src.price,
    tgt.plan_type               = src.plan_type,
    tgt.channel                 = src.channel,
    tgt.device_info             = src.device_info,
    tgt.start_gps               = src.start_gps,
    tgt.end_gps                 = src.end_gps,
    tgt.is_flagged              = src.is_flagged,
    tgt.day_of_week             = src.day_of_week,
    tgt.hour_of_day             = src.hour_of_day,
    tgt.week_of_year            = src.week_of_year,
    tgt.speed_kmh               = src.speed_kmh,
    tgt.revenue_bucket          = src.revenue_bucket,
    tgt.flag_ultra_short_trip   = src.flag_ultra_short_trip,
    tgt.flag_gps_mismatch       = src.flag_gps_mismatch,
    tgt.flag_zero_battery_ebike = src.flag_zero_battery_ebike,
    tgt.flag_unrealistic_speed  = src.flag_unrealistic_speed,
    tgt.flag_device_reuse       = src.flag_device_reuse,
    tgt.flag_outside_geofence   = src.flag_outside_geofence,
    tgt.anomaly_score           = src.anomaly_score,
    tgt.is_high_risk            = src.is_high_risk,
    tgt._updated_at             = CURRENT_TIMESTAMP(),
    tgt._source_file            = src._source_file
WHEN NOT MATCHED THEN INSERT (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    sk_user, sk_bike_start, sk_start_station, sk_end_station,
    start_time, end_time, duration_sec, duration_min,
    distance_km, price, plan_type, channel, device_info,
    start_gps, end_gps, is_flagged,
    day_of_week, hour_of_day, week_of_year,
    speed_kmh, revenue_bucket,
    flag_ultra_short_trip, flag_gps_mismatch, flag_zero_battery_ebike,
    flag_unrealistic_speed, flag_device_reuse, flag_outside_geofence,
    anomaly_score, is_high_risk,
    _created_at, _updated_at, _source_file
) VALUES (
    src.rental_id, src.user_id, src.bike_id, src.start_station_id, src.end_station_id,
    src.sk_user, src.sk_bike_start, src.sk_start_station, src.sk_end_station,
    src.start_time, src.end_time, src.duration_sec, src.duration_min,
    src.distance_km, src.price, src.plan_type, src.channel, src.device_info,
    src.start_gps, src.end_gps, src.is_flagged,
    src.day_of_week, src.hour_of_day, src.week_of_year,
    src.speed_kmh, src.revenue_bucket,
    src.flag_ultra_short_trip, src.flag_gps_mismatch, src.flag_zero_battery_ebike,
    src.flag_unrealistic_speed, src.flag_device_reuse, src.flag_outside_geofence,
    src.anomaly_score, src.is_high_risk,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), src._source_file
);


/* ============================================================
   5. CONVENIENCE VIEWS
   ============================================================ */

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
    du.city                                         AS user_city,
    du.kyc_status,
    du.is_student,
    du.corporate_id,
    db.bike_type,
    db.battery_level,
    db.is_ebike,
    db.odometer_km,
    db.firmware_version,
    ss.station_name                                 AS start_station_name,
    ss.city_zone                                    AS start_city_zone,
    ss.neighborhood                                 AS start_neighborhood,
    ss.latitude                                     AS start_lat,
    ss.longitude                                    AS start_lon,
    es.station_name                                 AS end_station_name,
    es.city_zone                                    AS end_city_zone,
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
WHERE is_high_risk = TRUE
ORDER BY anomaly_score DESC;

-- ============================================
-- VERIFY CURATED ROW COUNTS
-- ============================================

SELECT 'dim_stations'  AS table_name, COUNT(*) AS total_rows,
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
FROM dim_stations
UNION ALL
SELECT 'dim_bikes', COUNT(*),
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
FROM dim_bikes
UNION ALL
SELECT 'dim_users', COUNT(*),
       SUM(CASE WHEN is_current THEN 1 ELSE 0 END)
FROM dim_users
UNION ALL
SELECT 'fact_rentals', COUNT(*), COUNT(*) FROM fact_rentals;

-- FK integrity checks
SELECT 'orphan_rentals_no_user' AS check_name, COUNT(*) AS violations
FROM fact_rentals fr
WHERE fr.user_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dim_users du WHERE du.user_id = fr.user_id AND du.is_current = TRUE)
UNION ALL
SELECT 'orphan_rentals_no_bike', COUNT(*)
FROM fact_rentals fr
WHERE fr.bike_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dim_bikes db WHERE db.bike_id = fr.bike_id AND db.is_current = TRUE)
UNION ALL
SELECT 'orphan_rentals_no_start_station', COUNT(*)
FROM fact_rentals fr
WHERE fr.start_station_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM dim_stations ds WHERE ds.station_id = fr.start_station_id AND ds.is_current = TRUE);

-- Sample data
SELECT * FROM dim_stations LIMIT 3;
SELECT * FROM dim_bikes    LIMIT 3;
SELECT * FROM dim_users    LIMIT 3;
SELECT * FROM fact_rentals LIMIT 3;
