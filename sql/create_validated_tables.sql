USE DATABASE CITYRIDE_DB;
USE SCHEMA VALIDATED;

-- ============================================
-- JAVASCRIPT UDFs
-- ============================================

CREATE OR REPLACE FUNCTION udf_validate_email(email VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!EMAIL) return false;
    var pattern = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    return pattern.test(EMAIL.trim());
$$;

CREATE OR REPLACE FUNCTION udf_validate_phone(phone VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!PHONE) return false;
    var digits = PHONE.replace(/\D/g, '');
    return digits.length === 10 || (digits.length === 11 && digits[0] === '1');
$$;

CREATE OR REPLACE FUNCTION udf_validate_gps(gps_string VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!GPS_STRING) return false;
    var parts = GPS_STRING.split(',');
    if (parts.length !== 2) return false;
    var lat = parseFloat(parts[0].trim());
    var lon = parseFloat(parts[1].trim());
    if (isNaN(lat) || isNaN(lon)) return false;
    return (lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180);
$$;

CREATE OR REPLACE FUNCTION udf_validate_battery(battery VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!BATTERY) return false;
    var b = parseFloat(BATTERY);
    if (isNaN(b)) return false;
    return (b >= 0 && b <= 100);
$$;

CREATE OR REPLACE FUNCTION udf_is_ultra_short_trip(duration_sec FLOAT)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (DURATION_SEC === null || DURATION_SEC === undefined) return false;
    return DURATION_SEC < 60;
$$;

CREATE OR REPLACE FUNCTION udf_gps_distance_km(gps_start VARCHAR, gps_end VARCHAR)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
    if (!GPS_START || !GPS_END) return null;
    var p1 = GPS_START.split(',');
    var p2 = GPS_END.split(',');
    if (p1.length !== 2 || p2.length !== 2) return null;
    var lat1 = parseFloat(p1[0].trim()), lon1 = parseFloat(p1[1].trim());
    var lat2 = parseFloat(p2[0].trim()), lon2 = parseFloat(p2[1].trim());
    if (isNaN(lat1)||isNaN(lon1)||isNaN(lat2)||isNaN(lon2)) return null;
    var R = 6371;
    var dLat = (lat2 - lat1) * Math.PI / 180;
    var dLon = (lon2 - lon1) * Math.PI / 180;
    var a = Math.sin(dLat/2)*Math.sin(dLat/2)
          + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)
          * Math.sin(dLon/2)*Math.sin(dLon/2);
    var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return Math.round(R * c * 1000) / 1000;
$$;

-- ============================================
-- VALIDATED LAYER TABLES
-- ============================================

/* ============================================================
   1. VAL_STATIONS
   ============================================================ */

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

/* ============================================================
   2. VAL_BIKES
   ============================================================ */

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

/* ============================================================
   3. VAL_USERS
   ============================================================ */

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

/* ============================================================
   4. VAL_RENTALS
   ============================================================ */

CREATE OR REPLACE TABLE val_rentals (
    rental_id               VARCHAR(50)     NOT NULL,
    user_id                 VARCHAR(50),
    bike_id                 VARCHAR(50),
    start_station_id        VARCHAR(50),
    end_station_id          VARCHAR(50),
    start_time              TIMESTAMP_NTZ,
    end_time                TIMESTAMP_NTZ,
    duration_sec            NUMBER(10),
    distance_km             NUMBER(10,3),
    price                   NUMBER(10,2),
    plan_type               VARCHAR(50),
    channel                 VARCHAR(30),
    device_info             VARCHAR(200),
    start_gps               VARCHAR(100),
    end_gps                 VARCHAR(100),
    is_flagged              BOOLEAN,

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

-- ============================================
-- MERGE: RAW → VAL_STATIONS
-- ============================================

MERGE INTO val_stations AS tgt
USING (
    SELECT
        TRIM(station_id)                                    AS station_id,
        TRIM(station_name)                                  AS station_name,
        TRY_TO_NUMBER(TRIM(latitude),  10, 6)              AS latitude,
        TRY_TO_NUMBER(TRIM(longitude), 10, 6)              AS longitude,
        TRY_TO_NUMBER(TRIM(capacity))                       AS capacity,
        TRIM(neighborhood)                                  AS neighborhood,
        TRIM(city_zone)                                     AS city_zone,
        TRY_TO_DATE(TRIM(install_date))                    AS install_date,
        UPPER(TRIM(status))                                 AS status,

        (TRY_TO_NUMBER(TRIM(latitude),  10, 6) BETWEEN -90  AND 90
     AND TRY_TO_NUMBER(TRIM(longitude), 10, 6) BETWEEN -180 AND 180)
                                                            AS is_valid_gps,

        (TRY_TO_NUMBER(TRIM(capacity)) > 0)                AS is_valid_capacity,

        CASE
            WHEN TRIM(station_id) IS NULL OR TRIM(station_id) = '' THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(latitude),  10, 6) NOT BETWEEN -90  AND 90  THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(longitude), 10, 6) NOT BETWEEN -180 AND 180 THEN FALSE
            ELSE TRUE
        END                                                 AS is_valid_record,

        CASE
            WHEN TRIM(station_id) IS NULL OR TRIM(station_id) = '' THEN 'NULL_STATION_ID'
            WHEN TRY_TO_NUMBER(TRIM(latitude), 10, 6) NOT BETWEEN -90 AND 90   THEN 'INVALID_LATITUDE'
            WHEN TRY_TO_NUMBER(TRIM(longitude), 10, 6) NOT BETWEEN -180 AND 180 THEN 'INVALID_LONGITUDE'
            ELSE NULL
        END                                                 AS validation_errors,

        _loaded_at, _source_file, _row_number
    FROM RAW.raw_stations
    WHERE TRIM(station_id) IS NOT NULL AND TRIM(station_id) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(station_id) ORDER BY _loaded_at DESC) = 1
) AS src
ON tgt.station_id = src.station_id
WHEN MATCHED THEN UPDATE SET
    tgt.station_name      = src.station_name,
    tgt.latitude          = src.latitude,
    tgt.longitude         = src.longitude,
    tgt.capacity          = src.capacity,
    tgt.neighborhood      = src.neighborhood,
    tgt.city_zone         = src.city_zone,
    tgt.install_date      = src.install_date,
    tgt.status            = src.status,
    tgt.is_valid_gps      = src.is_valid_gps,
    tgt.is_valid_capacity = src.is_valid_capacity,
    tgt.is_valid_record   = src.is_valid_record,
    tgt.validation_errors = src.validation_errors,
    tgt._validated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file      = src._source_file
WHEN NOT MATCHED THEN INSERT (
    station_id, station_name, latitude, longitude, capacity,
    neighborhood, city_zone, install_date, status,
    is_valid_gps, is_valid_capacity, is_valid_record, validation_errors,
    _loaded_at, _source_file, _row_number
) VALUES (
    src.station_id, src.station_name, src.latitude, src.longitude, src.capacity,
    src.neighborhood, src.city_zone, src.install_date, src.status,
    src.is_valid_gps, src.is_valid_capacity, src.is_valid_record, src.validation_errors,
    src._loaded_at, src._source_file, src._row_number
);

-- ============================================
-- MERGE: RAW → VAL_BIKES
-- ============================================

MERGE INTO val_bikes AS tgt
USING (
    SELECT
        TRIM(bike_id)                                       AS bike_id,
        UPPER(TRIM(bike_type))                              AS bike_type,
        UPPER(TRIM(status))                                 AS status,
        TRY_TO_DATE(TRIM(purchase_date))                   AS purchase_date,
        TRY_TO_DATE(TRIM(last_service_date))               AS last_service_date,
        ABS(TRY_TO_NUMBER(TRIM(odometer_km), 10, 2))       AS odometer_km,
        TRY_TO_NUMBER(TRIM(battery_level), 5, 2)           AS battery_level,
        TRIM(firmware_version)                              AS firmware_version,

        (UPPER(TRIM(bike_type)) = 'EBIKE')                 AS is_ebike,

        (TRY_TO_NUMBER(TRIM(battery_level), 5, 2) BETWEEN 0 AND 100)
                                                            AS is_valid_battery,

        (TRY_TO_NUMBER(TRIM(odometer_km), 10, 2) >= 0)    AS is_valid_odometer,

        CASE
            WHEN TRIM(bike_id) IS NULL OR TRIM(bike_id) = '' THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(battery_level), 5, 2) NOT BETWEEN 0 AND 100 THEN FALSE
            ELSE TRUE
        END                                                 AS is_valid_record,

        CASE
            WHEN TRIM(bike_id) IS NULL OR TRIM(bike_id) = '' THEN 'NULL_BIKE_ID'
            WHEN TRY_TO_NUMBER(TRIM(battery_level), 5, 2) NOT BETWEEN 0 AND 100 THEN 'INVALID_BATTERY'
            ELSE NULL
        END                                                 AS validation_errors,

        _loaded_at, _source_file, _row_number
    FROM RAW.raw_bikes
    WHERE TRIM(bike_id) IS NOT NULL AND TRIM(bike_id) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(bike_id) ORDER BY _loaded_at DESC) = 1
) AS src
ON tgt.bike_id = src.bike_id
WHEN MATCHED THEN UPDATE SET
    tgt.bike_type         = src.bike_type,
    tgt.status            = src.status,
    tgt.purchase_date     = src.purchase_date,
    tgt.last_service_date = src.last_service_date,
    tgt.odometer_km       = src.odometer_km,
    tgt.battery_level     = src.battery_level,
    tgt.firmware_version  = src.firmware_version,
    tgt.is_ebike          = src.is_ebike,
    tgt.is_valid_battery  = src.is_valid_battery,
    tgt.is_valid_odometer = src.is_valid_odometer,
    tgt.is_valid_record   = src.is_valid_record,
    tgt.validation_errors = src.validation_errors,
    tgt._validated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file      = src._source_file
WHEN NOT MATCHED THEN INSERT (
    bike_id, bike_type, status, purchase_date, last_service_date,
    odometer_km, battery_level, firmware_version, is_ebike,
    is_valid_battery, is_valid_odometer, is_valid_record, validation_errors,
    _loaded_at, _source_file, _row_number
) VALUES (
    src.bike_id, src.bike_type, src.status, src.purchase_date, src.last_service_date,
    src.odometer_km, src.battery_level, src.firmware_version, src.is_ebike,
    src.is_valid_battery, src.is_valid_odometer, src.is_valid_record, src.validation_errors,
    src._loaded_at, src._source_file, src._row_number
);

-- ============================================
-- MERGE: RAW → VAL_USERS
-- ============================================

MERGE INTO val_users AS tgt
USING (
    SELECT
        TRIM(user_id)                                           AS user_id,
        INITCAP(TRIM(customer_name))                            AS customer_name,
        TRY_TO_DATE(TRIM(dob))                                 AS dob,
        INITCAP(TRIM(gender))                                   AS gender,
        LOWER(TRIM(email))                                      AS email,
        TRIM(phone)                                             AS phone,
        TRIM(address)                                           AS address,
        INITCAP(TRIM(city))                                     AS city,
        INITCAP(TRIM(state))                                    AS state,
        TRIM(region)                                            AS region,
        UPPER(TRIM(kyc_status))                                 AS kyc_status,
        TRY_TO_DATE(TRIM(registration_date))                   AS registration_date,
        (UPPER(TRIM(is_student)) IN ('Y','YES','TRUE','1'))     AS is_student,
        NULLIF(TRIM(corporate_id), '')                          AS corporate_id,

        CASE
            WHEN TRIM(email) IS NULL OR TRIM(email) = '' THEN FALSE
            WHEN REGEXP_LIKE(LOWER(TRIM(email)), '.*@.*\\..*') THEN TRUE
            ELSE FALSE
        END                                                     AS is_valid_email,

        CASE
            WHEN TRIM(phone) IS NULL OR TRIM(phone) = '' THEN FALSE
            ELSE TRUE
        END                                                     AS is_valid_phone,

        CASE
            WHEN TRY_TO_DATE(TRIM(dob)) IS NULL THEN FALSE
            WHEN TRY_TO_DATE(TRIM(dob)) > CURRENT_DATE() THEN FALSE
            ELSE TRUE
        END                                                     AS is_valid_dob,

        CASE
            WHEN TRIM(user_id) IS NULL OR TRIM(user_id) = '' THEN FALSE
            WHEN NOT REGEXP_LIKE(LOWER(TRIM(email)), '.*@.*\\..*') THEN FALSE
            ELSE TRUE
        END                                                     AS is_valid_record,

        CASE
            WHEN TRIM(user_id) IS NULL OR TRIM(user_id) = '' THEN 'NULL_USER_ID'
            WHEN NOT REGEXP_LIKE(LOWER(TRIM(email)), '.*@.*\\..*') THEN 'INVALID_EMAIL'
            ELSE NULL
        END                                                     AS validation_errors,

        _loaded_at, _source_file, _row_number
    FROM RAW.raw_users
    WHERE TRIM(user_id) IS NOT NULL AND TRIM(user_id) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(user_id) ORDER BY _loaded_at DESC) = 1
) AS src
ON tgt.user_id = src.user_id
WHEN MATCHED THEN UPDATE SET
    tgt.customer_name     = src.customer_name,
    tgt.dob               = src.dob,
    tgt.gender            = src.gender,
    tgt.email             = src.email,
    tgt.phone             = src.phone,
    tgt.address           = src.address,
    tgt.city              = src.city,
    tgt.state             = src.state,
    tgt.region            = src.region,
    tgt.kyc_status        = src.kyc_status,
    tgt.registration_date = src.registration_date,
    tgt.is_student        = src.is_student,
    tgt.corporate_id      = src.corporate_id,
    tgt.is_valid_email    = src.is_valid_email,
    tgt.is_valid_phone    = src.is_valid_phone,
    tgt.is_valid_dob      = src.is_valid_dob,
    tgt.is_valid_record   = src.is_valid_record,
    tgt.validation_errors = src.validation_errors,
    tgt._validated_at     = CURRENT_TIMESTAMP(),
    tgt._source_file      = src._source_file
WHEN NOT MATCHED THEN INSERT (
    user_id, customer_name, dob, gender, email, phone,
    address, city, state, region, kyc_status, registration_date,
    is_student, corporate_id,
    is_valid_email, is_valid_phone, is_valid_dob, is_valid_record, validation_errors,
    _loaded_at, _source_file, _row_number
) VALUES (
    src.user_id, src.customer_name, src.dob, src.gender, src.email, src.phone,
    src.address, src.city, src.state, src.region, src.kyc_status, src.registration_date,
    src.is_student, src.corporate_id,
    src.is_valid_email, src.is_valid_phone, src.is_valid_dob, src.is_valid_record, src.validation_errors,
    src._loaded_at, src._source_file, src._row_number
);

-- ============================================
-- MERGE: RAW → VAL_RENTALS (with Anomaly Flags)
-- ============================================

MERGE INTO val_rentals AS tgt
USING (
    SELECT
        TRIM(r.rental_id)                                                 AS rental_id,
        TRIM(r.user_id)                                                   AS user_id,
        TRIM(r.bike_id)                                                   AS bike_id,
        TRIM(r.start_station_id)                                          AS start_station_id,
        TRIM(r.end_station_id)                                            AS end_station_id,
        TRY_TO_TIMESTAMP_NTZ(TRIM(r.start_time))                         AS start_time,
        TRY_TO_TIMESTAMP_NTZ(TRIM(r.end_time))                           AS end_time,
        TRY_TO_NUMBER(TRIM(r.duration_sec))                               AS duration_sec,
        ABS(TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3))                   AS distance_km,
        ABS(TRY_TO_NUMBER(TRIM(r.price), 10, 2))                         AS price,
        LOWER(TRIM(r.plan_type))                                          AS plan_type,
        LOWER(TRIM(r.channel))                                            AS channel,
        TRIM(r.device_info)                                               AS device_info,
        TRIM(r.start_gps)                                                 AS start_gps,
        TRIM(r.end_gps)                                                   AS end_gps,
        (UPPER(TRIM(r.is_flagged)) IN ('Y','YES','TRUE','1'))             AS is_flagged,

        -- ANOMALY: Ultra-short trip (< 60 sec)
        (TRY_TO_NUMBER(TRIM(r.duration_sec)) IS NOT NULL
         AND TRY_TO_NUMBER(TRIM(r.duration_sec)) < 60)                   AS flag_ultra_short_trip,

        -- ANOMALY: GPS mismatch (>20% difference between GPS-derived and reported distance)
        CASE
            WHEN udf_gps_distance_km(TRIM(r.start_gps), TRIM(r.end_gps)) IS NULL THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3) IS NULL THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3) = 0 THEN FALSE
            WHEN ABS(udf_gps_distance_km(TRIM(r.start_gps), TRIM(r.end_gps))
                   - TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3))
                 > 0.20 * TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3)
            THEN TRUE ELSE FALSE
        END                                                               AS flag_gps_mismatch,

        -- ANOMALY: E-bike with zero battery
        CASE
            WHEN UPPER(TRIM(b.bike_type)) = 'EBIKE'
             AND TRY_TO_NUMBER(TRIM(b.battery_level), 5, 2) = 0
            THEN TRUE ELSE FALSE
        END                                                               AS flag_zero_battery_ebike,

        -- ANOMALY: Unrealistic speed > 50 km/h
        CASE
            WHEN TRY_TO_NUMBER(TRIM(r.duration_sec)) > 0
             AND TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3) IS NOT NULL
             AND (TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3)
                  / (TRY_TO_NUMBER(TRIM(r.duration_sec)) / 3600.0)) > 50
            THEN TRUE ELSE FALSE
        END                                                               AS flag_unrealistic_speed,

        -- ANOMALY: Device reuse — placeholder, updated in post-processing
        FALSE                                                             AS flag_device_reuse,

        -- ANOMALY: Outside geofence — start GPS has no station within ~500m
        CASE
            WHEN TRIM(r.start_gps) IS NULL OR TRIM(r.start_gps) = '' THEN FALSE
            WHEN NOT EXISTS (
                SELECT 1 FROM RAW.raw_stations rs
                WHERE ABS(TRY_TO_NUMBER(SPLIT_PART(TRIM(r.start_gps),',',1), 10, 6)
                        - TRY_TO_NUMBER(TRIM(rs.latitude), 10, 6)) < 0.005
                  AND ABS(TRY_TO_NUMBER(SPLIT_PART(TRIM(r.start_gps),',',2), 10, 6)
                        - TRY_TO_NUMBER(TRIM(rs.longitude), 10, 6)) < 0.005
            ) THEN TRUE ELSE FALSE
        END                                                               AS flag_outside_geofence,

        -- Validation flags
        (TRY_TO_TIMESTAMP_NTZ(TRIM(r.end_time)) >= TRY_TO_TIMESTAMP_NTZ(TRIM(r.start_time)))
                                                                          AS is_valid_times,
        (TRY_TO_NUMBER(TRIM(r.distance_km), 10, 3) >= 0)                AS is_valid_distance,
        (TRY_TO_NUMBER(TRIM(r.price), 10, 2) >= 0)                      AS is_valid_price,

        (b.bike_id IS NOT NULL)                                          AS has_matching_bike,
        (s.station_id IS NOT NULL)                                       AS has_matching_station,

        CASE
            WHEN TRIM(r.rental_id) IS NULL OR TRIM(r.rental_id) = '' THEN FALSE
            WHEN TRY_TO_TIMESTAMP_NTZ(TRIM(r.end_time)) < TRY_TO_TIMESTAMP_NTZ(TRIM(r.start_time)) THEN FALSE
            ELSE TRUE
        END                                                               AS is_valid_record,

        CASE
            WHEN TRIM(r.rental_id) IS NULL OR TRIM(r.rental_id) = '' THEN 'NULL_RENTAL_ID'
            WHEN TRY_TO_TIMESTAMP_NTZ(TRIM(r.end_time)) < TRY_TO_TIMESTAMP_NTZ(TRIM(r.start_time)) THEN 'END_BEFORE_START'
            ELSE NULL
        END                                                               AS validation_errors,

        r._loaded_at, r._source_file, r._row_number
    FROM RAW.raw_rentals r
    LEFT JOIN RAW.raw_bikes    b ON TRIM(r.bike_id)         = TRIM(b.bike_id)
    LEFT JOIN RAW.raw_stations s ON TRIM(r.start_station_id) = TRIM(s.station_id)
    WHERE TRIM(r.rental_id) IS NOT NULL AND TRIM(r.rental_id) != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(r.rental_id) ORDER BY r._loaded_at DESC) = 1
) AS src
ON tgt.rental_id = src.rental_id
WHEN MATCHED THEN UPDATE SET
    tgt.user_id                 = src.user_id,
    tgt.bike_id                 = src.bike_id,
    tgt.start_station_id        = src.start_station_id,
    tgt.end_station_id          = src.end_station_id,
    tgt.start_time              = src.start_time,
    tgt.end_time                = src.end_time,
    tgt.duration_sec            = src.duration_sec,
    tgt.distance_km             = src.distance_km,
    tgt.price                   = src.price,
    tgt.plan_type               = src.plan_type,
    tgt.channel                 = src.channel,
    tgt.device_info             = src.device_info,
    tgt.start_gps               = src.start_gps,
    tgt.end_gps                 = src.end_gps,
    tgt.is_flagged              = src.is_flagged,
    tgt.flag_ultra_short_trip   = src.flag_ultra_short_trip,
    tgt.flag_gps_mismatch       = src.flag_gps_mismatch,
    tgt.flag_zero_battery_ebike = src.flag_zero_battery_ebike,
    tgt.flag_unrealistic_speed  = src.flag_unrealistic_speed,
    tgt.flag_device_reuse       = src.flag_device_reuse,
    tgt.flag_outside_geofence   = src.flag_outside_geofence,
    tgt.is_valid_times          = src.is_valid_times,
    tgt.is_valid_distance       = src.is_valid_distance,
    tgt.is_valid_price          = src.is_valid_price,
    tgt.has_matching_bike       = src.has_matching_bike,
    tgt.has_matching_station    = src.has_matching_station,
    tgt.is_valid_record         = src.is_valid_record,
    tgt.validation_errors       = src.validation_errors,
    tgt._validated_at           = CURRENT_TIMESTAMP(),
    tgt._source_file            = src._source_file
WHEN NOT MATCHED THEN INSERT (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    start_time, end_time, duration_sec, distance_km, price,
    plan_type, channel, device_info, start_gps, end_gps, is_flagged,
    flag_ultra_short_trip, flag_gps_mismatch, flag_zero_battery_ebike,
    flag_unrealistic_speed, flag_device_reuse, flag_outside_geofence,
    is_valid_times, is_valid_distance, is_valid_price,
    has_matching_bike, has_matching_station, is_valid_record, validation_errors,
    _loaded_at, _source_file, _row_number
) VALUES (
    src.rental_id, src.user_id, src.bike_id, src.start_station_id, src.end_station_id,
    src.start_time, src.end_time, src.duration_sec, src.distance_km, src.price,
    src.plan_type, src.channel, src.device_info, src.start_gps, src.end_gps, src.is_flagged,
    src.flag_ultra_short_trip, src.flag_gps_mismatch, src.flag_zero_battery_ebike,
    src.flag_unrealistic_speed, src.flag_device_reuse, src.flag_outside_geofence,
    src.is_valid_times, src.is_valid_distance, src.is_valid_price,
    src.has_matching_bike, src.has_matching_station, src.is_valid_record, src.validation_errors,
    src._loaded_at, src._source_file, src._row_number
);

-- ============================================
-- POST-PROCESSING: Anomaly Score + Device Reuse
-- ============================================

-- Compute composite anomaly_score and is_high_risk
UPDATE val_rentals
SET
    anomaly_score = (
        CASE WHEN flag_ultra_short_trip   THEN 1 ELSE 0 END +
        CASE WHEN flag_gps_mismatch       THEN 1 ELSE 0 END +
        CASE WHEN flag_zero_battery_ebike THEN 1 ELSE 0 END +
        CASE WHEN flag_unrealistic_speed  THEN 1 ELSE 0 END +
        CASE WHEN flag_device_reuse       THEN 1 ELSE 0 END +
        CASE WHEN flag_outside_geofence   THEN 1 ELSE 0 END
    ),
    is_high_risk = (
        (CASE WHEN flag_ultra_short_trip   THEN 1 ELSE 0 END +
         CASE WHEN flag_gps_mismatch       THEN 1 ELSE 0 END +
         CASE WHEN flag_zero_battery_ebike THEN 1 ELSE 0 END +
         CASE WHEN flag_unrealistic_speed  THEN 1 ELSE 0 END +
         CASE WHEN flag_device_reuse       THEN 1 ELSE 0 END +
         CASE WHEN flag_outside_geofence   THEN 1 ELSE 0 END
        ) >= 2
    );

-- Flag device reuse: same device used by different users within 24h
UPDATE val_rentals v
SET flag_device_reuse = TRUE,
    anomaly_score     = anomaly_score + 1
WHERE device_info IS NOT NULL
  AND device_info  != ''
  AND EXISTS (
      SELECT 1 FROM val_rentals v2
      WHERE v2.device_info = v.device_info
        AND v2.user_id    != v.user_id
        AND ABS(DATEDIFF('hour', v.start_time, v2.start_time)) <= 24
  )
  AND flag_device_reuse = FALSE;

-- Recompute is_high_risk after device_reuse update
UPDATE val_rentals
SET is_high_risk = (anomaly_score >= 2);
