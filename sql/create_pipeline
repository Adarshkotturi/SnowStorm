
1. File Upload to Stages

-- Station files (CSV)
PUT file:///path/to/stations_master.csv @RAW.raw_stations_stage/master/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
PUT file:///path/to/stations_inc.csv @RAW.raw_stations_stage/incremental/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

-- Bike files (CSV)
PUT file:///path/to/bikes_master.csv @RAW.raw_bikes_stage/master/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
PUT file:///path/to/bikes_inc.csv @RAW.raw_bikes_stage/incremental/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

-- Rental files (CSV + NDJSON)
PUT file:///path/to/rentals_master.csv @RAW.raw_rentals_stage/master/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
PUT file:///path/to/rentals_inc.csv @RAW.raw_rentals_stage/incremental/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

-- User files (CSV)
PUT file:///path/to/users_master.csv @RAW.raw_users_stage/master/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
PUT file:///path/to/users_inc.csv @RAW.raw_users_stage/incremental/
  AUTO_COMPRESS = TRUE OVERWRITE = TRUE;

-- Verify
LIST @RAW.raw_stations_stage;
LIST @RAW.raw_bikes_stage;
LIST @RAW.raw_rentals_stage;
LIST @RAW.raw_users_stage;
2. COPY INTO (RAW Layer Load)
-- Load stations (CSV → RAW)
COPY INTO RAW.raw_stations (
    station_id, station_name, latitude, longitude, capacity,
    neighborhood, city_zone, install_date, status
)
FROM @RAW.raw_stations_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR   = 'CONTINUE'
PURGE      = FALSE
FORCE      = FALSE;

-- Load bikes (CSV → RAW)
COPY INTO RAW.raw_bikes (
    bike_id, bike_type, status, purchase_date, last_service_date,
    odometer_km, battery_level, firmware_version
)
FROM @RAW.raw_bikes_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- Load rentals (CSV → RAW)
COPY INTO RAW.raw_rentals (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    start_time, end_time, duration_sec, distance_km, price,
    plan_type, channel, device_info, start_gps, end_gps, is_flagged
)
FROM @RAW.raw_rentals_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- Load rentals (NDJSON → RAW) — for NDJSON files
COPY INTO RAW.raw_rentals (
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
    FROM @RAW.raw_rentals_stage
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.ndjson_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- Load users (CSV → RAW)
COPY INTO RAW.raw_users (
    user_id, customer_name, dob, gender, email, phone,
    address, city, state, region, kyc_status,
    registration_date, is_student, corporate_id
)
FROM @RAW.raw_users_stage
FILE_FORMAT = (FORMAT_NAME = 'RAW.csv_format')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE
FORCE       = FALSE;

-- Validate load counts
SELECT 'raw_stations' AS table_name, COUNT(*) AS row_count FROM RAW.raw_stations
UNION ALL SELECT 'raw_bikes',   COUNT(*) FROM RAW.raw_bikes
UNION ALL SELECT 'raw_rentals', COUNT(*) FROM RAW.raw_rentals
UNION ALL SELECT 'raw_users',   COUNT(*) FROM RAW.raw_users;

-- Audit log
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_stations', COUNT(*), 'SUCCESS' FROM RAW.raw_stations;
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_bikes', COUNT(*), 'SUCCESS' FROM RAW.raw_bikes;
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_rentals', COUNT(*), 'SUCCESS' FROM RAW.raw_rentals;
INSERT INTO AUDIT.audit_log (operation, target_table, rows_affected, status)
SELECT 'LOAD', 'raw_users', COUNT(*), 'SUCCESS' FROM RAW.raw_users;
4. JavaScript UDFs
USE SCHEMA VALIDATED;

-- Email Validation
CREATE OR REPLACE FUNCTION udf_validate_email(email VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!EMAIL) return false;
    var pattern = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
    return pattern.test(EMAIL.trim());
$$;

-- Phone Validation
CREATE OR REPLACE FUNCTION udf_validate_phone(phone VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!PHONE) return false;
    var digits = PHONE.replace(/\D/g, '');
    return digits.length === 10 || (digits.length === 11 && digits[0] === '1');
$$;

-- GPS Validation (lat: -90 to 90, lon: -180 to 180)
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
    return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
$$;

-- Battery Level Validation (0-100)
CREATE OR REPLACE FUNCTION udf_validate_battery(battery VARCHAR)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (!BATTERY) return false;
    var b = parseFloat(BATTERY);
    if (isNaN(b)) return false;
    return b >= 0 && b <= 100;
$$;

-- Ultra-Short Trip Flag (< 60 seconds)
CREATE OR REPLACE FUNCTION udf_is_ultra_short_trip(duration_sec NUMBER)
RETURNS BOOLEAN
LANGUAGE JAVASCRIPT
AS
$$
    if (DURATION_SEC === null || DURATION_SEC === undefined) return false;
    return DURATION_SEC < 60;
$$;

-- GPS-derived Distance (Haversine formula, returns km)
CREATE OR REPLACE FUNCTION udf_gps_distance_km(gps_start VARCHAR, gps_end VARCHAR)
RETURNS NUMBER
LANGUAGE JAVASCRIPT
AS
$$
    if (!GPS_START || !GPS_END) return null;
    var p1 = GPS_START.split(',');
    var p2 = GPS_END.split(',');
    if (p1.length !== 2 || p2.length !== 2) return null;
    var lat1 = parseFloat(p1[0]), lon1 = parseFloat(p1[1]);
    var lat2 = parseFloat(p2[0]), lon2 = parseFloat(p2[1]);
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
3. Streams (CDC)
-- Streams on RAW tables (append-only — only INSERT events matter)
USE SCHEMA RAW;

CREATE OR REPLACE STREAM stream_raw_stations
  ON TABLE RAW.raw_stations
  APPEND_ONLY   = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream for raw station data';

CREATE OR REPLACE STREAM stream_raw_bikes
  ON TABLE RAW.raw_bikes
  APPEND_ONLY   = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream for raw bike data';

CREATE OR REPLACE STREAM stream_raw_rentals
  ON TABLE RAW.raw_rentals
  APPEND_ONLY   = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream for raw rental data';

CREATE OR REPLACE STREAM stream_raw_users
  ON TABLE RAW.raw_users
  APPEND_ONLY   = TRUE
  SHOW_INITIAL_ROWS = TRUE
  COMMENT = 'CDC stream for raw user data';

-- Streams on VALIDATED tables (for CURATED layer)
USE SCHEMA VALIDATED;

CREATE OR REPLACE STREAM stream_val_stations
  ON TABLE VALIDATED.val_stations
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream for validated stations';

CREATE OR REPLACE STREAM stream_val_bikes
  ON TABLE VALIDATED.val_bikes
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream for validated bikes';

CREATE OR REPLACE STREAM stream_val_rentals
  ON TABLE VALIDATED.val_rentals
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream for validated rentals';

CREATE OR REPLACE STREAM stream_val_users
  ON TABLE VALIDATED.val_users
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream for validated users';
4. MERGE: RAW → VALIDATED
4.1 Stations
MERGE INTO VALIDATED.val_stations AS tgt
USING (
    SELECT
        TRIM(station_id)                                        AS station_id,
        TRIM(station_name)                                      AS station_name,
        TRY_TO_NUMBER(TRIM(latitude), 10, 6)                   AS latitude,
        TRY_TO_NUMBER(TRIM(longitude), 10, 6)                  AS longitude,
        TRY_TO_NUMBER(TRIM(capacity))                           AS capacity,
        TRIM(neighborhood)                                      AS neighborhood,
        TRIM(city_zone)                                         AS city_zone,
        TRY_TO_DATE(TRIM(install_date))                        AS install_date,
        UPPER(TRIM(status))                                     AS status,

        -- GPS validation
        CASE
            WHEN TRY_TO_NUMBER(TRIM(latitude), 10, 6) BETWEEN -90  AND 90
             AND TRY_TO_NUMBER(TRIM(longitude), 10, 6) BETWEEN -180 AND 180
            THEN TRUE ELSE FALSE
        END                                                     AS is_valid_gps,

        -- Capacity validation
        CASE
            WHEN TRY_TO_NUMBER(TRIM(capacity)) > 0 THEN TRUE ELSE FALSE
        END                                                     AS is_valid_capacity,

        -- Composite validity
        CASE
            WHEN TRIM(station_id) IS NULL OR TRIM(station_id) = '' THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(latitude), 10, 6) NOT BETWEEN -90 AND 90 THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(longitude), 10, 6) NOT BETWEEN -180 AND 180 THEN FALSE
            ELSE TRUE
        END                                                     AS is_valid_record,

        CASE
            WHEN TRIM(station_id) IS NULL OR TRIM(station_id) = '' THEN 'NULL_STATION_ID'
            WHEN TRY_TO_NUMBER(TRIM(latitude), 10, 6) NOT BETWEEN -90 AND 90 THEN 'INVALID_LATITUDE'
            WHEN TRY_TO_NUMBER(TRIM(longitude), 10, 6) NOT BETWEEN -180 AND 180 THEN 'INVALID_LONGITUDE'
            ELSE NULL
        END                                                     AS validation_errors,

        _loaded_at,
        _source_file,
        _row_number
    FROM RAW.stream_raw_stations
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
4.2 Bikes
MERGE INTO VALIDATED.val_bikes AS tgt
USING (
    SELECT
        TRIM(bike_id)                                           AS bike_id,
        UPPER(TRIM(bike_type))                                  AS bike_type,
        UPPER(TRIM(status))                                     AS status,
        TRY_TO_DATE(TRIM(purchase_date))                       AS purchase_date,
        TRY_TO_DATE(TRIM(last_service_date))                   AS last_service_date,
        ABS(TRY_TO_NUMBER(TRIM(odometer_km), 10, 2))           AS odometer_km,
        TRY_TO_NUMBER(TRIM(battery_level), 5, 2)               AS battery_level,
        TRIM(firmware_version)                                  AS firmware_version,

        (UPPER(TRIM(bike_type)) = 'EBIKE')                     AS is_ebike,

        CASE
            WHEN TRY_TO_NUMBER(TRIM(battery_level), 5, 2) BETWEEN 0 AND 100 THEN TRUE
            ELSE FALSE
        END                                                     AS is_valid_battery,

        CASE
            WHEN TRY_TO_NUMBER(TRIM(odometer_km), 10, 2) >= 0 THEN TRUE
            ELSE FALSE
        END                                                     AS is_valid_odometer,

        CASE
            WHEN TRIM(bike_id) IS NULL OR TRIM(bike_id) = '' THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(battery_level), 5, 2) NOT BETWEEN 0 AND 100 THEN FALSE
            ELSE TRUE
        END                                                     AS is_valid_record,

        CASE
            WHEN TRIM(bike_id) IS NULL OR TRIM(bike_id) = '' THEN 'NULL_BIKE_ID'
            WHEN TRY_TO_NUMBER(TRIM(battery_level), 5, 2) NOT BETWEEN 0 AND 100 THEN 'INVALID_BATTERY'
            ELSE NULL
        END                                                     AS validation_errors,

        _loaded_at, _source_file, _row_number
    FROM RAW.stream_raw_bikes
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
4.3 Users
MERGE INTO VALIDATED.val_users AS tgt
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

        ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(
            IFF(TRIM(user_id) IS NULL OR TRIM(user_id)='',' NULL_USER_ID', NULL),
            IFF(NOT REGEXP_LIKE(LOWER(TRIM(email)),'.*@.*\\..*'), 'INVALID_EMAIL', NULL)
        ), '|')                                                 AS validation_errors,

        _loaded_at, _source_file, _row_number
    FROM RAW.stream_raw_users
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
4.4 Rentals (with Anomaly Flags)
MERGE INTO VALIDATED.val_rentals AS tgt
USING (
    SELECT
        TRIM(rental_id)                                                   AS rental_id,
        TRIM(user_id)                                                     AS user_id,
        TRIM(bike_id)                                                     AS bike_id,
        TRIM(start_station_id)                                            AS start_station_id,
        TRIM(end_station_id)                                              AS end_station_id,
        TRY_TO_TIMESTAMP_NTZ(TRIM(start_time))                           AS start_time,
        TRY_TO_TIMESTAMP_NTZ(TRIM(end_time))                             AS end_time,
        TRY_TO_NUMBER(TRIM(duration_sec))                                 AS duration_sec,
        ABS(TRY_TO_NUMBER(TRIM(distance_km), 10, 3))                     AS distance_km,
        ABS(TRY_TO_NUMBER(TRIM(price), 10, 2))                           AS price,
        LOWER(TRIM(plan_type))                                            AS plan_type,
        LOWER(TRIM(channel))                                              AS channel,
        TRIM(device_info)                                                 AS device_info,
        TRIM(start_gps)                                                   AS start_gps,
        TRIM(end_gps)                                                     AS end_gps,
        (UPPER(TRIM(is_flagged)) IN ('Y','YES','TRUE','1'))               AS is_flagged,

        -- Anomaly: ultra-short trip
        (TRY_TO_NUMBER(TRIM(duration_sec)) < 60
         AND TRY_TO_NUMBER(TRIM(duration_sec)) IS NOT NULL)               AS flag_ultra_short_trip,

        -- Anomaly: GPS mismatch (gps-derived distance vs reported > 20%)
        CASE
            WHEN VALIDATED.udf_gps_distance_km(TRIM(start_gps), TRIM(end_gps)) IS NULL THEN FALSE
            WHEN TRY_TO_NUMBER(TRIM(distance_km), 10, 3) IS NULL THEN FALSE
            WHEN ABS(VALIDATED.udf_gps_distance_km(TRIM(start_gps), TRIM(end_gps))
                   - TRY_TO_NUMBER(TRIM(distance_km), 10, 3))
                 > 0.20 * TRY_TO_NUMBER(TRIM(distance_km), 10, 3) THEN TRUE
            ELSE FALSE
        END                                                               AS flag_gps_mismatch,

        -- Anomaly: e-bike with zero battery
        CASE
            WHEN UPPER(TRIM(bike_type_ref.bike_type)) = 'EBIKE'
             AND TRY_TO_NUMBER(TRIM(bike_type_ref.battery_level), 5, 2) = 0
            THEN TRUE ELSE FALSE
        END                                                               AS flag_zero_battery_ebike,

        -- Anomaly: unrealistic speed > 50 km/h
        CASE
            WHEN TRY_TO_NUMBER(TRIM(duration_sec)) > 0
             AND TRY_TO_NUMBER(TRIM(distance_km), 10, 3) IS NOT NULL
             AND (TRY_TO_NUMBER(TRIM(distance_km), 10, 3)
                  / (TRY_TO_NUMBER(TRIM(duration_sec)) / 3600.0)) > 50
            THEN TRUE ELSE FALSE
        END                                                               AS flag_unrealistic_speed,

        -- Anomaly: device_reuse (same device across multiple user_ids — flagged in post-processing)
        FALSE                                                             AS flag_device_reuse,

        -- Anomaly: outside geofence (simplified: GPS provided but no matching station)
        CASE
            WHEN TRIM(start_gps) IS NULL OR TRIM(start_gps) = '' THEN FALSE
            WHEN NOT EXISTS (
                SELECT 1 FROM RAW.raw_stations rs
                WHERE ABS(TRY_TO_NUMBER(SPLIT_PART(TRIM(start_gps),',',1), 10,6) - TRY_TO_NUMBER(rs.latitude,10,6)) < 0.005
                  AND ABS(TRY_TO_NUMBER(SPLIT_PART(TRIM(start_gps),',',2), 10,6) - TRY_TO_NUMBER(rs.longitude,10,6)) < 0.005
            ) THEN TRUE
            ELSE FALSE
        END                                                               AS flag_outside_geofence,

        -- Validation flags
        (TRY_TO_TIMESTAMP_NTZ(TRIM(end_time)) >= TRY_TO_TIMESTAMP_NTZ(TRIM(start_time)))
                                                                          AS is_valid_times,
        (TRY_TO_NUMBER(TRIM(distance_km), 10, 3) >= 0)                  AS is_valid_distance,
        (TRY_TO_NUMBER(TRIM(price), 10, 2) >= 0)                        AS is_valid_price,

        -- FK checks
        (EXISTS (SELECT 1 FROM RAW.raw_bikes rb WHERE TRIM(rb.bike_id) = TRIM(r.bike_id)))
                                                                          AS has_matching_bike,
        (EXISTS (SELECT 1 FROM RAW.raw_stations rs WHERE TRIM(rs.station_id) = TRIM(r.start_station_id)))
                                                                          AS has_matching_station,

        _loaded_at, _source_file, _row_number
    FROM RAW.stream_raw_rentals r
    LEFT JOIN RAW.raw_bikes bike_type_ref ON TRIM(r.bike_id) = TRIM(bike_type_ref.bike_id)
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
    src.has_matching_bike, src.has_matching_station,
    (src.is_valid_times AND src.is_valid_distance AND src.is_valid_price),
    NULL,
    src._loaded_at, src._source_file, src._row_number
);

-- Post-processing: compute anomaly_score and is_high_risk
UPDATE VALIDATED.val_rentals
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
         CASE WHEN flag_outside_geofence   THEN 1 ELSE 0 END) >= 2
    );

-- Device reuse: flag device_info used by multiple user_ids in a 24h window
UPDATE VALIDATED.val_rentals v
SET flag_device_reuse = TRUE
WHERE v.device_info IS NOT NULL
  AND v.device_info != ''
  AND EXISTS (
      SELECT 1 FROM VALIDATED.val_rentals v2
      WHERE v2.device_info = v.device_info
        AND v2.user_id != v.user_id
        AND ABS(DATEDIFF('hour', v.start_time, v2.start_time)) <= 24
  );
5. MERGE: VALIDATED → CURATED
5.1 dim_stations (SCD Type-2)
-- Step 1: expire old records where attributes changed
UPDATE CURATED.dim_stations tgt
SET
    valid_to   = CURRENT_TIMESTAMP(),
    is_current = FALSE,
    _updated_at = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
  AND EXISTS (
      SELECT 1 FROM VALIDATED.stream_val_stations src
      WHERE src.station_id = tgt.station_id
        AND src.is_valid_record = TRUE
        AND (src.station_name != tgt.station_name
          OR src.capacity     != tgt.capacity
          OR src.status       != tgt.status
          OR src.city_zone    != tgt.city_zone)
  );

-- Step 2: insert new current version
INSERT INTO CURATED.dim_stations (
    station_id, station_name, latitude, longitude, capacity,
    neighborhood, city_zone, install_date, status,
    valid_from, valid_to, is_current, _source_file
)
SELECT
    src.station_id, src.station_name, src.latitude, src.longitude, src.capacity,
    src.neighborhood, src.city_zone, src.install_date, src.status,
    CURRENT_TIMESTAMP(), NULL, TRUE, src._source_file
FROM VALIDATED.stream_val_stations src
WHERE src.is_valid_record = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM CURATED.dim_stations tgt
      WHERE tgt.station_id = src.station_id AND tgt.is_current = TRUE
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.station_id ORDER BY src._validated_at DESC) = 1;
5.2 dim_bikes (SCD Type-2)
-- Expire changed records
UPDATE CURATED.dim_bikes tgt
SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE, _updated_at = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
  AND EXISTS (
      SELECT 1 FROM VALIDATED.stream_val_bikes src
      WHERE src.bike_id = tgt.bike_id
        AND src.is_valid_record = TRUE
        AND (src.battery_level    != tgt.battery_level
          OR src.odometer_km      != tgt.odometer_km
          OR src.last_service_date != tgt.last_service_date
          OR src.firmware_version  != tgt.firmware_version
          OR src.status            != tgt.status)
  );

-- Insert new current version
INSERT INTO CURATED.dim_bikes (
    bike_id, bike_type, status, purchase_date, last_service_date,
    odometer_km, battery_level, firmware_version, is_ebike,
    valid_from, valid_to, is_current, _source_file
)
SELECT
    src.bike_id, src.bike_type, src.status, src.purchase_date, src.last_service_date,
    src.odometer_km, src.battery_level, src.firmware_version, src.is_ebike,
    CURRENT_TIMESTAMP(), NULL, TRUE, src._source_file
FROM VALIDATED.stream_val_bikes src
WHERE src.is_valid_record = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM CURATED.dim_bikes tgt
      WHERE tgt.bike_id = src.bike_id AND tgt.is_current = TRUE
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.bike_id ORDER BY src._validated_at DESC) = 1;
5.3 dim_users (SCD Type-2)
-- Expire changed records
UPDATE CURATED.dim_users tgt
SET valid_to = CURRENT_TIMESTAMP(), is_current = FALSE, _updated_at = CURRENT_TIMESTAMP()
WHERE is_current = TRUE
  AND EXISTS (
      SELECT 1 FROM VALIDATED.stream_val_users src
      WHERE src.user_id = tgt.user_id
        AND src.is_valid_record = TRUE
        AND (src.email     != tgt.email
          OR src.phone     != tgt.phone
          OR src.kyc_status != tgt.kyc_status
          OR src.region    != tgt.region)
  );

-- Insert new current version
INSERT INTO CURATED.dim_users (
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
FROM VALIDATED.stream_val_users src
WHERE src.is_valid_record = TRUE
  AND NOT EXISTS (
      SELECT 1 FROM CURATED.dim_users tgt
      WHERE tgt.user_id = src.user_id AND tgt.is_current = TRUE
  )
QUALIFY ROW_NUMBER() OVER (PARTITION BY src.user_id ORDER BY src._validated_at DESC) = 1;
5.4 fact_rentals (MERGE + Derived Fields)
MERGE INTO CURATED.fact_rentals AS tgt
USING (
    SELECT
        vr.rental_id,
        vr.user_id,
        vr.bike_id,
        vr.start_station_id,
        vr.end_station_id,

        -- Surrogate key lookups
        du.sk_user,
        db.sk_bike                                              AS sk_bike_start,
        ss.sk_station                                           AS sk_start_station,
        es.sk_station                                           AS sk_end_station,

        vr.start_time,
        vr.end_time,
        vr.duration_sec,
        ROUND(vr.duration_sec / 60.0, 2)                       AS duration_min,
        vr.distance_km,
        vr.price,
        vr.plan_type,
        vr.channel,
        vr.device_info,
        vr.start_gps,
        vr.end_gps,
        vr.is_flagged,

        -- Time attributes
        DAYNAME(vr.start_time)                                  AS day_of_week,
        HOUR(vr.start_time)                                     AS hour_of_day,
        WEEKOFYEAR(vr.start_time)                               AS week_of_year,

        -- Speed
        CASE
            WHEN vr.duration_sec > 0
            THEN ROUND(vr.distance_km / (vr.duration_sec / 3600.0), 2)
            ELSE NULL
        END                                                     AS speed_kmh,

        -- Revenue bucket
        CASE
            WHEN vr.price < 50   THEN 'LOW'
            WHEN vr.price < 150  THEN 'MEDIUM'
            WHEN vr.price < 500  THEN 'HIGH'
            ELSE 'PREMIUM'
        END                                                     AS revenue_bucket,

        -- Anomaly fields
        vr.flag_ultra_short_trip,
        vr.flag_gps_mismatch,
        vr.flag_zero_battery_ebike,
        vr.flag_unrealistic_speed,
        vr.flag_device_reuse,
        vr.flag_outside_geofence,
        vr.anomaly_score,
        vr.is_high_risk,
        vr._source_file

    FROM VALIDATED.stream_val_rentals vr
    LEFT JOIN CURATED.dim_users    du ON vr.user_id         = du.user_id    AND du.is_current = TRUE
    LEFT JOIN CURATED.dim_bikes    db ON vr.bike_id          = db.bike_id    AND db.is_current = TRUE
    LEFT JOIN CURATED.dim_stations ss ON vr.start_station_id = ss.station_id AND ss.is_current = TRUE
    LEFT JOIN CURATED.dim_stations es ON vr.end_station_id   = es.station_id AND es.is_current = TRUE
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
6. Tasks (Automation)
-- Dedicated warehouse for pipeline
CREATE OR REPLACE WAREHOUSE task_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  COMMENT = 'Dedicated warehouse for CityRide pipeline tasks';

-- Task: RAW → VALIDATED Stations
CREATE OR REPLACE TASK task_raw_to_val_stations
  WAREHOUSE = task_wh
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_stations')
AS
  -- (MERGE from Section 6.1)
  CALL SYSTEM$LOG('INFO', 'task_raw_to_val_stations completed');

-- Task: RAW → VALIDATED Bikes
CREATE OR REPLACE TASK task_raw_to_val_bikes
  WAREHOUSE = task_wh
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_bikes')
AS
  -- (MERGE from Section 6.2)
  CALL SYSTEM$LOG('INFO', 'task_raw_to_val_bikes completed');

-- Task: RAW → VALIDATED Users
CREATE OR REPLACE TASK task_raw_to_val_users
  WAREHOUSE = task_wh
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_users')
AS
  -- (MERGE from Section 6.3)
  CALL SYSTEM$LOG('INFO', 'task_raw_to_val_users completed');

-- Task: RAW → VALIDATED Rentals
CREATE OR REPLACE TASK task_raw_to_val_rentals
  WAREHOUSE = task_wh
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('RAW.stream_raw_rentals')
AS
  -- (MERGE from Section 6.4 + anomaly score UPDATE)
  CALL SYSTEM$LOG('INFO', 'task_raw_to_val_rentals completed');

-- Task: VALIDATED → CURATED Dimensions (SCD2)
CREATE OR REPLACE TASK task_curate_dimensions
  WAREHOUSE = task_wh
  AFTER task_raw_to_val_stations, task_raw_to_val_bikes, task_raw_to_val_users
AS
BEGIN
  -- Step A: dim_stations SCD2 (Section 7.1)
  -- Step B: dim_bikes    SCD2 (Section 7.2)
  -- Step C: dim_users    SCD2 (Section 7.3)
  CALL SYSTEM$LOG('INFO', 'task_curate_dimensions completed');
END;

-- Task: VALIDATED → CURATED Fact Rentals
CREATE OR REPLACE TASK task_curate_rentals
  WAREHOUSE = task_wh
  AFTER task_curate_dimensions, task_raw_to_val_rentals
AS
  -- (MERGE from Section 5.4)
  CALL SYSTEM$LOG('INFO', 'task_curate_rentals completed');

-- Resume all tasks (reverse dependency order — children first)
ALTER TASK task_curate_rentals      RESUME;
ALTER TASK task_curate_dimensions   RESUME;
ALTER TASK task_raw_to_val_rentals  RESUME;
ALTER TASK task_raw_to_val_users    RESUME;
ALTER TASK task_raw_to_val_bikes    RESUME;
ALTER TASK task_raw_to_val_stations RESUME;

-- Monitor
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
));
