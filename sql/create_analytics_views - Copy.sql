USE DATABASE CITYRIDE_DB;
USE SCHEMA ANALYTICS;

-- ============================================
-- KPI ANALYTICS VIEWS — CityRide
-- ============================================

/* ============================================================
   KPI 1: Anomalous Rental Probability Score
   Definition: (Count of rentals marked high-risk by rules + anomaly score)
               / (Total rentals) * 100
   Use: Prioritize investigations (abuse, theft risk, device/account misuse)
   ============================================================ */

CREATE OR REPLACE VIEW v_kpi1_anomaly_score AS
WITH base AS (
    SELECT
        COUNT(*)                                        AS total_rentals,
        SUM(CASE WHEN is_high_risk = TRUE THEN 1 ELSE 0 END)
                                                        AS high_risk_rentals,
        SUM(CASE WHEN flag_ultra_short_trip   THEN 1 ELSE 0 END) AS cnt_ultra_short,
        SUM(CASE WHEN flag_gps_mismatch       THEN 1 ELSE 0 END) AS cnt_gps_mismatch,
        SUM(CASE WHEN flag_zero_battery_ebike THEN 1 ELSE 0 END) AS cnt_zero_battery,
        SUM(CASE WHEN flag_unrealistic_speed  THEN 1 ELSE 0 END) AS cnt_unreal_speed,
        SUM(CASE WHEN flag_device_reuse       THEN 1 ELSE 0 END) AS cnt_device_reuse,
        SUM(CASE WHEN flag_outside_geofence   THEN 1 ELSE 0 END) AS cnt_outside_geofence
    FROM CURATED.fact_rentals
),
by_zone AS (
    SELECT
        ss.city_zone,
        COUNT(fr.rental_id)                            AS zone_rentals,
        SUM(CASE WHEN fr.is_high_risk THEN 1 ELSE 0 END) AS zone_high_risk,
        ROUND(100.0 * SUM(CASE WHEN fr.is_high_risk THEN 1 ELSE 0 END)
              / NULLIF(COUNT(fr.rental_id), 0), 2)     AS zone_anomaly_pct
    FROM CURATED.fact_rentals fr
    LEFT JOIN CURATED.dim_stations ss
           ON fr.start_station_id = ss.station_id AND ss.is_current = TRUE
    GROUP BY ss.city_zone
)
SELECT
    b.total_rentals,
    b.high_risk_rentals,
    ROUND(100.0 * b.high_risk_rentals / NULLIF(b.total_rentals, 0), 2)
                                                        AS anomalous_rental_probability_score,
    b.cnt_ultra_short,
    b.cnt_gps_mismatch,
    b.cnt_zero_battery,
    b.cnt_unreal_speed,
    b.cnt_device_reuse,
    b.cnt_outside_geofence,
    z.city_zone,
    z.zone_rentals,
    z.zone_high_risk,
    z.zone_anomaly_pct
FROM base b
CROSS JOIN by_zone z
ORDER BY z.zone_anomaly_pct DESC;


/* ============================================================
   KPI 2: Station Availability Score
   Definition: % of time a station simultaneously has ≥1 bike AND ≥1 free dock
   Use: Measures service level; low score indicates rebalancing need
   Note: Approximated from rental data — station is unavailable when
         active rentals at a station = capacity (full) or 0 bikes
   ============================================================ */

CREATE OR REPLACE VIEW v_kpi2_station_availability AS
WITH station_capacity AS (
    SELECT station_id, station_name, city_zone, neighborhood, capacity
    FROM CURATED.dim_stations
    WHERE is_current = TRUE
),
rental_activity AS (
    SELECT
        start_station_id                                AS station_id,
        start_time,
        end_time,
        1                                               AS bikes_out
    FROM CURATED.fact_rentals
    WHERE start_station_id IS NOT NULL
),
-- For each station: count total rental hours and estimate available hours
station_stats AS (
    SELECT
        sc.station_id,
        sc.station_name,
        sc.city_zone,
        sc.neighborhood,
        sc.capacity,
        COUNT(ra.rental_id)                             AS total_rentals,
        SUM(ra.duration_min)                            AS total_rental_minutes,
        AVG(ra.duration_min)                            AS avg_rental_duration_min,
        -- Estimate concurrent utilization: avg bikes out vs capacity
        ROUND(AVG(ra.duration_min) * COUNT(ra.rental_id) / NULLIF(sc.capacity * 60 * 24, 0) * 100, 2)
                                                        AS estimated_utilization_pct
    FROM station_capacity sc
    LEFT JOIN CURATED.fact_rentals ra ON sc.station_id = ra.start_station_id
    GROUP BY sc.station_id, sc.station_name, sc.city_zone, sc.neighborhood, sc.capacity
)
SELECT
    station_id,
    station_name,
    city_zone,
    neighborhood,
    capacity,
    total_rentals,
    avg_rental_duration_min,
    estimated_utilization_pct,
    -- Availability score: 100 - estimated utilization (proxy for free dock availability)
    GREATEST(0, ROUND(100 - estimated_utilization_pct, 2))
                                                        AS station_availability_score,
    CASE
        WHEN estimated_utilization_pct > 80 THEN 'REBALANCE_NEEDED'
        WHEN estimated_utilization_pct > 60 THEN 'MONITOR'
        ELSE 'HEALTHY'
    END                                                 AS availability_status
FROM station_stats
ORDER BY station_availability_score ASC;


/* ============================================================
   KPI 3: Active Rider Engagement Ratio
   Definition: % of registered riders with ≥1 rental in the last 30 days
   Use: Engagement and dormancy trend
   ============================================================ */

CREATE OR REPLACE VIEW v_kpi3_rider_engagement AS
WITH all_riders AS (
    SELECT user_id, registration_date, is_student, corporate_id, region
    FROM CURATED.dim_users
    WHERE is_current = TRUE
),
active_riders AS (
    SELECT DISTINCT user_id
    FROM CURATED.fact_rentals
    WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
),
engagement_summary AS (
    SELECT
        COUNT(DISTINCT ar.user_id)                      AS total_registered_riders,
        COUNT(DISTINCT act.user_id)                     AS active_riders_last_30d,
        ROUND(100.0 * COUNT(DISTINCT act.user_id)
              / NULLIF(COUNT(DISTINCT ar.user_id), 0), 2)
                                                        AS active_rider_engagement_ratio
    FROM all_riders ar
    LEFT JOIN active_riders act ON ar.user_id = act.user_id
),
by_region AS (
    SELECT
        ar.region,
        COUNT(DISTINCT ar.user_id)                      AS region_total,
        COUNT(DISTINCT act.user_id)                     AS region_active,
        ROUND(100.0 * COUNT(DISTINCT act.user_id)
              / NULLIF(COUNT(DISTINCT ar.user_id), 0), 2)
                                                        AS region_engagement_pct
    FROM all_riders ar
    LEFT JOIN active_riders act ON ar.user_id = act.user_id
    GROUP BY ar.region
),
by_plan AS (
    SELECT
        fr.plan_type,
        COUNT(DISTINCT fr.user_id)                      AS plan_active_riders,
        ROUND(AVG(fr.price), 2)                         AS avg_price,
        COUNT(*)                                        AS rentals_in_30d
    FROM CURATED.fact_rentals fr
    WHERE fr.start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY fr.plan_type
)
SELECT
    es.total_registered_riders,
    es.active_riders_last_30d,
    es.active_rider_engagement_ratio,
    br.region,
    br.region_total,
    br.region_active,
    br.region_engagement_pct,
    bp.plan_type,
    bp.plan_active_riders,
    bp.avg_price,
    bp.rentals_in_30d
FROM engagement_summary es
CROSS JOIN by_region br
CROSS JOIN by_plan bp
ORDER BY br.region_engagement_pct DESC;


/* ============================================================
   KPI 4: Fleet Maintenance Health Index
   Definition: % of bikes within all health thresholds:
               - No active error state (status = 'ACTIVE' or 'AVAILABLE')
               - battery_level ≥ 25% for e-bikes
               - service age ≤ policy (last_service_date ≤ 90 days ago)
               - odometer within limits (< 10,000 km)
   Use: Core metric for maintenance planning
   ============================================================ */

CREATE OR REPLACE VIEW v_kpi4_fleet_health AS
WITH bike_health AS (
    SELECT
        bike_id,
        bike_type,
        status,
        battery_level,
        odometer_km,
        last_service_date,
        firmware_version,
        is_ebike,

        -- Health check 1: Status is operational
        (UPPER(status) IN ('ACTIVE','AVAILABLE','IN_SERVICE'))
                                                        AS is_status_ok,

        -- Health check 2: Battery level ≥ 25% for e-bikes (classic bikes: always pass)
        CASE
            WHEN is_ebike = TRUE THEN (battery_level >= 25)
            ELSE TRUE
        END                                             AS is_battery_ok,

        -- Health check 3: Last serviced within 90 days
        (DATEDIFF('day', last_service_date, CURRENT_DATE()) <= 90)
                                                        AS is_service_age_ok,

        -- Health check 4: Odometer under 10,000 km
        (odometer_km < 10000)                           AS is_odometer_ok,

        -- All checks pass = healthy bike
        (
            (UPPER(status) IN ('ACTIVE','AVAILABLE','IN_SERVICE'))
            AND CASE WHEN is_ebike = TRUE THEN (battery_level >= 25) ELSE TRUE END
            AND (DATEDIFF('day', last_service_date, CURRENT_DATE()) <= 90)
            AND (odometer_km < 10000)
        )                                               AS is_healthy
    FROM CURATED.dim_bikes
    WHERE is_current = TRUE
),
summary AS (
    SELECT
        COUNT(*)                                        AS total_bikes,
        SUM(CASE WHEN is_healthy THEN 1 ELSE 0 END)    AS healthy_bikes,
        SUM(CASE WHEN is_ebike THEN 1 ELSE 0 END)      AS total_ebikes,
        SUM(CASE WHEN is_ebike AND NOT is_battery_ok THEN 1 ELSE 0 END)
                                                        AS low_battery_ebikes,
        SUM(CASE WHEN NOT is_service_age_ok THEN 1 ELSE 0 END)
                                                        AS overdue_service_bikes,
        SUM(CASE WHEN NOT is_odometer_ok THEN 1 ELSE 0 END)
                                                        AS high_odometer_bikes,
        SUM(CASE WHEN NOT is_status_ok THEN 1 ELSE 0 END)
                                                        AS inactive_bikes,
        ROUND(100.0 * SUM(CASE WHEN is_healthy THEN 1 ELSE 0 END)
              / NULLIF(COUNT(*), 0), 2)                 AS fleet_maintenance_health_index
    FROM bike_health
)
SELECT
    s.total_bikes,
    s.healthy_bikes,
    s.fleet_maintenance_health_index,
    s.total_ebikes,
    s.low_battery_ebikes,
    s.overdue_service_bikes,
    s.high_odometer_bikes,
    s.inactive_bikes,
    b.bike_id,
    b.bike_type,
    b.status,
    b.battery_level,
    b.odometer_km,
    b.last_service_date,
    b.is_healthy,
    b.is_battery_ok,
    b.is_service_age_ok,
    b.is_odometer_ok,
    CASE
        WHEN NOT b.is_service_age_ok THEN 'SERVICE_OVERDUE'
        WHEN NOT b.is_battery_ok     THEN 'LOW_BATTERY'
        WHEN NOT b.is_odometer_ok    THEN 'HIGH_ODOMETER'
        WHEN NOT b.is_status_ok      THEN 'INACTIVE'
        ELSE 'HEALTHY'
    END                                                 AS maintenance_flag
FROM summary s
CROSS JOIN bike_health b
ORDER BY b.is_healthy ASC, b.odometer_km DESC;


/* ============================================================
   KPI 5: Average Rental Revenue (ARR) by Channel
   Channels: App / Kiosk / Corporate
   Definition: Average price per completed rental for each channel
   Use: Revenue mix and channel optimization
   ============================================================ */

CREATE OR REPLACE VIEW v_kpi5_arr_by_channel AS
WITH channel_stats AS (
    SELECT
        UPPER(channel)                                  AS channel,
        COUNT(*)                                        AS total_rentals,
        SUM(price)                                      AS total_revenue,
        ROUND(AVG(price), 2)                            AS avg_revenue_per_rental,
        ROUND(MIN(price), 2)                            AS min_price,
        ROUND(MAX(price), 2)                            AS max_price,
        ROUND(AVG(duration_min), 2)                     AS avg_duration_min,
        ROUND(AVG(distance_km), 3)                      AS avg_distance_km,
        COUNT(DISTINCT user_id)                         AS unique_riders,
        SUM(CASE WHEN is_high_risk THEN 1 ELSE 0 END)  AS high_risk_count
    FROM CURATED.fact_rentals
    WHERE price IS NOT NULL
    GROUP BY UPPER(channel)
),
plan_channel_stats AS (
    SELECT
        UPPER(channel)                                  AS channel,
        UPPER(plan_type)                                AS plan_type,
        COUNT(*)                                        AS plan_rentals,
        ROUND(AVG(price), 2)                            AS plan_avg_revenue,
        SUM(price)                                      AS plan_total_revenue
    FROM CURATED.fact_rentals
    WHERE price IS NOT NULL
    GROUP BY UPPER(channel), UPPER(plan_type)
)
SELECT
    cs.channel,
    cs.total_rentals,
    cs.total_revenue,
    cs.avg_revenue_per_rental                           AS arr_by_channel,
    cs.min_price,
    cs.max_price,
    cs.avg_duration_min,
    cs.avg_distance_km,
    cs.unique_riders,
    cs.high_risk_count,
    ROUND(100.0 * cs.total_revenue
          / SUM(cs.total_revenue) OVER (), 2)           AS revenue_share_pct,
    ps.plan_type,
    ps.plan_rentals,
    ps.plan_avg_revenue,
    ps.plan_total_revenue
FROM channel_stats cs
LEFT JOIN plan_channel_stats ps ON cs.channel = ps.channel
ORDER BY cs.avg_revenue_per_rental DESC;


/* ============================================================
   BONUS: Anomaly Rule Catalog View
   Use: Audit trail for anomaly detection rules + hit rates
   ============================================================ */

CREATE OR REPLACE VIEW v_anomaly_rule_catalog AS
SELECT
    'RULE_01'                                           AS rule_id,
    'Ultra-Short Trip'                                  AS rule_name,
    'duration_sec < 60 seconds'                         AS rule_definition,
    'Abuse, accidental unlock, theft risk'              AS use_case,
    SUM(CASE WHEN flag_ultra_short_trip   THEN 1 ELSE 0 END) AS hit_count,
    COUNT(*)                                            AS total_rentals,
    ROUND(100.0 * SUM(CASE WHEN flag_ultra_short_trip THEN 1 ELSE 0 END)
          / NULLIF(COUNT(*), 0), 2)                     AS hit_rate_pct
FROM CURATED.fact_rentals
UNION ALL
SELECT
    'RULE_02', 'GPS Mismatch',
    'GPS-derived distance vs reported distance > 20%',
    'Telemetry manipulation, GPS spoofing',
    SUM(CASE WHEN flag_gps_mismatch THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN flag_gps_mismatch THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
FROM CURATED.fact_rentals
UNION ALL
SELECT
    'RULE_03', 'E-bike Zero Battery',
    'bike_type = EBIKE AND battery_level = 0',
    'Unreported usage, sensor failure, theft',
    SUM(CASE WHEN flag_zero_battery_ebike THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN flag_zero_battery_ebike THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
FROM CURATED.fact_rentals
UNION ALL
SELECT
    'RULE_04', 'Unrealistic Speed',
    'speed_kmh > 50 (derived from distance / duration)',
    'Data manipulation, motor tampering',
    SUM(CASE WHEN flag_unrealistic_speed THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN flag_unrealistic_speed THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
FROM CURATED.fact_rentals
UNION ALL
SELECT
    'RULE_05', 'Device Reuse Across Accounts',
    'Same device_info used by multiple user_ids within 24 hours',
    'Shared device abuse, account farming',
    SUM(CASE WHEN flag_device_reuse THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN flag_device_reuse THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
FROM CURATED.fact_rentals
UNION ALL
SELECT
    'RULE_06', 'Outside Station Geofence',
    'start_gps > ~500m from any known station',
    'Unauthorized pickup, station bypass',
    SUM(CASE WHEN flag_outside_geofence THEN 1 ELSE 0 END), COUNT(*),
    ROUND(100.0 * SUM(CASE WHEN flag_outside_geofence THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2)
FROM CURATED.fact_rentals;
