# RBAC + Dynamic Data Masking — CityRide

## Overview

Three roles control access to CURATED layer tables. Each role sees different rows and different levels of column masking.

| Role | User Type | Row Access | Email Masking | Phone | GPS |
|---|---|---|---|---|---|
| MANAGER_ROLE | Ops manager / Data engineer | All city_zones | Full (`rider@email.com`) | Full | Full precision |
| ANALYST_ROLE | Business analyst | All city_zones | Partial (`r****@email.com`) | Partial (`****XXXX`) | Truncated (2 dp) |
| OPS_ROLE | Field operations | Only assigned region/zone | Fully masked (`****`) | Fully masked | `0.000000,0.000000` |

---

## 1. Roles

```
ACCOUNTADMIN
    └── SYSADMIN
           ├── MANAGER_ROLE    ← full read on all CURATED tables, all zones
           ├── ANALYST_ROLE    ← all zones, PII masked at column level
           └── OPS_ROLE        ← filtered to assigned region only, PII masked
```

### What each role can access

| Table | MANAGER_ROLE | ANALYST_ROLE | OPS_ROLE |
|---|---|---|---|
| dim_stations | All rows | All rows | Only their zone |
| dim_bikes | All rows | All rows | All rows |
| dim_users | All rows | All rows (masked PII) | Only own region users (masked) |
| fact_rentals | All rows | All rows (masked GPS) | Only rentals in their zone |
| v_rentals_full | All rows | All rows (masked) | Filtered + masked |
| v_anomaly_rentals | All rows | All rows | Only their zone |

---

## 2. User-Role Mapping Table

```sql
USE DATABASE CITYRIDE_DB;
USE SCHEMA SECURITY;

CREATE OR REPLACE TABLE user_role_mapping (
    snowflake_user  VARCHAR(100)    NOT NULL,
    role_type       VARCHAR(30)     NOT NULL,   -- 'MANAGER', 'ANALYST', 'OPS'
    mapped_region   VARCHAR(100),               -- NULL for MANAGER/ANALYST
    mapped_zone     VARCHAR(100),               -- NULL for MANAGER/ANALYST
    _created_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_user_role_mapping PRIMARY KEY (snowflake_user)
);

INSERT INTO user_role_mapping (snowflake_user, role_type, mapped_region, mapped_zone) VALUES
    ('MANAGER_USER1',   'MANAGER',  NULL,    NULL),
    ('ANALYST_USER1',   'ANALYST',  NULL,    NULL),
    ('OPS_USER_NORTH',  'OPS',      'North', 'North-Zone'),
    ('OPS_USER_SOUTH',  'OPS',      'South', 'South-Zone');
```

---

## 3. Grant Privileges

```sql
USE ROLE SYSADMIN;

-- Database + schema + warehouse usage
GRANT USAGE ON DATABASE CITYRIDE_DB TO ROLE MANAGER_ROLE;
GRANT USAGE ON DATABASE CITYRIDE_DB TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE CITYRIDE_DB TO ROLE OPS_ROLE;

GRANT USAGE ON SCHEMA CITYRIDE_DB.CURATED   TO ROLE MANAGER_ROLE;
GRANT USAGE ON SCHEMA CITYRIDE_DB.CURATED   TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA CITYRIDE_DB.CURATED   TO ROLE OPS_ROLE;

GRANT USAGE ON SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE MANAGER_ROLE;
GRANT USAGE ON SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA CITYRIDE_DB.ANALYTICS TO ROLE OPS_ROLE;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MANAGER_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE OPS_ROLE;

-- Table-level grants
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED   TO ROLE MANAGER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED   TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CITYRIDE_DB.CURATED   TO ROLE OPS_ROLE;

GRANT SELECT ON ALL VIEWS IN SCHEMA CITYRIDE_DB.CURATED    TO ROLE MANAGER_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CITYRIDE_DB.CURATED    TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CITYRIDE_DB.CURATED    TO ROLE OPS_ROLE;

GRANT SELECT ON ALL VIEWS IN SCHEMA CITYRIDE_DB.ANALYTICS  TO ROLE MANAGER_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CITYRIDE_DB.ANALYTICS  TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CITYRIDE_DB.ANALYTICS  TO ROLE OPS_ROLE;

GRANT USAGE ON SCHEMA CITYRIDE_DB.SECURITY TO ROLE SYSADMIN;
```

---

## 4. Dynamic Data Masking Policies

### 4.1 Email Masking Policy

```sql
USE SCHEMA CITYRIDE_DB.SECURITY;

CREATE OR REPLACE MASKING POLICY mask_email AS (email_val VARCHAR)
RETURNS VARCHAR ->
CASE
    WHEN IS_ROLE_IN_SESSION('MANAGER_ROLE') THEN email_val
    WHEN IS_ROLE_IN_SESSION('ANALYST_ROLE') THEN
        CASE
            WHEN email_val IS NULL THEN NULL
            ELSE SUBSTR(email_val, 1, 1) || '****@' ||
                 SPLIT_PART(email_val, '@', 2)
        END
    ELSE '****'
END;
```

| Role | Input | Output |
|---|---|---|
| MANAGER_ROLE | `rider@example.com` | `rider@example.com` |
| ANALYST_ROLE | `rider@example.com` | `r****@example.com` |
| OPS_ROLE | `rider@example.com` | `****` |

### 4.2 Phone Masking Policy

```sql
CREATE OR REPLACE MASKING POLICY mask_phone AS (phone_val VARCHAR)
RETURNS VARCHAR ->
CASE
    WHEN IS_ROLE_IN_SESSION('MANAGER_ROLE') THEN phone_val
    WHEN IS_ROLE_IN_SESSION('ANALYST_ROLE') THEN
        CASE
            WHEN phone_val IS NULL THEN NULL
            ELSE '****' || RIGHT(REGEXP_REPLACE(phone_val, '\\D', ''), 4)
        END
    ELSE '****'
END;
```

| Role | Input | Output |
|---|---|---|
| MANAGER_ROLE | `+91 98765 43210` | `+91 98765 43210` |
| ANALYST_ROLE | `+91 98765 43210` | `****3210` |
| OPS_ROLE | `+91 98765 43210` | `****` |

### 4.3 GPS Masking Policy

```sql
CREATE OR REPLACE MASKING POLICY mask_gps AS (gps_val VARCHAR)
RETURNS VARCHAR ->
CASE
    WHEN IS_ROLE_IN_SESSION('MANAGER_ROLE') THEN gps_val
    WHEN IS_ROLE_IN_SESSION('ANALYST_ROLE') THEN
        -- Truncate to 2 decimal places (city-level precision only)
        CASE
            WHEN gps_val IS NULL THEN NULL
            ELSE TO_VARCHAR(ROUND(TRY_TO_NUMBER(SPLIT_PART(gps_val, ',', 1)), 2)) || ',' ||
                 TO_VARCHAR(ROUND(TRY_TO_NUMBER(SPLIT_PART(gps_val, ',', 2)), 2))
        END
    ELSE '0.000000,0.000000'
END;
```

| Role | Input | Output |
|---|---|---|
| MANAGER_ROLE | `12.971599,77.594566` | `12.971599,77.594566` |
| ANALYST_ROLE | `12.971599,77.594566` | `12.97,77.59` |
| OPS_ROLE | `12.971599,77.594566` | `0.000000,0.000000` |

---

## 5. Apply Masking Policies to Columns

```sql
-- Apply to dim_users
ALTER TABLE CITYRIDE_DB.CURATED.dim_users
    MODIFY COLUMN email SET MASKING POLICY CITYRIDE_DB.SECURITY.mask_email;

ALTER TABLE CITYRIDE_DB.CURATED.dim_users
    MODIFY COLUMN phone SET MASKING POLICY CITYRIDE_DB.SECURITY.mask_phone;

-- Apply to fact_rentals (GPS columns)
ALTER TABLE CITYRIDE_DB.CURATED.fact_rentals
    MODIFY COLUMN start_gps SET MASKING POLICY CITYRIDE_DB.SECURITY.mask_gps;

ALTER TABLE CITYRIDE_DB.CURATED.fact_rentals
    MODIFY COLUMN end_gps SET MASKING POLICY CITYRIDE_DB.SECURITY.mask_gps;
```

---

## 6. Row Access Policies

### 6.1 Row Access Policy — fact_rentals (by city_zone via start_station_id)

```sql
CREATE OR REPLACE ROW ACCESS POLICY rap_rentals_by_zone
AS (start_station_id VARCHAR) RETURNS BOOLEAN ->
CASE
    WHEN IS_ROLE_IN_SESSION('MANAGER_ROLE') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('ANALYST_ROLE') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('OPS_ROLE') THEN
        EXISTS (
            SELECT 1
            FROM CITYRIDE_DB.SECURITY.user_role_mapping urm
            JOIN CITYRIDE_DB.CURATED.dim_stations ds
              ON urm.mapped_zone = ds.city_zone
             AND ds.station_id   = start_station_id
             AND ds.is_current   = TRUE
            WHERE urm.snowflake_user = CURRENT_USER()
              AND urm.role_type = 'OPS'
        )
    ELSE FALSE
END;

ALTER TABLE CITYRIDE_DB.CURATED.fact_rentals
    ADD ROW ACCESS POLICY rap_rentals_by_zone ON (start_station_id);
```

### 6.2 Row Access Policy — dim_stations (by city_zone)

```sql
CREATE OR REPLACE ROW ACCESS POLICY rap_stations_by_zone
AS (city_zone VARCHAR) RETURNS BOOLEAN ->
CASE
    WHEN IS_ROLE_IN_SESSION('MANAGER_ROLE') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('ANALYST_ROLE') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('OPS_ROLE') THEN
        EXISTS (
            SELECT 1 FROM CITYRIDE_DB.SECURITY.user_role_mapping
            WHERE snowflake_user = CURRENT_USER()
              AND role_type      = 'OPS'
              AND mapped_zone    = city_zone
        )
    ELSE FALSE
END;

ALTER TABLE CITYRIDE_DB.CURATED.dim_stations
    ADD ROW ACCESS POLICY rap_stations_by_zone ON (city_zone);
```

### 6.3 Row Access Policy — dim_users (by region)

```sql
CREATE OR REPLACE ROW ACCESS POLICY rap_users_by_region
AS (region VARCHAR) RETURNS BOOLEAN ->
CASE
    WHEN IS_ROLE_IN_SESSION('MANAGER_ROLE') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('ANALYST_ROLE') THEN TRUE
    WHEN IS_ROLE_IN_SESSION('OPS_ROLE') THEN
        EXISTS (
            SELECT 1 FROM CITYRIDE_DB.SECURITY.user_role_mapping
            WHERE snowflake_user = CURRENT_USER()
              AND role_type      = 'OPS'
              AND mapped_region  = region
        )
    ELSE FALSE
END;

ALTER TABLE CITYRIDE_DB.CURATED.dim_users
    ADD ROW ACCESS POLICY rap_users_by_region ON (region);
```

---

## 7. Implementation Order

```
Step 1: CREATE ROLES (MANAGER_ROLE, ANALYST_ROLE, OPS_ROLE)
          ↓
Step 2: CREATE user_role_mapping table + populate
          ↓
Step 3: GRANT database/schema/warehouse usage to each role
          ↓
Step 4: GRANT SELECT on CURATED + ANALYTICS objects to each role
          ↓
Step 5: CREATE MASKING POLICIES (email, phone, GPS)
          ↓
Step 6: APPLY masking policies to columns (ALTER TABLE ... SET MASKING POLICY)
          ↓
Step 7: CREATE ROW ACCESS POLICIES (fact_rentals, dim_stations, dim_users)
          ↓
Step 8: APPLY row access policies to tables (ALTER TABLE ... ADD ROW ACCESS POLICY)
          ↓
Step 9: CREATE USERS + assign roles
          ↓
Step 10: TEST — login as each role, verify row filtering + masking
```

---

## 8. Constraints and Notes

- **One masking policy per column** — cannot stack multiple policies on the same column.
- **One row access policy per table** — the policy function must handle all roles internally.
- **Row access policies are transparent** — users see fewer rows, not an error.
- **Masking policies are transparent** — users see masked values as if that is real data.
- **ACCOUNTADMIN bypass** — ACCOUNTADMIN is NOT automatically exempt; handle it explicitly.
- **Policy ownership** — masking and row access policies are owned by SYSADMIN or a dedicated SECURITY_ADMIN role.
- **GPS precision** — ANALYST sees city-level precision only (2 decimal places ≈ 1.1 km accuracy). OPS sees `0.000000,0.000000` to prevent location tracking.

---

## 9. Security Demo

```sql
-- Same query, 3 different results:

USE ROLE MANAGER_ROLE;
SELECT user_id, email, phone, start_gps, start_city_zone
FROM CITYRIDE_DB.CURATED.v_rentals_full LIMIT 5;
-- → Full data: rider@example.com, +91 98765 43210, 12.971599,77.594566 — all zones

USE ROLE ANALYST_ROLE;
SELECT user_id, email, phone, start_gps, start_city_zone
FROM CITYRIDE_DB.CURATED.v_rentals_full LIMIT 5;
-- → Masked: r****@example.com, ****3210, 12.97,77.59 — all zones still visible

USE ROLE OPS_ROLE;
SELECT user_id, email, phone, start_gps, start_city_zone
FROM CITYRIDE_DB.CURATED.v_rentals_full LIMIT 5;
-- → Fully masked AND row-filtered: only rows in assigned zone, 0.000000,0.000000 for GPS
```

---

## 10. Testing Checklist

| Test | Login As | Expected Result |
|---|---|---|
| Manager sees all stations | MANAGER_USER1 | All zones in dim_stations |
| OPS sees only their zone | OPS_USER_NORTH | Only North-Zone stations |
| Analyst sees all rentals | ANALYST_USER1 | All rows in fact_rentals |
| OPS sees only their zone rentals | OPS_USER_NORTH | Only rentals starting in North-Zone |
| Manager sees full email | MANAGER_USER1 | `rider@example.com` |
| Analyst sees partial email | ANALYST_USER1 | `r****@example.com` |
| OPS sees masked email | OPS_USER_NORTH | `****` |
| Manager sees full GPS | MANAGER_USER1 | `12.971599,77.594566` |
| Analyst sees truncated GPS | ANALYST_USER1 | `12.97,77.59` |
| OPS sees zeroed GPS | OPS_USER_NORTH | `0.000000,0.000000` |
| KPI counts same across manager/analyst | Both | Same totals despite masking |
