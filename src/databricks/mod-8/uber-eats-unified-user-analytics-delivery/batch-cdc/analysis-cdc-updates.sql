-- ANALYSIS - CDC Updates Inspection and Validation
--
-- PURPOSE:
-- This module provides queries to inspect and understand CDC operations in the pipeline.
-- Use these queries to verify that SCD Type 1 and Type 2 are working correctly.
--
-- WHAT IT DOES:
-- - Identifies which users have been updated (multiple versions in history)
-- - Compares current state (Type 1) vs historical versions (Type 2)
-- - Shows exactly what fields changed and when
-- - Validates CDC behavior for INSERT, UPDATE, DELETE operations
--
-- USE CASES:
-- - Development: Verify CDC is capturing changes correctly
-- - Debugging: Investigate data quality issues
-- - Training: Understand SCD Type 1 vs Type 2 differences
-- - Compliance: Audit trail verification for LGPD/GDPR
--
-- LEARNING OBJECTIVES:
-- - Understand how CDC tracks changes over time
-- - Compare SCD Type 1 (current) vs Type 2 (history)
-- - Analyze field-level changes using LAG() window functions
-- - Validate temporal consistency with __START_AT and __END_AT
--
-- IMPORTANT NOTE ON __CURRENT COLUMN:
-- SCD Type 2 uses __END_AT column to track current records:
-- - __END_AT IS NULL → Current/active version
-- - __END_AT IS NOT NULL → Historical/superseded version
-- Some Databricks versions may add __CURRENT boolean, but __END_AT is the standard approach.

-- ============================================================================
-- QUERY 1: Find Users with Multiple Updates (Change Frequency)
-- ============================================================================
-- Shows which users have changed most frequently
-- Use Case: Identify data quality issues or suspicious activity

SELECT
  cpf,
  COUNT(*) - 1 AS number_of_updates,
  MIN(__START_AT) AS first_seen,
  MAX(__START_AT) AS last_updated,
  DATEDIFF(day, MIN(__START_AT), MAX(__START_AT)) AS days_tracked,
  MAX(CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END) AS is_currently_active
FROM silver_users_history
GROUP BY cpf
HAVING COUNT(*) > 1  -- Only users with updates (multiple versions)
ORDER BY number_of_updates DESC
LIMIT 20;

-- ============================================================================
-- QUERY 2: Recent Updates (Last 7 Days)
-- ============================================================================
-- Shows users who were updated recently
-- Use Case: Monitor daily pipeline operations

SELECT
  cpf,
  email,
  city,
  __START_AT AS version_created_at,
  CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current_version,
  DATEDIFF(hour, __START_AT, current_timestamp()) AS hours_since_update
FROM silver_users_history
WHERE __START_AT >= current_date() - INTERVAL 7 DAYS
ORDER BY __START_AT DESC;

-- ============================================================================
-- QUERY 3: Field-Level Change Detection
-- ============================================================================
-- Shows EXACTLY what changed between versions for specific users
-- Use Case: Audit trail for compliance (LGPD/GDPR data access requests)

WITH user_versions AS (
  SELECT
    cpf,
    email,
    delivery_address,
    city,
    first_name,
    last_name,
    job,
    company_name,
    __START_AT,
    __END_AT,

    -- Compare with previous version
    LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_email,
    LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_address,
    LAG(city) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_city,
    LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_first_name,
    LAG(last_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_last_name,
    LAG(job) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_job,
    LAG(company_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_company

  FROM silver_users_history
  WHERE cpf IS NOT NULL
)

SELECT
  cpf,
  __START_AT AS change_timestamp,

  -- Email changes
  CASE
    WHEN email != prev_email OR (email IS NOT NULL AND prev_email IS NULL)
    THEN CONCAT('Email: "', COALESCE(prev_email, 'NULL'), '" → "', email, '"')
    ELSE NULL
  END AS email_change,

  -- Address changes
  CASE
    WHEN delivery_address != prev_address OR (delivery_address IS NOT NULL AND prev_address IS NULL)
    THEN CONCAT('Address: "', COALESCE(prev_address, 'NULL'), '" → "', delivery_address, '"')
    ELSE NULL
  END AS address_change,

  -- City changes
  CASE
    WHEN city != prev_city OR (city IS NOT NULL AND prev_city IS NULL)
    THEN CONCAT('City: "', COALESCE(prev_city, 'NULL'), '" → "', city, '"')
    ELSE NULL
  END AS city_change,

  -- Name changes
  CASE
    WHEN first_name != prev_first_name OR last_name != prev_last_name
         OR (first_name IS NOT NULL AND prev_first_name IS NULL)
         OR (last_name IS NOT NULL AND prev_last_name IS NULL)
    THEN CONCAT('Name: "', COALESCE(prev_first_name, 'NULL'), ' ', COALESCE(prev_last_name, 'NULL'),
                '" → "', first_name, ' ', last_name, '"')
    ELSE NULL
  END AS name_change,

  -- Job changes
  CASE
    WHEN job != prev_job OR (job IS NOT NULL AND prev_job IS NULL)
    THEN CONCAT('Job: "', COALESCE(prev_job, 'NULL'), '" → "', job, '"')
    ELSE NULL
  END AS job_change,

  -- Company changes
  CASE
    WHEN company_name != prev_company OR (company_name IS NOT NULL AND prev_company IS NULL)
    THEN CONCAT('Company: "', COALESCE(prev_company, 'NULL'), '" → "', company_name, '"')
    ELSE NULL
  END AS company_change,

  CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current_version

FROM user_versions
WHERE prev_email IS NOT NULL  -- Exclude first version (no previous to compare)
  AND (
    email != prev_email OR
    delivery_address != prev_address OR
    city != prev_city OR
    first_name != prev_first_name OR
    last_name != prev_last_name OR
    job != prev_job OR
    company_name != prev_company
  )
ORDER BY cpf, __START_AT DESC
LIMIT 100;

-- ============================================================================
-- QUERY 4: SCD Type 1 vs Type 2 Comparison
-- ============================================================================
-- Compares current state (Type 1) with current version (Type 2)
-- Use Case: Validate that Type 1 and Type 2 are in sync

SELECT
  t1.cpf,
  t1.email AS type1_email,
  t2.email AS type2_email_current,
  t1.city AS type1_city,
  t2.city AS type2_city_current,
  t1.job AS type1_job,
  t2.job AS type2_job_current,

  -- Validation flags
  CASE WHEN t1.email = t2.email THEN '✓' ELSE '✗ MISMATCH' END AS email_match,
  CASE WHEN t1.city = t2.city THEN '✓' ELSE '✗ MISMATCH' END AS city_match,
  CASE WHEN t1.job = t2.job THEN '✓' ELSE '✗ MISMATCH' END AS job_match

FROM silver_users_unified AS t1
INNER JOIN silver_users_history AS t2
  ON t1.cpf = t2.cpf
  AND t2.__END_AT IS NULL  -- Only current version from Type 2 (__END_AT NULL means active)
WHERE
  t1.email != t2.email OR
  t1.city != t2.city OR
  t1.job != t2.job
LIMIT 100;

-- ============================================================================
-- QUERY 5: Active Users in Type 1 vs Type 2
-- ============================================================================
-- Count validation: Type 1 should match Type 2 current records
-- Use Case: Data quality monitoring

SELECT
  'SCD Type 1 (Current State)' AS table_name,
  COUNT(DISTINCT cpf) AS user_count
FROM silver_users_unified

UNION ALL

SELECT
  'SCD Type 2 (Current Version)' AS table_name,
  COUNT(DISTINCT cpf) AS user_count
FROM silver_users_history
WHERE __END_AT IS NULL  -- Current version (__END_AT NULL means active)

UNION ALL

SELECT
  'SCD Type 2 (All Versions)' AS table_name,
  COUNT(*) AS user_count
FROM silver_users_history;

-- ============================================================================
-- QUERY 6: Email Change Audit Trail (LGPD/GDPR Compliance)
-- ============================================================================
-- Tracks all email changes for a specific user
-- Use Case: Respond to data subject access requests (DSAR)
-- USAGE: Replace 'USER_CPF_HERE' with actual CPF

-- SELECT
--   cpf,
--   email,
--   __START_AT AS version_start,
--   __END_AT AS version_end,
--   CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current,
--   COALESCE(
--     DATEDIFF(day, __START_AT, __END_AT),
--     DATEDIFF(day, __START_AT, current_timestamp())
--   ) AS days_active
-- FROM silver_users_history
-- WHERE cpf = 'USER_CPF_HERE'
-- ORDER BY __START_AT DESC;

-- ============================================================================
-- QUERY 7: Soft Deletes Detection (SCD Type 2)
-- ============================================================================
-- Finds users who were deleted (have __END_AT but __END_AT NULL for no versions)
-- Use Case: GDPR Right to be Forgotten compliance

SELECT
  cpf,
  MAX(email) AS last_known_email,
  MAX(__END_AT) AS deleted_at,
  DATEDIFF(day, MAX(__END_AT), current_timestamp()) AS days_since_deletion,
  COUNT(*) AS total_versions
FROM silver_users_history
WHERE cpf NOT IN (
  SELECT DISTINCT cpf
  FROM silver_users_history
  WHERE __END_AT IS NULL  -- Current version (__END_AT NULL means active)
)
GROUP BY cpf
ORDER BY deleted_at DESC
LIMIT 20;

-- ============================================================================
-- QUERY 8: Most Changed Fields (Data Quality Insights)
-- ============================================================================
-- Identifies which fields change most frequently across all users
-- Use Case: Understand data volatility and stability

WITH field_changes AS (
  SELECT
    cpf,
    CASE WHEN email != LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS email_changed,
    CASE WHEN delivery_address != LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS address_changed,
    CASE WHEN city != LAG(city) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS city_changed,
    CASE WHEN first_name != LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS firstname_changed,
    CASE WHEN last_name != LAG(last_name) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS lastname_changed,
    CASE WHEN job != LAG(job) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS job_changed,
    CASE WHEN company_name != LAG(company_name) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN 1 ELSE 0 END AS company_changed
  FROM silver_users_history
)

SELECT
  'email' AS field_name,
  SUM(email_changed) AS total_changes,
  ROUND(AVG(email_changed) * 100, 2) AS change_rate_pct
FROM field_changes

UNION ALL SELECT 'delivery_address', SUM(address_changed), ROUND(AVG(address_changed) * 100, 2) FROM field_changes
UNION ALL SELECT 'city', SUM(city_changed), ROUND(AVG(city_changed) * 100, 2) FROM field_changes
UNION ALL SELECT 'first_name', SUM(firstname_changed), ROUND(AVG(firstname_changed) * 100, 2) FROM field_changes
UNION ALL SELECT 'last_name', SUM(lastname_changed), ROUND(AVG(lastname_changed) * 100, 2) FROM field_changes
UNION ALL SELECT 'job', SUM(job_changed), ROUND(AVG(job_changed) * 100, 2) FROM field_changes
UNION ALL SELECT 'company_name', SUM(company_changed), ROUND(AVG(company_changed) * 100, 2) FROM field_changes

ORDER BY total_changes DESC;

-- ============================================================================
-- QUERY 9: Time-Based Update Patterns
-- ============================================================================
-- Shows when updates typically occur (hourly/daily patterns)
-- Use Case: Optimize pipeline refresh schedules

SELECT
  DATE_TRUNC('hour', __START_AT) AS update_hour,
  COUNT(*) AS updates_count,
  COUNT(DISTINCT cpf) AS unique_users_updated
FROM silver_users_history
WHERE __START_AT >= current_date() - INTERVAL 30 DAYS
GROUP BY DATE_TRUNC('hour', __START_AT)
ORDER BY update_hour DESC
LIMIT 100;

-- ============================================================================
-- QUERY 10: Out-of-Order Events Detection
-- ============================================================================
-- Validates that SEQUENCE BY is working correctly (events processed in order)
-- Use Case: Data quality validation for CDC ordering

SELECT
  cpf,
  __START_AT AS version_start,
  dt_current_timestamp AS source_timestamp,
  DATEDIFF(second, LAG(dt_current_timestamp) OVER (PARTITION BY cpf ORDER BY __START_AT), dt_current_timestamp) AS seconds_between_source_events,
  CASE
    WHEN dt_current_timestamp < LAG(dt_current_timestamp) OVER (PARTITION BY cpf ORDER BY __START_AT)
    THEN '⚠️ OUT OF ORDER'
    ELSE '✓ In Order'
  END AS ordering_status
FROM silver_users_history
WHERE cpf IN (
  SELECT cpf FROM silver_users_history GROUP BY cpf HAVING COUNT(*) > 1
)
ORDER BY cpf, __START_AT
LIMIT 100;
