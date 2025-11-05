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
-- - Validates CDC behavior for snapshot-based change tracking
--
-- USE CASES:
-- - Development: Verify CDC is capturing changes correctly
-- - Debugging: Investigate data quality issues
-- - Training: Understand SCD Type 1 vs Type 2 differences
-- - Compliance: Audit trail verification for LGPD/GDPR
--
-- LEARNING OBJECTIVES:
-- - Understand how CDC tracks changes over time in batch mode
-- - Compare SCD Type 1 (current) vs Type 2 (history)
-- - Analyze field-level changes using LAG() window functions
-- - Validate snapshot consistency with start_date timestamps
--
-- IMPORTANT NOTE ON SIMPLIFIED PATTERN:
-- This implementation uses a simplified declarative pattern:
-- - Type 1 (silver_users_unified): Overwrites with latest snapshot
-- - Type 2 (silver_users_history): Appends every snapshot with start_date timestamp
-- - is_current column is always TRUE (simplified approach)
-- - To find actual changes, compare snapshots by start_date

-- ============================================================================
-- QUERY 1: Find Users with Multiple Snapshots
-- ============================================================================
-- Shows which users appear in multiple snapshots (change frequency)
-- Use Case: Identify data quality issues or high-activity users

SELECT
  cpf,
  COUNT(*) AS snapshot_count,
  MIN(start_date) AS first_seen,
  MAX(start_date) AS last_updated,
  DATEDIFF(day, MIN(start_date), MAX(start_date)) AS days_tracked
FROM onewaysolution.batch.silver_users_history
GROUP BY cpf
HAVING COUNT(*) > 1  -- Users appearing in multiple snapshots
ORDER BY snapshot_count DESC
LIMIT 20;

-- ============================================================================
-- QUERY 2: Recent Snapshots (Last 7 Days)
-- ============================================================================
-- Shows snapshots captured recently
-- Use Case: Monitor daily pipeline operations

SELECT
  DATE(start_date) AS snapshot_date,
  COUNT(DISTINCT cpf) AS unique_users,
  COUNT(*) AS total_records,
  MIN(start_date) AS earliest_record,
  MAX(start_date) AS latest_record
FROM onewaysolution.batch.silver_users_history
WHERE start_date >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY DATE(start_date)
ORDER BY snapshot_date DESC;

-- ============================================================================
-- QUERY 3: Field-Level Change Detection (Actual Changes)
-- ============================================================================
-- Shows EXACTLY what changed between snapshots for specific users
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
    start_date,

    -- Compare with previous snapshot
    LAG(email) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_email,
    LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_address,
    LAG(city) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_city,
    LAG(first_name) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_first_name,
    LAG(last_name) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_last_name,
    LAG(job) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_job,
    LAG(company_name) OVER (PARTITION BY cpf ORDER BY start_date) AS prev_company

  FROM onewaysolution.batch.silver_users_history
  WHERE cpf IS NOT NULL
)

SELECT
  cpf,
  start_date AS change_timestamp,

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
  END AS company_change

FROM user_versions
WHERE prev_email IS NOT NULL  -- Exclude first snapshot (no previous to compare)
  AND (
    email != prev_email OR
    delivery_address != prev_address OR
    city != prev_city OR
    first_name != prev_first_name OR
    last_name != prev_last_name OR
    job != prev_job OR
    company_name != prev_company
  )
ORDER BY cpf, start_date DESC
LIMIT 100;

-- ============================================================================
-- QUERY 4: SCD Type 1 vs Type 2 Comparison
-- ============================================================================
-- Compares current state (Type 1) with latest snapshot in Type 2
-- Use Case: Validate that Type 1 and Type 2 are in sync

WITH latest_type2 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY start_date DESC) AS rn
  FROM onewaysolution.batch.silver_users_history
)

SELECT
  t1.cpf,
  t1.email AS type1_email,
  t2.email AS type2_email_latest,
  t1.city AS type1_city,
  t2.city AS type2_city_latest,
  t1.job AS type1_job,
  t2.job AS type2_job_latest,

  -- Validation flags
  CASE WHEN t1.email = t2.email THEN '✓ Match' ELSE '✗ MISMATCH' END AS email_match,
  CASE WHEN t1.city = t2.city THEN '✓ Match' ELSE '✗ MISMATCH' END AS city_match,
  CASE WHEN t1.job = t2.job THEN '✓ Match' ELSE '✗ MISMATCH' END AS job_match,

  t1.updated_at AS type1_timestamp,
  t2.start_date AS type2_timestamp

FROM onewaysolution.batch.silver_users_unified AS t1
INNER JOIN latest_type2 AS t2
  ON t1.cpf = t2.cpf
  AND t2.rn = 1  -- Latest snapshot from Type 2
WHERE
  t1.email != t2.email OR
  t1.city != t2.city OR
  t1.job != t2.job
LIMIT 100;

-- ============================================================================
-- QUERY 5: Record Counts Validation
-- ============================================================================
-- Count validation: Type 1 should match Type 2 latest snapshot count
-- Use Case: Data quality monitoring

SELECT
  'SCD Type 1 (Current State)' AS table_name,
  COUNT(DISTINCT cpf) AS user_count,
  MAX(updated_at) AS last_refresh
FROM onewaysolution.batch.silver_users_unified

UNION ALL

SELECT
  'SCD Type 2 (Latest Snapshot)' AS table_name,
  COUNT(DISTINCT cpf) AS user_count,
  MAX(start_date) AS last_refresh
FROM onewaysolution.batch.silver_users_history
WHERE start_date = (SELECT MAX(start_date) FROM onewaysolution.batch.silver_users_history)

UNION ALL

SELECT
  'SCD Type 2 (All Snapshots)' AS table_name,
  COUNT(*) AS user_count,
  MAX(start_date) AS last_refresh
FROM onewaysolution.batch.silver_users_history;

-- ============================================================================
-- QUERY 6: Email Change Audit Trail (LGPD/GDPR Compliance)
-- ============================================================================
-- Tracks all email changes for a specific user
-- Use Case: Respond to data subject access requests (DSAR)
-- USAGE: Replace 'USER_CPF_HERE' with actual CPF

-- Example (uncomment and replace CPF):
-- SELECT
--   cpf,
--   email,
--   start_date AS snapshot_timestamp,
--   LAG(email) OVER (PARTITION BY cpf ORDER BY start_date) AS previous_email,
--   CASE
--     WHEN LAG(email) OVER (PARTITION BY cpf ORDER BY start_date) IS NULL THEN 'Initial Record'
--     WHEN email != LAG(email) OVER (PARTITION BY cpf ORDER BY start_date) THEN 'Changed'
--     ELSE 'Unchanged'
--   END AS change_status
-- FROM onewaysolution.batch.silver_users_history
-- WHERE cpf = 'USER_CPF_HERE'
-- ORDER BY start_date DESC;

-- ============================================================================
-- QUERY 7: Compare Current vs Previous Snapshot (What Changed Today?)
-- ============================================================================
-- Compares the two most recent snapshots to find actual changes
-- Use Case: Daily change summary report

WITH current_snapshot AS (
  SELECT *
  FROM onewaysolution.batch.silver_users_history
  WHERE start_date = (SELECT MAX(start_date) FROM onewaysolution.batch.silver_users_history)
),
previous_snapshot AS (
  SELECT *
  FROM onewaysolution.batch.silver_users_history
  WHERE start_date = (
    SELECT MAX(start_date)
    FROM onewaysolution.batch.silver_users_history
    WHERE start_date < (SELECT MAX(start_date) FROM onewaysolution.batch.silver_users_history)
  )
)

SELECT
  c.cpf,
  CASE
    WHEN p.cpf IS NULL THEN 'NEW USER'
    WHEN c.email != p.email THEN 'EMAIL CHANGED'
    WHEN c.city != p.city THEN 'CITY CHANGED'
    WHEN c.phone_number != p.phone_number THEN 'PHONE CHANGED'
    WHEN c.delivery_address != p.delivery_address THEN 'ADDRESS CHANGED'
    WHEN c.job != p.job THEN 'JOB CHANGED'
    ELSE 'NO CHANGE'
  END AS change_type,
  p.email AS old_email,
  c.email AS new_email,
  p.city AS old_city,
  c.city AS new_city,
  p.start_date AS previous_snapshot_date,
  c.start_date AS current_snapshot_date
FROM current_snapshot c
LEFT JOIN previous_snapshot p ON c.cpf = p.cpf
WHERE
  p.cpf IS NULL OR  -- New user
  c.email != p.email OR
  c.city != p.city OR
  c.phone_number != p.phone_number OR
  c.delivery_address != p.delivery_address OR
  c.job != p.job
ORDER BY change_type, c.cpf
LIMIT 100;

-- ============================================================================
-- QUERY 8: Most Changed Fields (Data Quality Insights)
-- ============================================================================
-- Identifies which fields change most frequently across all users
-- Use Case: Understand data volatility and stability

WITH field_changes AS (
  SELECT
    cpf,
    CASE WHEN email != LAG(email) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS email_changed,
    CASE WHEN delivery_address != LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS address_changed,
    CASE WHEN city != LAG(city) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS city_changed,
    CASE WHEN first_name != LAG(first_name) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS firstname_changed,
    CASE WHEN last_name != LAG(last_name) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS lastname_changed,
    CASE WHEN job != LAG(job) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS job_changed,
    CASE WHEN company_name != LAG(company_name) OVER (PARTITION BY cpf ORDER BY start_date) THEN 1 ELSE 0 END AS company_changed
  FROM onewaysolution.batch.silver_users_history
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
-- QUERY 9: Snapshot Timeline Analysis
-- ============================================================================
-- Shows when snapshots were captured (hourly/daily patterns)
-- Use Case: Validate pipeline refresh schedules

SELECT
  DATE(start_date) AS snapshot_date,
  COUNT(DISTINCT start_date) AS snapshot_count,
  MIN(start_date) AS earliest_snapshot,
  MAX(start_date) AS latest_snapshot,
  COUNT(DISTINCT cpf) AS unique_users,
  COUNT(*) AS total_records
FROM onewaysolution.batch.silver_users_history
WHERE start_date >= CURRENT_DATE() - INTERVAL 30 DAYS
GROUP BY DATE(start_date)
ORDER BY snapshot_date DESC;

-- ============================================================================
-- QUERY 10: Users with Most Versions (High Activity)
-- ============================================================================
-- Identifies users who appear in many snapshots
-- Use Case: Fraud detection or data quality analysis

WITH user_stats AS (
  SELECT
    cpf,
    COUNT(*) AS version_count,
    MIN(start_date) AS first_seen,
    MAX(start_date) AS last_seen,
    COUNT(DISTINCT email) AS distinct_emails,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT(DISTINCT job) AS distinct_jobs
  FROM onewaysolution.batch.silver_users_history
  GROUP BY cpf
)

SELECT
  cpf,
  version_count AS snapshot_appearances,
  first_seen,
  last_seen,
  DATEDIFF(day, first_seen, last_seen) AS days_active,
  distinct_emails AS email_variations,
  distinct_cities AS city_variations,
  distinct_jobs AS job_variations,
  CASE
    WHEN distinct_emails > 3 THEN '⚠️ High email changes'
    WHEN distinct_cities > 3 THEN '⚠️ High city changes'
    ELSE '✓ Normal'
  END AS activity_flag
FROM user_stats
WHERE version_count > 1
ORDER BY version_count DESC, distinct_emails DESC
LIMIT 50;

-- ============================================================================
-- PRODUCTION NOTES
-- ============================================================================

-- UNDERSTANDING THE SIMPLIFIED PATTERN:
--
-- Unlike apply_changes() which tracks individual INSERT/UPDATE/DELETE events,
-- our simplified pattern works with snapshots:
--
-- 1. Type 1 (silver_users_unified):
--    - Stores ONLY latest state
--    - Overwrites on each run
--    - Query for current operational data
--
-- 2. Type 2 (silver_users_history):
--    - Stores ALL snapshots
--    - Appends on each run (even if no changes!)
--    - Query to compare snapshots and find actual changes
--
-- IMPORTANT TRADE-OFF:
-- Type 2 grows with EVERY snapshot, not just changes.
-- To find actual changes, use QUERY 7 to compare consecutive snapshots.
--
-- OPTIMIZATION FOR LARGE DATASETS:
-- If Type 2 grows too large, consider:
-- 1. Only append records where hash changed (see TRACKING_UPDATES.md)
-- 2. Use Change Data Feed from Type 1 table
-- 3. Migrate to streaming CDC pattern (stream-cdc folder)

-- VALIDATION CHECKLIST:
-- ✅ Run QUERY 4: Type 1 and Type 2 should match for latest snapshot
-- ✅ Run QUERY 5: Record counts should be consistent
-- ✅ Run QUERY 7: Identify actual changes between snapshots
-- ✅ Run QUERY 8: Monitor which fields change frequently
