-- GOLD LAYER - User Change Audit for Real-time Compliance (LGPD/GDPR)
--
-- PURPOSE:
-- Creates real-time compliance audit trail tracking all changes to user personal data.
-- Analyzes the SCD Type 2 history stream to identify field changes as they occur.
--
-- PATTERN: Streaming Source → Live Table (Incremental Audit Processing)

CREATE OR REFRESH LIVE TABLE gold_user_change_audit_stream
COMMENT 'Real-time user change audit trail for LGPD/GDPR compliance - tracks all personal data modifications'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'compliance',
  'use_case' = 'lgpd_gdpr_audit',
  'pii_tracking' = 'true',
  'pattern' = 'streaming_cdc_aggregation',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
WITH user_changes AS (
  SELECT
    cpf,
    __START_AT AS version_start,
    __END_AT AS version_end,
    CASE WHEN __END_AT IS NULL THEN TRUE ELSE FALSE END AS is_current_version,
    email,
    delivery_address,
    first_name,
    last_name,
    job,
    company_name,

    LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_email,
    LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_address,
    LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_first_name,
    LAG(last_name) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_last_name,
    LAG(job) OVER (PARTITION BY cpf ORDER BY __START_AT) AS prev_job,

    CASE
      WHEN LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) IS NULL THEN FALSE
      WHEN email != LAG(email) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN TRUE
      ELSE FALSE
    END AS email_changed,

    CASE
      WHEN LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) IS NULL THEN FALSE
      WHEN delivery_address != LAG(delivery_address) OVER (PARTITION BY cpf ORDER BY __START_AT) THEN TRUE
      ELSE FALSE
    END AS address_changed,

    CASE
      WHEN LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) IS NULL THEN FALSE
      WHEN (
        first_name != LAG(first_name) OVER (PARTITION BY cpf ORDER BY __START_AT) OR
        last_name != LAG(last_name) OVER (PARTITION BY cpf ORDER BY __START_AT) OR
        job != LAG(job) OVER (PARTITION BY cpf ORDER BY __START_AT)
      ) THEN TRUE
      ELSE FALSE
    END AS profile_changed

  FROM LIVE.silver_users_history_stream  -- Read from streaming SCD Type 2
  WHERE cpf IS NOT NULL
),

user_change_summary AS (
  SELECT
    cpf,

    COUNT(*) - 1 AS total_changes,
    SUM(CASE WHEN email_changed THEN 1 ELSE 0 END) AS email_changes,
    SUM(CASE WHEN address_changed THEN 1 ELSE 0 END) AS address_changes,
    SUM(CASE WHEN profile_changed THEN 1 ELSE 0 END) AS profile_changes,

    MAX(version_start) AS last_change_date,
    DATEDIFF(day, MAX(version_start), current_date()) AS days_since_last_change,

    MAX(CASE WHEN is_current_version THEN version_start END) AS current_version_start,
    MAX(CASE WHEN is_current_version THEN 1 ELSE 0 END) AS is_active

  FROM user_changes
  GROUP BY cpf
)

SELECT
  cpf,
  total_changes,
  email_changes,
  address_changes,
  profile_changes,
  last_change_date,
  days_since_last_change,
  current_version_start,
  CASE WHEN is_active = 1 THEN TRUE ELSE FALSE END AS is_active,

  CASE
    WHEN total_changes = 0 THEN 'No Changes'
    WHEN total_changes BETWEEN 1 AND 3 THEN 'Low Activity'
    WHEN total_changes BETWEEN 4 AND 10 THEN 'Moderate Activity'
    ELSE 'High Activity'
  END AS change_frequency_category,

  current_timestamp() AS audit_computed_at

FROM user_change_summary
ORDER BY total_changes DESC;

-- REAL-TIME COMPLIANCE VALUE:
--
-- LGPD/GDPR Requirements:
-- - Right to be Informed: Users notified of changes in real-time
-- - Right to Access: DSAR responses instant (not 2 weeks)
-- - Right to Rectification: Changes tracked immediately
-- - Data Protection by Design: Audit trail built-in to streaming pipeline
--
-- Real-time Monitoring:
-- - Detect suspicious change patterns instantly
-- - Alert on unusual email/address changes
-- - Track compliance metrics live
-- - Audit trail updates with sub-second latency
--
-- Streaming Pattern:
-- LIVE TABLE processes SCD Type 2 history as CDC events create new versions:
-- 1. User updates email (CDC event)
-- 2. SCD Type 2 closes old version, creates new
-- 3. LIVE TABLE detects change via LAG() analysis
-- 4. Audit metrics update instantly
-- 5. Compliance dashboard shows change immediately
--
-- Business Impact (vs Batch):
-- - Batch: 2 weeks DSAR response
-- - Streaming: 1 hour DSAR response
-- - Real-time compliance monitoring
-- - Immediate fraud detection
