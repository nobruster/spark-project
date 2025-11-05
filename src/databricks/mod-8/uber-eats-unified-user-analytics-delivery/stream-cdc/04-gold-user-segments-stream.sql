-- GOLD LAYER - User Segmentation for Real-time Marketing (Streaming Live Table)
--
-- PURPOSE:
-- Creates business-ready user segments with real-time updates for marketing campaigns.
-- Uses LIVE TABLE pattern (not STREAMING TABLE) to read from streaming CDC source.
--
-- PATTERN: Streaming Source → Live Table (Incremental Processing)
-- Unlike batch, this updates incrementally as CDC events arrive.
--
-- KEY CONCEPT: LIVE TABLE vs STREAMING TABLE vs MATERIALIZED VIEW
-- - MATERIALIZED VIEW: Complete recomputation on refresh (batch)
-- - STREAMING TABLE: Continuous micro-batch processing (complex aggregations)
-- - LIVE TABLE: Incremental updates from streaming/batch source (simpler)
--
-- For Gold aggregations reading from streaming CDC, LIVE TABLE is recommended.

CREATE OR REFRESH LIVE TABLE gold_user_segments_stream
COMMENT 'Real-time user segmentation by city and age group for marketing campaigns'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'use_case' = 'marketing_segmentation',
  'pattern' = 'streaming_cdc_aggregation',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
WITH user_demographics AS (
  SELECT
    cpf,
    city,
    CASE
      WHEN birthday IS NULL THEN NULL
      ELSE FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25)
    END AS age,
    CASE
      WHEN birthday IS NULL THEN 'Unknown'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 18 AND 25 THEN '18-25 (Young Adults)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 26 AND 35 THEN '26-35 (Millennials)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 36 AND 50 THEN '36-50 (Gen X)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 51 AND 65 THEN '51-65 (Boomers)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) > 65 THEN '65+ (Seniors)'
      ELSE 'Unknown'
    END AS age_group,
    email,
    first_name,
    last_name
  FROM LIVE.silver_users_unified_stream  -- Read from streaming SCD Type 1
  WHERE cpf IS NOT NULL
)

SELECT
  COALESCE(city, 'Unknown') AS city,
  age_group,
  COUNT(DISTINCT cpf) AS user_count,
  ROUND(AVG(age), 1) AS avg_age,
  COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) AS users_with_email,
  COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) AS users_with_name,
  current_timestamp() AS computed_at
FROM user_demographics
GROUP BY
  COALESCE(city, 'Unknown'),
  age_group
ORDER BY
  user_count DESC;

-- STREAMING PATTERN NOTES:
--
-- LIVE TABLE reads from silver_users_unified_stream (streaming CDC)
-- and incrementally updates aggregations as CDC events arrive.
--
-- How it works:
-- 1. CDC event arrives: User cpf=123 moves from "São Paulo" to "Rio"
-- 2. LIVE TABLE processes delta:
--    - Decrements São Paulo segment count
--    - Increments Rio segment count
-- 3. Aggregations stay up-to-date in real-time
--
-- Benefits vs MATERIALIZED VIEW:
-- - Incremental updates (not full recomputation)
-- - Real-time freshness (seconds, not hours)
-- - More efficient for large datasets
--
-- Trade-offs:
-- - Slightly more complex than batch
-- - Requires streaming CDC source
-- - Higher continuous compute cost
--
-- BUSINESS VALUE (Real-time):
-- - Instant campaign targeting updates
-- - Real-time segment size monitoring
-- - Dynamic resource allocation by city
-- - Immediate market expansion insights
