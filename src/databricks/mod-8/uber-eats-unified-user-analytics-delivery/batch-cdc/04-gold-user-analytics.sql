-- GOLD LAYER - Unified User Analytics (Single Comprehensive View)
--
-- PURPOSE:
-- This module creates a single, comprehensive analytics table combining:
-- - Marketing segmentation (city, age groups)
-- - Business intelligence (demographics, data quality)
-- - Compliance metrics (change audit summary)
--
-- PATTERN: MATERIALIZED VIEW (End-to-End Batch CDC)
--
-- WHY ONE GOLD TABLE?
-- Simplifies the pipeline while providing all key metrics:
-- - Marketing: Segment sizes, age distributions
-- - BI: Data quality, profile completeness
-- - Compliance: Change frequency indicators
--
-- DATA FLOW:
--   silver_users_unified (SCD Type 1 - current state) + silver_users_history (SCD Type 2)
--     → Aggregations and metrics
--     → gold_user_analytics (single comprehensive view)

CREATE OR REFRESH MATERIALIZED VIEW gold_user_analytics
COMMENT 'Comprehensive user analytics combining marketing, BI, and compliance metrics'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'use_case' = 'marketing_bi_compliance',
  'refresh_schedule' = 'hourly',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
WITH user_demographics AS (
  -- Calculate user demographics from current state (SCD Type 1)
  SELECT
    cpf,
    city,
    email,
    first_name,
    last_name,
    birthday,
    job,
    delivery_address,

    -- Age calculation
    CASE
      WHEN birthday IS NULL THEN NULL
      ELSE FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25)
    END AS age,

    -- Age group segmentation
    CASE
      WHEN birthday IS NULL THEN 'Unknown'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 18 AND 25 THEN '18-25 (Young Adults)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 26 AND 35 THEN '26-35 (Millennials)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 36 AND 50 THEN '36-50 (Gen X)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) BETWEEN 51 AND 65 THEN '51-65 (Boomers)'
      WHEN FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25) > 65 THEN '65+ (Seniors)'
      ELSE 'Unknown'
    END AS age_group,

    -- Job categorization for B2B insights
    CASE
      WHEN job LIKE '%Executivo%' OR job LIKE '%Diretor%' OR job LIKE '%Gerente%' THEN 'Leadership'
      WHEN job LIKE '%Coordenador%' OR job LIKE '%Supervisor%' OR job LIKE '%Especialista%' THEN 'Management'
      WHEN job LIKE '%Analista%' OR job LIKE '%Consultor%' OR job LIKE '%Desenvolvedor%' THEN 'Professional'
      WHEN job LIKE '%Assistente%' OR job LIKE '%Associado%' OR job LIKE '%Junior%' THEN 'Entry-Level'
      WHEN job LIKE '%Designer%' OR job LIKE '%Arquiteto%' OR job LIKE '%Engenheiro%' THEN 'Technical'
      WHEN job IS NOT NULL THEN 'Other'
      ELSE 'Unknown'
    END AS job_category,

    -- Profile completeness indicator
    CASE
      WHEN email IS NOT NULL
           AND first_name IS NOT NULL
           AND last_name IS NOT NULL
           AND birthday IS NOT NULL
           AND delivery_address IS NOT NULL
           AND job IS NOT NULL
      THEN 1 ELSE 0
    END AS is_complete_profile

  FROM silver_users_unified
  WHERE cpf IS NOT NULL
),

change_metrics AS (
  -- Calculate change frequency from history (SCD Type 2)
  SELECT
    cpf,
    COUNT(*) - 1 AS total_changes,
    MAX(start_date) AS last_change_date,
    DATEDIFF(day, MAX(start_date), current_date()) AS days_since_last_change
  FROM silver_users_history
  WHERE cpf IS NOT NULL
  GROUP BY cpf
),

city_analytics AS (
  -- Aggregate by city with all metrics
  SELECT
    COALESCE(d.city, 'Unknown') AS city,

    -- Marketing: Segment size by age group
    d.age_group,
    COUNT(DISTINCT d.cpf) AS user_count,
    ROUND(AVG(d.age), 1) AS avg_age,

    -- BI: Data quality metrics
    COUNT(DISTINCT CASE WHEN d.email IS NOT NULL THEN d.cpf END) AS users_with_email,
    COUNT(DISTINCT CASE WHEN d.first_name IS NOT NULL THEN d.cpf END) AS users_with_name,
    COUNT(DISTINCT CASE WHEN d.birthday IS NOT NULL THEN d.cpf END) AS users_with_birthday,
    COUNT(DISTINCT CASE WHEN d.job IS NOT NULL THEN d.cpf END) AS users_with_job,
    COUNT(DISTINCT CASE WHEN d.delivery_address IS NOT NULL THEN d.cpf END) AS users_with_address,
    SUM(d.is_complete_profile) AS complete_profiles,

    -- BI: Profile completeness percentage
    ROUND(
      (COUNT(DISTINCT CASE WHEN d.email IS NOT NULL THEN d.cpf END) +
       COUNT(DISTINCT CASE WHEN d.first_name IS NOT NULL THEN d.cpf END) +
       COUNT(DISTINCT CASE WHEN d.birthday IS NOT NULL THEN d.cpf END) +
       COUNT(DISTINCT CASE WHEN d.job IS NOT NULL THEN d.cpf END) +
       COUNT(DISTINCT CASE WHEN d.delivery_address IS NOT NULL THEN d.cpf END)) * 100.0
      / (COUNT(DISTINCT d.cpf) * 5), 1
    ) AS profile_completeness_pct,

    -- BI: Top job category
    MODE(d.job_category) AS top_job_category,

    -- Compliance: Change activity summary
    ROUND(AVG(COALESCE(c.total_changes, 0)), 1) AS avg_changes_per_user,
    SUM(CASE WHEN COALESCE(c.total_changes, 0) > 5 THEN 1 ELSE 0 END) AS high_activity_users,
    MAX(c.last_change_date) AS most_recent_change

  FROM user_demographics d
  LEFT JOIN change_metrics c ON d.cpf = c.cpf
  GROUP BY
    COALESCE(d.city, 'Unknown'),
    d.age_group
)

SELECT
  city,
  age_group,

  -- Marketing metrics
  user_count,
  avg_age,
  users_with_email,
  users_with_name,

  -- BI metrics
  users_with_birthday,
  users_with_job,
  users_with_address,
  complete_profiles,
  profile_completeness_pct,
  top_job_category,

  -- Data quality grade
  CASE
    WHEN profile_completeness_pct >= 90 THEN 'Excellent'
    WHEN profile_completeness_pct >= 75 THEN 'Good'
    WHEN profile_completeness_pct >= 50 THEN 'Fair'
    ELSE 'Poor'
  END AS data_quality_grade,

  -- Compliance metrics
  avg_changes_per_user,
  high_activity_users,
  most_recent_change,

  -- Metadata
  current_timestamp() AS computed_at

FROM city_analytics
ORDER BY user_count DESC;

-- BUSINESS VALUE:
--
-- Marketing Team:
-- - user_count by city and age_group for campaign targeting
-- - users_with_email for direct marketing reach
-- - Segment-specific insights
--
-- Business Intelligence:
-- - profile_completeness_pct for data quality monitoring
-- - data_quality_grade for executive dashboards
-- - top_job_category for B2B opportunities
--
-- Compliance Team:
-- - avg_changes_per_user for audit patterns
-- - high_activity_users for fraud detection
-- - most_recent_change for LGPD/GDPR tracking
--
-- Operations:
-- - Single table instead of multiple (simplified queries)
-- - Hourly refresh aligns with Bronze refresh
-- - MATERIALIZED VIEW pattern (efficient batch processing)

-- QUERY EXAMPLES:
--
-- 1. Marketing campaign in São Paulo for millennials:
-- SELECT * FROM gold_user_analytics
-- WHERE city = 'São Paulo' AND age_group = '26-35 (Millennials)';
--
-- 2. Data quality monitoring:
-- SELECT city, SUM(user_count) AS total_users, AVG(profile_completeness_pct) AS avg_quality
-- FROM gold_user_analytics
-- GROUP BY city
-- HAVING AVG(profile_completeness_pct) < 75;
--
-- 3. Compliance audit summary:
-- SELECT city, SUM(high_activity_users) AS suspicious_users
-- FROM gold_user_analytics
-- GROUP BY city
-- ORDER BY suspicious_users DESC;
