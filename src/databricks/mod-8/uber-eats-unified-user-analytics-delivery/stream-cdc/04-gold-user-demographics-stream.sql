-- GOLD LAYER - User Demographics for Real-time Business Intelligence
--
-- PURPOSE:
-- Creates demographic insights for real-time BI dashboards and strategic planning.
-- Aggregates user profiles by multiple dimensions with continuous updates.
--
-- PATTERN: Streaming Source → Live Table (Incremental Aggregations)

CREATE OR REFRESH LIVE TABLE gold_user_demographics_stream
COMMENT 'Real-time user demographic analysis for business intelligence and strategic planning'
TBLPROPERTIES (
  'quality' = 'gold',
  'layer' = 'analytics',
  'use_case' = 'business_intelligence',
  'pattern' = 'streaming_cdc_aggregation',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
WITH user_metrics AS (
  SELECT
    cpf,
    city,
    email,
    birthday,
    job,
    first_name,
    last_name,
    delivery_address,

    CASE
      WHEN birthday IS NULL THEN NULL
      ELSE FLOOR(DATEDIFF(day, birthday, current_date()) / 365.25)
    END AS age,

    CASE
      WHEN job LIKE '%Executivo%' OR job LIKE '%Diretor%' OR job LIKE '%Gerente%' THEN 'Leadership'
      WHEN job LIKE '%Coordenador%' OR job LIKE '%Supervisor%' OR job LIKE '%Especialista%' THEN 'Management'
      WHEN job LIKE '%Analista%' OR job LIKE '%Consultor%' OR job LIKE '%Desenvolvedor%' THEN 'Professional'
      WHEN job LIKE '%Assistente%' OR job LIKE '%Associado%' OR job LIKE '%Junior%' THEN 'Entry-Level'
      WHEN job LIKE '%Designer%' OR job LIKE '%Arquiteto%' OR job LIKE '%Engenheiro%' THEN 'Technical'
      WHEN job IS NOT NULL THEN 'Other'
      ELSE 'Unknown'
    END AS job_category,

    CASE
      WHEN email IS NOT NULL
           AND first_name IS NOT NULL
           AND last_name IS NOT NULL
           AND birthday IS NOT NULL
           AND delivery_address IS NOT NULL
           AND job IS NOT NULL
      THEN 1 ELSE 0
    END AS is_complete_profile

  FROM LIVE.silver_users_unified_stream  -- Read from streaming SCD Type 1
  WHERE cpf IS NOT NULL
),

city_demographics AS (
  SELECT
    COALESCE(city, 'Unknown') AS city,

    COUNT(DISTINCT cpf) AS total_users,
    COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) AS users_with_email,
    COUNT(DISTINCT CASE WHEN birthday IS NOT NULL THEN cpf END) AS users_with_birthday,
    COUNT(DISTINCT CASE WHEN job IS NOT NULL THEN cpf END) AS users_with_job,
    COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) AS users_with_name,
    COUNT(DISTINCT CASE WHEN delivery_address IS NOT NULL THEN cpf END) AS users_with_address,
    SUM(is_complete_profile) AS complete_profiles,

    ROUND(AVG(age), 1) AS avg_user_age,

    MODE(job_category) AS top_job_category,

    ROUND(
      (COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN birthday IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN job IS NOT NULL THEN cpf END) +
       COUNT(DISTINCT CASE WHEN delivery_address IS NOT NULL THEN cpf END)) * 100.0
      / (COUNT(DISTINCT cpf) * 5), 1
    ) AS profile_completeness_pct

  FROM user_metrics
  GROUP BY COALESCE(city, 'Unknown')
)

SELECT
  city,
  total_users,
  users_with_email,
  users_with_birthday,
  users_with_job,
  users_with_name,
  users_with_address,
  complete_profiles,
  avg_user_age,
  top_job_category,
  profile_completeness_pct,

  CASE
    WHEN profile_completeness_pct >= 90 THEN 'Excellent'
    WHEN profile_completeness_pct >= 75 THEN 'Good'
    WHEN profile_completeness_pct >= 50 THEN 'Fair'
    ELSE 'Poor'
  END AS data_quality_grade,

  current_timestamp() AS computed_at

FROM city_demographics
ORDER BY total_users DESC;

-- REAL-TIME BI VALUE:
--
-- Immediate Insights:
-- - Dashboard refreshes as data changes
-- - Data quality monitoring in real-time
-- - Geographic expansion decisions with fresh data
-- - Profile completeness tracked continuously
--
-- Executive Dashboards:
-- - "As of now" metrics (not hours-old)
-- - Real-time user base size
-- - Live demographic distributions
-- - Instant data quality alerts
--
-- Streaming Pattern:
-- LIVE TABLE incrementally updates demographics as:
-- - New users register (INSERT events)
-- - Users update profiles (UPDATE events)
-- - Users change cities (geographic shifts)
-- - Data quality improves (more complete profiles)
