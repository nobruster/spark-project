-- SILVER LAYER - User Data Staging (Unified Domain Preparation)
--
-- PURPOSE:
-- This module unifies user data from MongoDB and MSSQL into a single staging table before applying CDC.
-- It implements FULL OUTER JOIN with conflict resolution to handle users existing in one or both systems.
-- This is where type casting happens (Bronze contains raw data, Silver standardizes types).
--
-- WHAT IT DOES:
-- - Merges MongoDB (delivery data) with MSSQL (profile data) at user_id grain
-- - Resolves field conflicts using COALESCE (prioritizes non-null values)
-- - Applies type casting for data standardization (Bronze is raw, Silver is typed)
-- - Creates a complete user profile combining operational and CRM data
-- - Prepares clean, unified data for CDC processing
--
-- DATA FLOW:
--   bronze_mongodb_users (raw) + bronze_mssql_users (raw)
--     -> FULL OUTER JOIN on user_id
--     -> Type casting (STRING → BIGINT, TIMESTAMP, DATE)
--     -> COALESCE for conflict resolution
--     -> silver_users_staging (unified, typed schema)
--     -> Input for AUTO CDC Flow
--
-- WHY FULL OUTER JOIN?
-- Different systems may have partial user data:
-- - New MongoDB users (orders placed) may not yet be in MSSQL (profile pending)
-- - MSSQL registered users may not have placed orders yet (no MongoDB record)
-- - Most users exist in BOTH systems (ideal state)
--
-- FULL OUTER JOIN ensures we capture ALL users regardless of source coverage.
--
-- CONFLICT RESOLUTION STRATEGY:
-- When a field exists in both sources, we use COALESCE(mongodb.field, mssql.field):
-- - Prioritizes the FIRST non-null value encountered
-- - For common fields (cpf, phone, uuid, country):
--   * MongoDB value takes precedence (more frequently updated)
--   * Falls back to MSSQL if MongoDB is null
--
-- TYPE CASTING IN SILVER (NOT BRONZE):
-- Bronze contains exact source replicas (all types as-is from JSON).
-- Silver applies standardization:
-- - user_id: STRING → BIGINT (for numeric operations and joins)
-- - birthday: STRING → DATE (for age calculations)
-- - dt_current_timestamp: STRING → TIMESTAMP (for CDC sequencing)
--
-- This follows Medallion best practices:
-- - Bronze = Raw replica (no transformations)
-- - Silver = Cleaned, typed, unified data
--
-- WHY MATERIALIZED VIEW (NOT STREAMING)?
-- Both bronze tables are materialized views (batch), so staging must also be batch:
-- - Batch-to-batch transformations use materialized views
-- - Streaming requires streaming sources (we use batch here)
-- - Refresh schedule aligns with bronze layer updates
--
-- KEY FEATURES:
-- - FULL OUTER JOIN: Captures users from either system
-- - Type casting: Standardizes data types from raw Bronze
-- - Conflict resolution: COALESCE handles duplicate fields
-- - Unified schema: 14 fields combining both sources
-- - Change Data Feed: Enables downstream CDC processing
--
-- UNIFIED SCHEMA DESIGN:
-- From MongoDB (delivery-focused):
--   - email, delivery_address, city
-- From MSSQL (profile-focused):
--   - first_name, last_name, birthday, job, company_name
-- Common fields (prioritize MongoDB):
--   - cpf, phone_number, uuid, country
-- System fields:
--   - user_id (join key), dt_current_timestamp (for CDC sequencing)
--
-- LEARNING OBJECTIVES:
-- - Implement FULL OUTER JOIN for multi-source unification
-- - Apply type casting in Silver (not Bronze)
-- - Apply conflict resolution with COALESCE
-- - Design unified schema merging disparate sources
-- - Prepare staging data for CDC workflows
--
-- OUTPUT SCHEMA:
-- - user_id: Unique identifier (BIGINT, from either source)
-- - email: Delivery email (STRING, MongoDB)
-- - delivery_address: Current address (STRING, MongoDB)
-- - city: City (STRING, MongoDB)
-- - first_name: User first name (STRING, MSSQL)
-- - last_name: User last name (STRING, MSSQL)
-- - birthday: Date of birth (DATE, MSSQL)
-- - job: Job title (STRING, MSSQL)
-- - company_name: Employer (STRING, MSSQL)
-- - cpf: Tax ID (STRING, MongoDB first, then MSSQL)
-- - phone_number: Phone (STRING, MongoDB first, then MSSQL)
-- - uuid: UUID (STRING, MongoDB first, then MSSQL)
-- - country: Country (STRING, MongoDB first, then MSSQL)
-- - dt_current_timestamp: Latest timestamp (TIMESTAMP, for CDC sequencing)

CREATE OR REFRESH MATERIALIZED VIEW silver_users_staging
COMMENT 'Unified user domain combining MongoDB delivery data with MSSQL profile data - staging for CDC processing'
TBLPROPERTIES (
  'quality' = 'silver',
  'layer' = 'staging',
  'sources' = 'mongodb,mssql',
  'join_type' = 'full_outer',
  'delta.enableChangeDataFeed' = 'true'
)
AS
WITH deduplicated_mongo AS (
  -- Deduplicate MongoDB data: keep latest record per CPF
  SELECT
    cpf,
    user_id,
    uuid,
    email,
    delivery_address,
    city,
    phone_number,
    country,
    dt_current_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY cpf
      ORDER BY
        CAST(dt_current_timestamp AS TIMESTAMP) DESC NULLS LAST,
        ingestion_timestamp DESC
    ) AS rn
  FROM bronze_mongodb_users
  WHERE cpf IS NOT NULL
),
deduplicated_mssql AS (
  -- Deduplicate MSSQL data: keep latest record per CPF
  SELECT
    cpf,
    user_id,
    uuid,
    first_name,
    last_name,
    birthday,
    job,
    company_name,
    phone_number,
    country,
    dt_current_timestamp,
    ROW_NUMBER() OVER (
      PARTITION BY cpf
      ORDER BY
        CAST(dt_current_timestamp AS TIMESTAMP) DESC NULLS LAST,
        ingestion_timestamp DESC
    ) AS rn
  FROM bronze_mssql_users
  WHERE cpf IS NOT NULL
)
SELECT
  -- Business key: CPF (Brazilian unified identifier) is the true user key
  COALESCE(mongo.cpf, mssql.cpf) AS cpf,

  -- System identifiers (may differ between systems)
  CAST(COALESCE(mongo.user_id, mssql.user_id) AS BIGINT) AS user_id,
  COALESCE(mongo.uuid, mssql.uuid) AS uuid,

  -- Delivery data (from MongoDB operational database)
  mongo.email,
  mongo.delivery_address,
  mongo.city,

  -- Profile data (from MSSQL CRM system)
  mssql.first_name,
  mssql.last_name,
  CAST(mssql.birthday AS DATE) AS birthday,
  mssql.job,
  mssql.company_name,

  -- Common fields (prioritize MongoDB as more frequently updated)
  COALESCE(mongo.phone_number, mssql.phone_number) AS phone_number,
  COALESCE(mongo.country, mssql.country) AS country,

  -- CDC sequencing timestamp (latest from either system)
  CAST(
    GREATEST(
      COALESCE(mongo.dt_current_timestamp, '1900-01-01 00:00:00'),
      COALESCE(mssql.dt_current_timestamp, '1900-01-01 00:00:00')
    ) AS TIMESTAMP
  ) AS dt_current_timestamp,

  current_timestamp() AS processed_timestamp

FROM deduplicated_mongo AS mongo
FULL OUTER JOIN deduplicated_mssql AS mssql
  ON mongo.cpf = mssql.cpf  -- Join on CPF (Brazilian business key), not user_id
  AND mongo.rn = 1
  AND mssql.rn = 1
WHERE
  COALESCE(mongo.cpf, mssql.cpf) IS NOT NULL
  AND (mongo.rn = 1 OR mongo.rn IS NULL)
  AND (mssql.rn = 1 OR mssql.rn IS NULL);
