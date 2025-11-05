-- BRONZE LAYER - MSSQL User Data Ingestion (User Management System / CRM)
--
-- PURPOSE:
-- This module ingests user personal and professional information from MSSQL into the bronze layer.
-- MSSQL stores CRM data updated when users register or update their profile information.
--
-- WHAT IT DOES:
-- - Reads JSON files exported from MSSQL user management system
-- - Creates a materialized view for batch processing (not streaming)
-- - Preserves EXACT raw data structure WITHOUT any transformations
-- - Only adds metadata columns for data lineage and audit tracking
--
-- DATA FLOW:
--   Cloud Storage (mssql_users_*.json)
--     -> read_files() batch ingestion
--     -> bronze_mssql_users (raw replica + metadata)
--     -> Silver layer for type casting and transformations
--
-- WHY NO TRANSFORMATIONS IN BRONZE?
-- Bronze layer philosophy - "Land it as-is":
-- - Exact replica of source data (no CAST, no renaming, no filtering)
-- - Easier troubleshooting (see exactly what source sent)
-- - Flexibility to reprocess Silver with different logic
-- - Auditability for compliance (perfect source replica)
-- - Type casting belongs in Silver, not Bronze
--
-- WHY MATERIALIZED VIEW (NOT STREAMING)?
-- Profile data (name, birthday, job, company) changes infrequently:
-- - Updates happen only when users modify their account settings
-- - Batch refresh (hourly/daily) provides sufficient freshness
-- - Significantly more cost-effective than continuous streaming
-- - Aligns with source system update patterns
--
-- KEY FEATURES:
-- - Raw ingestion: SELECT * preserves all source fields as-is
-- - Schema-on-read: Automatically infers structure from JSON
-- - Materialized view: Snapshot refreshed on schedule
-- - Change Data Feed: Enables incremental processing in Silver layer
--
-- MSSQL DATA CHARACTERISTICS:
-- - Source System: MSSQL (user management / CRM)
-- - Update Frequency: Low (only on profile updates)
-- - Data Grain: One record per user_id
-- - Key Fields: first_name, last_name, birthday, job, company_name (profile-focused)
--
-- LEARNING OBJECTIVES:
-- - Understand pure Bronze layer (no transformations)
-- - Learn when to use MATERIALIZED VIEW vs STREAMING TABLE
-- - Implement metadata enrichment for lineage tracking
-- - Configure Delta properties for CDC-ready tables
--
-- OUTPUT SCHEMA:
-- - * (all source columns, preserved as-is with original types)
-- - ingestion_timestamp: When record was ingested (metadata)
-- - source_file: Origin file path for lineage (metadata)

CREATE OR REFRESH MATERIALIZED VIEW bronze_mssql_users
COMMENT 'Raw user profile data from MSSQL user management system - exact replica with metadata only'
TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'ingestion',
  'source' = 'mssql',
  'transformation' = 'none',
  'refresh_schedule' = 'hourly',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM read_files(
  'abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/mssql/users/*',
  format => 'json'
);
