-- BRONZE LAYER - MongoDB User Data Ingestion (Operational Database)
--
-- PURPOSE:
-- This module ingests user delivery-related information from MongoDB into the bronze layer.
-- MongoDB stores operational data updated when users place orders or change delivery preferences.
--
-- WHAT IT DOES:
-- - Reads JSON files exported from MongoDB's operational database
-- - Creates a materialized view for batch processing (not streaming)
-- - Preserves EXACT raw data structure WITHOUT any transformations
-- - Only adds metadata columns for data lineage and audit tracking
--
-- DATA FLOW:
--   Cloud Storage (mongodb_users_*.json)
--     -> read_files() batch ingestion
--     -> bronze_mongodb_users (raw replica + metadata)
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
-- User profile data changes slowly compared to transactional data (orders, deliveries).
-- Batch processing is more cost-effective and appropriate for this use case:
-- - Updates happen hourly or daily (scheduled refresh)
-- - No need for sub-second latency
-- - Reduces compute costs significantly vs continuous processing
--
-- KEY FEATURES:
-- - Raw ingestion: SELECT * preserves all source fields as-is
-- - Schema-on-read: Automatically infers structure from JSON
-- - Materialized view: Snapshot refreshed on schedule
-- - Change Data Feed: Enables incremental processing in Silver layer
--
-- MONGODB DATA CHARACTERISTICS:
-- - Source System: MongoDB (operational database)
-- - Update Frequency: High (every order or delivery preference change)
-- - Data Grain: One record per user_id
-- - Key Fields: email, delivery_address, city (delivery-focused)
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

CREATE OR REFRESH MATERIALIZED VIEW bronze_mongodb_users
COMMENT 'Raw user delivery data from MongoDB operational database - exact replica with metadata only'
TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'ingestion',
  'source' = 'mongodb',
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
  'abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/mongodb/users/*',
  format => 'json'
);
