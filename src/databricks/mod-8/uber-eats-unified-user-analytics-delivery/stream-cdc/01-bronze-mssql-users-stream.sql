-- BRONZE LAYER - MSSQL User CDC Stream (Real-time Change Events)
--
-- PURPOSE:
-- This module ingests CDC (Change Data Capture) events from MSSQL in real-time.
-- Processes individual INSERT/UPDATE/DELETE operations from the CRM system
-- as they occur, enabling real-time user profile updates.
--
-- WHAT IT DOES:
-- - Reads CDC events using Auto Loader (cloud_files)
-- - Creates a STREAMING TABLE for continuous processing
-- - Preserves CDC metadata: operation type, sequence number
-- - Enables real-time profile updates downstream
--
-- DATA FLOW:
--   MSSQL CDC / Change Tracking → CDC Events (JSON)
--     → cloud_files() / Auto Loader (streaming ingestion)
--     → bronze_mssql_users_stream (streaming table)
--     → Silver layer for real-time CDC processing
--
-- CDC EVENT STRUCTURE:
-- Each event contains:
-- - operation: INSERT, UPDATE, DELETE (CDC operation type)
-- - sequenceNum: Event ordering for correct temporal sequence
-- - profile fields: first_name, last_name, birthday, job, company_name
-- - dt_current_timestamp: When the change occurred in source system
--
-- WHY STREAMING TABLE (NOT MATERIALIZED VIEW)?
-- - CRM profile updates need real-time propagation
-- - Marketing campaigns require up-to-date user data
-- - Compliance requires immediate audit trail updates
-- - Event-driven architecture for downstream consumers
--
-- KEY DIFFERENCES FROM BATCH CDC:
-- Batch (Snapshots):
--   - Full data dumps periodically
--   - MATERIALIZED VIEW
--   - Compares snapshots to find changes
--   - Hourly/daily refresh
--
-- Streaming (CDC Events):
--   - Individual change events
--   - STREAMING TABLE
--   - Processes actual CDC operations
--   - Continuous (real-time)
--
-- AUTO LOADER FEATURES:
-- - Schema inference and evolution
-- - Exactly-once processing semantics
-- - Automatic file discovery (new CDC files)
-- - Checkpoint management for fault tolerance
-- - Scalable for high-volume CDC streams
--
-- CDC OPERATION TYPES:
-- - INSERT: New user registered in CRM
-- - UPDATE: Profile modified (name, job, birthday, etc.)
-- - DELETE: User removed from CRM (soft delete in our pipeline)
--
-- MSSQL CDC DATA CHARACTERISTICS:
-- - Source System: MSSQL (user management / CRM)
-- - Update Frequency: Lower than MongoDB (profile updates only)
-- - Data Grain: One CDC event per change operation
-- - Key Fields: first_name, last_name, birthday, job, company_name
--
-- LEARNING OBJECTIVES:
-- - Process streaming CDC events from MSSQL
-- - Use Auto Loader for incremental CDC ingestion
-- - Handle CDC operation types (INSERT/UPDATE/DELETE)
-- - Configure streaming tables for real-time processing
-- - Understand exactly-once semantics in streaming
--
-- OUTPUT SCHEMA:
-- - user_id: User identifier (from source)
-- - first_name: User first name
-- - last_name: User last name
-- - birthday: Date of birth
-- - job: Job title
-- - company_name: Employer name
-- - cpf: Brazilian tax ID (business key)
-- - phone_number: Contact number
-- - uuid: Universal unique identifier
-- - country: Country code
-- - operation: CDC operation type (INSERT/UPDATE/DELETE)
-- - sequenceNum: Event sequence for ordering
-- - dt_current_timestamp: Source system timestamp
-- - _rescued_data: Auto Loader schema evolution column

CREATE OR REFRESH STREAMING TABLE bronze_mssql_users_stream
COMMENT 'Real-time CDC events from MSSQL CRM system - streaming ingestion'
TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'ingestion',
  'source' = 'mssql',
  'pattern' = 'streaming_cdc',
  'delta.enableChangeDataFeed' = 'true',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
SELECT
  -- ALL source fields preserved AS-IS (no casting, no transformations)
  *,

  -- Auto Loader metadata only
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file,
  _metadata.file_modification_time AS file_modified_at

FROM cloud_files(
  'abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/cdc/mssql/users',
  'json',
  map(
    -- Auto Loader configuration
    'cloudFiles.inferColumnTypes', 'true',          -- Infer schema automatically
    'cloudFiles.schemaEvolutionMode', 'rescue',     -- Handle schema changes gracefully
    'cloudFiles.maxFilesPerTrigger', '1000',        -- Process up to 1000 files per micro-batch
    'cloudFiles.useNotifications', 'false'          -- Use directory listing (not Event Grid)
  )
);

-- BRONZE LAYER PHILOSOPHY:
-- - NO transformations (no CAST, no WHERE, no renaming)
-- - Exact replica of source CDC events
-- - Type casting happens in Silver layer
-- - Data quality filtering happens in Silver layer
-- - Only metadata enrichment allowed (ingestion_timestamp, source_file)

-- PRODUCTION NOTES:
-- 1. Auto Loader checkpoint: Managed automatically by DLT
-- 2. Schema evolution: New CRM fields captured in _rescued_data
-- 3. Exactly-once guarantee: Enabled by Auto Loader + Delta Lake
-- 4. Backpressure control: maxFilesPerTrigger limits micro-batch size
-- 5. Monitoring: Streaming metrics available in DLT UI

-- COST CONSIDERATIONS:
-- - Continuous processing (24/7) vs batch (1-2 hours/day)
-- - ~8x higher cost than batch CDC
-- - Use serverless compute for auto-scaling
-- - Monitor DBU consumption closely
-- - Consider trigger intervals for cost optimization

-- CDC SOURCE SYSTEMS COMPARISON:
-- MongoDB:
--   - Operational data (high update frequency)
--   - Delivery-focused fields
--   - Change streams or oplog
--
-- MSSQL:
--   - CRM/profile data (low update frequency)
--   - Profile-focused fields
--   - CDC or Change Tracking enabled

-- TROUBLESHOOTING:
-- - Schema mismatch: Check _rescued_data for new fields
-- - Missing events: Verify sequenceNum continuity gaps
-- - Increasing lag: Scale up compute or increase maxFilesPerTrigger
-- - Duplicate processing: Check watermarks and checkpoints
-- - Type conversion errors: Adjust cloudFiles.schemaHints
