-- BRONZE LAYER - MongoDB User CDC Stream (Real-time Change Events)
--
-- PURPOSE:
-- This module ingests CDC (Change Data Capture) events from MongoDB in real-time.
-- Unlike batch ingestion (snapshots), this processes individual INSERT/UPDATE/DELETE operations
-- as they occur in the source system.
--
-- WHAT IT DOES:
-- - Reads CDC events using Auto Loader (cloud_files)
-- - Creates a STREAMING TABLE (not materialized view)
-- - Preserves CDC metadata: operation type, sequence number
-- - Enables continuous processing for real-time data
--
-- DATA FLOW:
--   MongoDB Change Streams → CDC Events (JSON)
--     → cloud_files() / Auto Loader (streaming ingestion)
--     → bronze_mongodb_users_stream (streaming table)
--     → Silver layer for real-time CDC processing
--
-- CDC EVENT STRUCTURE:
-- Each event contains:
-- - operation: INSERT, UPDATE, DELETE (CDC operation type)
-- - sequenceNum: Event ordering for correct temporal sequence
-- - user data fields: email, delivery_address, city, etc.
-- - dt_current_timestamp: When the change occurred in source system
--
-- WHY STREAMING TABLE (NOT MATERIALIZED VIEW)?
-- - Real-time CDC events arrive continuously
-- - Need sub-second latency for downstream processing
-- - Auto Loader handles incremental file discovery
-- - Streaming pattern for event-driven architecture
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
-- - Automatic file discovery (new files appear)
-- - Checkpoint management for fault tolerance
-- - Scalable for high-volume CDC streams
--
-- CDC OPERATION TYPES:
-- - INSERT: New user created in MongoDB
-- - UPDATE: User data modified (email, address, etc.)
-- - DELETE: User removed from MongoDB (soft delete in our pipeline)
--
-- LEARNING OBJECTIVES:
-- - Understand streaming CDC vs batch CDC
-- - Use Auto Loader (cloud_files) for incremental ingestion
-- - Process CDC events with operation types
-- - Handle sequence numbers for temporal ordering
-- - Configure STREAMING TABLE for real-time processing
--
-- OUTPUT SCHEMA:
-- - user_id: User identifier (from source)
-- - email: User email address
-- - delivery_address: Delivery location
-- - city: City name
-- - cpf: Brazilian tax ID (business key)
-- - phone_number: Contact number
-- - uuid: Universal unique identifier
-- - country: Country code
-- - operation: CDC operation type (INSERT/UPDATE/DELETE)
-- - sequenceNum: Event sequence for ordering
-- - dt_current_timestamp: Source system timestamp
-- - _rescued_data: Auto Loader schema evolution column

CREATE OR REFRESH STREAMING TABLE bronze_mongodb_users_stream
COMMENT 'Real-time CDC events from MongoDB operational database - streaming ingestion'
TBLPROPERTIES (
  'quality' = 'bronze',
  'layer' = 'ingestion',
  'source' = 'mongodb',
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
  'abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/cdc/mongodb/users',
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
-- 1. Auto Loader checkpoint location: managed by DLT automatically
-- 2. Schema evolution: New fields captured in _rescued_data column
-- 3. Exactly-once semantics: Guaranteed by Auto Loader + Delta
-- 4. Backpressure: Controlled by maxFilesPerTrigger setting
-- 5. Monitoring: Check streaming metrics in DLT UI

-- COST OPTIMIZATION:
-- - Runs continuously (24/7) vs batch (1-2 hours/day)
-- - Use serverless for auto-scaling
-- - Monitor DBU consumption in streaming mode
-- - Consider trigger intervals if strict real-time not needed

-- TROUBLESHOOTING:
-- - If schema mismatches: Check _rescued_data column
-- - If missing events: Verify sequenceNum continuity
-- - If lag increases: Increase cluster size or maxFilesPerTrigger
-- - If duplicate events: Verify exactly-once is working (check watermarks)
