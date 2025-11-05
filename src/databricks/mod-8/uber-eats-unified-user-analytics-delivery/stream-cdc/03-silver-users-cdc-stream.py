"""
SILVER LAYER - User CDC Processing (Real-time SCD Type 1 and Type 2)

PURPOSE:
This module applies Change Data Capture (CDC) to the unified event stream, creating two tables
with different historical tracking strategies. It uses Lakeflow's apply_changes() to automatically
handle INSERT, UPDATE, and DELETE operations in real-time.

WHAT IT DOES:
- Reads unified CDC event stream from silver_users_staging_stream
- Creates SCD Type 1 table (current state only) for operational queries
- Creates SCD Type 2 table (full history) for audit and compliance
- Automatically handles out-of-order events using SEQUENCE BY timestamp
- Tracks all changes for LGPD/GDPR compliance requirements

DATA FLOW:
  silver_users_staging_stream (streaming CDC events)
    -> apply_changes() with streaming source
    -> silver_users_unified_stream (SCD Type 1)
    -> silver_users_history_stream (SCD Type 2)

WHY TWO CDC TABLES?

SCD Type 1 (Current State):
- Stores ONLY the latest version of each user
- UPDATE overwrites previous values
- DELETE physically removes the record (or marks as deleted)
- Use Cases: Operational dashboards, current user lookups, real-time reporting
- Example: Marketing needs current email addresses for campaigns

SCD Type 2 (Full History):
- Stores ALL versions of each user over time
- UPDATE closes old record (__END_AT) and creates new record
- DELETE soft-deletes by setting __END_AT timestamp
- Adds columns: __START_AT, __END_AT, __CURRENT
- Use Cases: Audit trails, compliance reporting, historical analysis
- Example: LGPD requires tracking when email addresses changed

KEY DIFFERENCES FROM BATCH CDC:

Batch CDC (Snapshot-based):
- Source: MATERIALIZED VIEW (complete snapshots)
- Pattern: create_target_table() + apply_changes()
- Processing: Compares snapshots to detect changes
- Trigger: On-demand or scheduled (hourly/daily)
- Cost: Lower (processes 1-2 hours/day)

Streaming CDC (Event-based):
- Source: STREAMING TABLE (CDC events)
- Pattern: create_streaming_table() + apply_changes()
- Processing: Processes CDC events as they arrive
- Trigger: Continuous (always running)
- Cost: Higher (processes 24/7)

AUTO CDC FLOW FEATURES:
- Automatically detects INSERT, UPDATE, DELETE operations from event stream
- Handles out-of-order events using SEQUENCE BY sequenceNum + timestamp
- Maintains referential integrity across both SCD tables
- Deduplicates simultaneous changes
- Supports partial updates (MongoDB event + MSSQL event merge)

OUT-OF-ORDER HANDLING:
SEQUENCE BY ensures events are processed in correct order:
- If User 1 update at 10:00 arrives after update at 10:05
- CDC processes them in timestamp/sequenceNum order (10:00 first, then 10:05)
- Prevents data inconsistency from network delays or async CDC streams

PARTIAL EVENT MERGING:
apply_changes() merges events by key (cpf):
- MongoDB event: cpf=123, email=new@email.com, operation=UPDATE
- MSSQL event: cpf=123, first_name=NewName, operation=UPDATE
- Result: Both fields updated in unified profile

LEARNING OBJECTIVES:
- Implement streaming AUTO CDC Flow with apply_changes()
- Understand SCD Type 1 vs Type 2 tradeoffs in real-time
- Handle streaming CDC for slowly changing dimensions
- Design CDC pipelines for real-time compliance
- Use SEQUENCE BY for out-of-order event handling in streams

CONFIGURATION:
- keys: cpf (Brazilian business key for change detection)
- sequence_by: sequenceNum + dt_current_timestamp (temporal ordering)
- stored_as_scd_type: 1 or 2 (historical tracking strategy)
- track_history_column_list: ONLY for Type 2 (columns to track changes)
- apply_as_deletes: operation = 'DELETE' (explicit delete handling)

OUTPUT SCHEMAS:

silver_users_unified_stream (SCD Type 1):
- cpf: Brazilian unified identifier (business key)
- user_id: System identifier
- uuid: Universal unique identifier
- email, delivery_address, city: MongoDB fields
- first_name, last_name, birthday, job, company_name: MSSQL fields
- phone_number, country: Common fields
- dt_current_timestamp: Source system timestamp

silver_users_history_stream (SCD Type 2):
- All columns from SCD Type 1 PLUS:
- __START_AT: When this version became active
- __END_AT: When this version was superseded (null for current)
- __CURRENT: Boolean flag for current version (or check __END_AT IS NULL)

PRODUCTION NOTES:
- track_history_column_list is ONLY supported for SCD Type 2
- For Type 1, all columns are automatically tracked (overwrite mode)
- Type 2 explicitly lists which columns trigger new versions
- Streaming CDC runs continuously (24/7) vs batch (1-2 hours/day)
- Monitor DBU consumption and streaming lag metrics
"""

import dlt
from pyspark.sql.functions import col, current_timestamp, count, window, struct
from pyspark.sql.functions import max as spark_max, min as spark_min, avg

# ============================================================================
# SCD TYPE 1 - Current State Only (Streaming)
# ============================================================================
# Creates: silver_users_unified_stream
# Behavior: UPDATE overwrites, DELETE removes (real-time)
# Use Case: Operational queries, marketing campaigns, customer support
# Track History: Not applicable (Type 1 doesn't track history)

# Define the streaming target table structure
dlt.create_streaming_table(
    name="silver_users_unified_stream",
    comment="Current state of unified user profiles - SCD Type 1 for real-time operational queries",
    table_properties={
        "quality": "silver",
        "layer": "curation",
        "scd_type": "1",
        "use_case": "operations",
        "pattern": "streaming_cdc",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Apply CDC changes from streaming source (real-time processing)
dlt.apply_changes(
    target="silver_users_unified_stream",
    source="silver_users_staging_stream",  # STREAMING TABLE (not materialized view!)
    keys=["cpf"],  # Business key for identifying records
    sequence_by=col("sequenceNum"),  # Primary ordering by sequence number
    # Alternative: sequence_by=col("dt_current_timestamp") if sequenceNum not available
    stored_as_scd_type=1,  # Current state only (overwrite on UPDATE)

    # Explicit DELETE handling for streaming CDC
    apply_as_deletes="operation = 'DELETE'",  # When operation=DELETE, remove record

    # Column configuration
    column_list=None,  # Track all columns for Type 1
    except_column_list=[
        "operation",          # Exclude CDC metadata
        "sequenceNum",        # Exclude CDC metadata
        "source_system",      # Exclude CDC metadata
        "ingestion_timestamp" # Exclude pipeline metadata
    ],

    # Streaming-specific configuration
    ignore_null_updates=False,  # Process all events (even if some fields are null)
    apply_as_truncates=None     # No truncate logic for streaming
)

# ============================================================================
# SCD TYPE 2 - Full History (Streaming)
# ============================================================================
# Creates: silver_users_history_stream
# Behavior: UPDATE closes old record + creates new, DELETE soft-deletes (real-time)
# Use Case: LGPD/GDPR compliance, audit trails, historical analysis
# Track History: Explicitly lists columns that trigger new versions

# Define the streaming target table structure
dlt.create_streaming_table(
    name="silver_users_history_stream",
    comment="Complete change history of user profiles - SCD Type 2 for real-time audit and compliance (LGPD/GDPR)",
    table_properties={
        "quality": "silver",
        "layer": "curation",
        "scd_type": "2",
        "use_case": "compliance_audit",
        "pattern": "streaming_cdc",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Apply CDC changes with full history tracking (real-time)
dlt.apply_changes(
    target="silver_users_history_stream",
    source="silver_users_staging_stream",  # STREAMING TABLE
    keys=["cpf"],  # Business key
    sequence_by=col("sequenceNum"),  # Primary ordering

    stored_as_scd_type=2,  # Full history (create new versions on UPDATE)

    # Track specific columns for versioning (field-level change detection)
    track_history_column_list=[
        "email",            # Track email changes (LGPD/GDPR requirement)
        "delivery_address",  # Track address changes (PII tracking)
        "city",             # Track city changes (location tracking)
        "first_name",       # Track name changes (identity verification)
        "last_name",        # Track name changes (identity verification)
        "job",              # Track job changes (demographic analysis)
        "company_name"      # Track company changes (B2B insights)
    ],
    # When any of these columns change, a new version is created
    # Other columns (user_id, uuid, phone_number, country) still stored but don't trigger versioning

    # Explicit DELETE handling
    apply_as_deletes="operation = 'DELETE'",  # Soft delete: sets __END_AT timestamp

    # Column configuration
    except_column_list=[
        "operation",          # Exclude CDC metadata
        "sequenceNum",        # Exclude CDC metadata
        "source_system",      # Exclude CDC metadata
        "ingestion_timestamp" # Exclude pipeline metadata
    ],

    # Streaming-specific configuration
    ignore_null_updates=False,  # Process all events
    apply_as_truncates=None

    # NOTE: SCD Type 2 automatically adds these columns:
    # - __START_AT: When this version became active (TIMESTAMP)
    # - __END_AT: When this version was superseded (TIMESTAMP, NULL = current)
    # - __CURRENT: Boolean flag for current version (TRUE/FALSE)
    #
    # To query current version: WHERE __END_AT IS NULL
    # To query historical version: WHERE __START_AT <= '2025-01-01' AND (__END_AT > '2025-01-01' OR __END_AT IS NULL)
)


# ============================================================================
# STREAMING CDC MONITORING TABLE (Optional)
# ============================================================================
# Track CDC processing metrics for monitoring and alerting

@dlt.table(
    name="silver_cdc_monitoring",
    comment="Streaming CDC processing metrics for monitoring",
    table_properties={
        "quality": "silver",
        "layer": "monitoring",
        "use_case": "observability"
    }
)
def silver_cdc_monitoring():
    """
    Aggregates CDC events for monitoring streaming pipeline health.

    Metrics tracked:
    - Event counts by operation type (INSERT/UPDATE/DELETE)
    - Event counts by source system (MongoDB/MSSQL)
    - Processing lag (event time vs processing time)
    - sequenceNum gaps (missing events)
    """
    from pyspark.sql.functions import count, max as spark_max, min as spark_min, avg

    staging = dlt.read_stream("silver_users_staging_stream")

    return staging.groupBy(
        window(col("dt_current_timestamp"), "5 minutes"),  # 5-minute tumbling window
        col("operation"),
        col("source_system")
    ).agg(
        count("*").alias("event_count"),
        spark_min("sequenceNum").alias("min_sequence"),
        spark_max("sequenceNum").alias("max_sequence"),
        avg(
            (current_timestamp().cast("long") - col("dt_current_timestamp").cast("long"))
        ).alias("avg_lag_seconds")
    )


# ============================================================================
# PRODUCTION NOTES - STREAMING CDC SPECIFIC
# ============================================================================

"""
STREAMING CDC COST IMPLICATIONS:

Batch CDC (Snapshot-based):
- Runs: 1-2 hours/day
- DBUs: ~100/month
- Cost: ~$50/month

Streaming CDC (Event-based):
- Runs: 24 hours/day × 30 days = 720 hours/month
- DBUs: ~800/month
- Cost: ~$400/month

Trade-off: Real-time capability = 8x cost increase

WHEN TO USE STREAMING CDC:
✓ Real-time fraud detection required
✓ Sub-second latency needed
✓ High-volume transactional data
✓ Event-driven downstream consumers
✓ Budget allows for continuous processing

WHEN TO USE BATCH CDC:
✓ Hourly/daily freshness sufficient
✓ Slowly changing dimension data (user profiles)
✓ Cost optimization priority
✓ Source systems export snapshots (not CDC events)

MONITORING CHECKLIST:
□ Check streaming lag metrics (event time vs processing time)
□ Monitor sequenceNum continuity (gaps indicate missing events)
□ Track DBU consumption vs budget
□ Alert on DELETE operations (unusual for user profiles)
□ Validate SCD Type 1 and Type 2 consistency

TROUBLESHOOTING:
- High lag: Scale up cluster or optimize transformations
- Missing events: Check sequenceNum gaps in monitoring table
- High cost: Consider triggered mode with intervals vs continuous
- Data inconsistency: Verify sequenceBy ordering is correct
- Schema evolution: Check _rescued_data in Bronze tables
"""
