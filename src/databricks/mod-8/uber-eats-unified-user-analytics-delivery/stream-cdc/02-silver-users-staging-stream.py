"""
SILVER LAYER - User CDC Stream Staging (Real-time Unification)

PURPOSE:
This module unifies CDC events from MongoDB and MSSQL in real-time using stream processing.
Unlike batch CDC (which compares snapshots), this processes individual change events as they arrive.

WHAT IT DOES:
- Reads CDC streams from Bronze (MongoDB + MSSQL)
- Performs stream-stream join or union of CDC events
- Applies conflict resolution for overlapping fields
- Creates unified CDC event stream for downstream processing
- Maintains temporal ordering using sequenceNum

DATA FLOW:
  bronze_mongodb_users_stream + bronze_mssql_users_stream
    → Stream-stream processing
    → Conflict resolution (COALESCE)
    → silver_users_staging_stream
    → Input for streaming apply_changes()

WHY STREAM-STREAM PROCESSING?
Unlike batch CDC (FULL OUTER JOIN on snapshots), streaming CDC processes events:
- Events arrive independently from each source
- Need to merge events in real-time
- Can't wait for complete snapshot
- Use event time (sequenceNum, dt_current_timestamp) for ordering

STREAM PROCESSING PATTERNS:
1. Union Pattern: Combine both streams, let apply_changes() handle merging
2. Join Pattern: Join streams by key with windowing (complex, usually avoided)

For CDC use case: UNION pattern is simpler and more efficient.

KEY DIFFERENCES FROM BATCH CDC:
Batch (Snapshots):
  - FULL OUTER JOIN on complete snapshots
  - MATERIALIZED VIEW
  - All data available at once

Streaming (CDC Events):
  - UNION of event streams
  - STREAMING TABLE
  - Events arrive incrementally
  - apply_changes() handles merging by key

LEARNING OBJECTIVES:
- Process streaming CDC events in real-time
- Use dlt.read_stream() for continuous ingestion
- Understand stream-stream processing patterns
- Handle temporal ordering in event streams
- Configure streaming tables for CDC processing

OUTPUT SCHEMA:
- cpf: Business key (Brazilian tax ID)
- user_id: System identifier
- uuid: Universal identifier
- email: From MongoDB events
- delivery_address: From MongoDB events
- city: From MongoDB events
- first_name: From MSSQL events
- last_name: From MSSQL events
- birthday: From MSSQL events
- job: From MSSQL events
- company_name: From MSSQL events
- phone_number: From either source
- country: From either source
- operation: CDC operation (INSERT/UPDATE/DELETE)
- sequenceNum: Event sequence number
- dt_current_timestamp: Event timestamp
- source_system: Which system generated the event
"""

import dlt
from pyspark.sql.functions import col, lit, coalesce, when

# ============================================================================
# STREAMING TABLE: Unified CDC Events
# ============================================================================
# Combines CDC events from both MongoDB and MSSQL streams
# Uses UNION pattern (not JOIN) for streaming CDC

@dlt.table(
    name="silver_users_staging_stream",
    comment="Unified CDC event stream combining MongoDB and MSSQL real-time changes",
    table_properties={
        "quality": "silver",
        "layer": "staging",
        "pattern": "streaming_cdc",
        "sources": "mongodb,mssql",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_users_staging_stream():
    """
    Unifies CDC events from MongoDB and MSSQL using UNION pattern.

    Pattern Explanation:
    - Read both CDC streams independently
    - Standardize schema for union compatibility
    - Tag events with source_system for lineage
    - Let apply_changes() downstream handle key-based merging

    Alternative Pattern (Stream-Stream Join):
    - More complex, requires watermarking and windows
    - Not recommended for CDC (events may not align in time)
    - Use apply_changes() key-based merging instead
    """

    # Read MongoDB CDC stream
    mongodb_stream = dlt.read_stream("bronze_mongodb_users_stream")

    # Read MSSQL CDC stream
    mssql_stream = dlt.read_stream("bronze_mssql_users_stream")

    # Standardize MongoDB events to unified schema
    # TYPE CASTING HAPPENS HERE (not in Bronze)
    mongodb_unified = mongodb_stream.select(
        # Business key and identifiers (WITH TYPE CASTING)
        col("cpf"),
        col("user_id").cast("bigint").alias("user_id"),  # STRING → BIGINT
        col("uuid"),

        # MongoDB-specific fields (delivery data)
        col("email"),
        col("delivery_address"),
        col("city"),

        # MSSQL fields (null for MongoDB events)
        lit(None).cast("string").alias("first_name"),
        lit(None).cast("string").alias("last_name"),
        lit(None).cast("date").alias("birthday"),
        lit(None).cast("string").alias("job"),
        lit(None).cast("string").alias("company_name"),

        # Common fields
        col("phone_number"),
        col("country"),

        # CDC metadata (WITH TYPE CASTING)
        col("operation"),
        col("sequenceNum").cast("bigint").alias("sequenceNum"),  # STRING → BIGINT
        col("dt_current_timestamp").cast("timestamp").alias("dt_current_timestamp"),  # STRING → TIMESTAMP

        # Source tracking
        lit("mongodb").alias("source_system"),
        col("ingestion_timestamp")
    ).filter(col("cpf").isNotNull())  # Data quality filter in Silver, not Bronze

    # Standardize MSSQL events to unified schema
    # TYPE CASTING HAPPENS HERE (not in Bronze)
    mssql_unified = mssql_stream.select(
        # Business key and identifiers (WITH TYPE CASTING)
        col("cpf"),
        col("user_id").cast("bigint").alias("user_id"),  # STRING → BIGINT
        col("uuid"),

        # MongoDB fields (null for MSSQL events)
        lit(None).cast("string").alias("email"),
        lit(None).cast("string").alias("delivery_address"),
        lit(None).cast("string").alias("city"),

        # MSSQL-specific fields (profile data, WITH TYPE CASTING)
        col("first_name"),
        col("last_name"),
        col("birthday").cast("date").alias("birthday"),  # STRING → DATE
        col("job"),
        col("company_name"),

        # Common fields
        col("phone_number"),
        col("country"),

        # CDC metadata (WITH TYPE CASTING)
        col("operation"),
        col("sequenceNum").cast("bigint").alias("sequenceNum"),  # STRING → BIGINT
        col("dt_current_timestamp").cast("timestamp").alias("dt_current_timestamp"),  # STRING → TIMESTAMP

        # Source tracking
        lit("mssql").alias("source_system"),
        col("ingestion_timestamp")
    ).filter(col("cpf").isNotNull())  # Data quality filter in Silver, not Bronze

    # Union both streams
    # apply_changes() downstream will merge events by cpf key
    unified_stream = mongodb_unified.union(mssql_unified)

    return unified_stream


# ============================================================================
# STREAMING TABLE: Latest State Per Key (Optional - for debugging)
# ============================================================================
# This table shows the most recent event per cpf from the staging stream
# Useful for debugging and validation, not used in production CDC flow

@dlt.table(
    name="silver_users_staging_latest",
    comment="Latest CDC event per user (debugging view) - deduped by cpf",
    table_properties={
        "quality": "silver",
        "layer": "staging",
        "use_case": "debugging",
        "delta.enableChangeDataFeed": "true"
    }
)
def silver_users_staging_latest():
    """
    Shows latest CDC event per user for debugging.
    Not required for CDC flow, but helpful for validation.
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    staging = dlt.read_stream("silver_users_staging_stream")

    # Deduplicate by cpf, keeping latest event
    window_spec = Window.partitionBy("cpf").orderBy(
        col("dt_current_timestamp").desc(),
        col("sequenceNum").desc()
    )

    return staging.withColumn("rn", row_number().over(window_spec)) \
                  .filter(col("rn") == 1) \
                  .drop("rn")


# ============================================================================
# PRODUCTION NOTES
# ============================================================================

"""
WHY UNION INSTEAD OF JOIN FOR CDC STREAMS?

1. Events Arrive Asynchronously:
   - MongoDB event for cpf=123 at T1
   - MSSQL event for cpf=123 at T5
   - Stream-stream join would require watermarking and buffering
   - Complex window management, potential data loss

2. apply_changes() Handles Merging:
   - Downstream apply_changes() merges by key (cpf)
   - Uses sequenceNum/timestamp for ordering
   - Automatically handles late arrivals
   - Simpler than stream-stream join

3. Partial Events Are OK:
   - MongoDB event with only email + address
   - MSSQL event with only name + job
   - apply_changes() merges them into complete profile
   - No need to wait for both events

STREAM PROCESSING GUARANTEES:
- Exactly-once semantics (Auto Loader + Delta)
- Event ordering preserved by sequenceNum
- Watermarking handled automatically by DLT
- Checkpointing for fault tolerance

MONITORING:
- Check streaming_metrics table in DLT
- Monitor lag between event time and processing time
- Alert if sequenceNum gaps detected
- Track DBU consumption in continuous mode

COST OPTIMIZATION:
- Use serverless for auto-scaling
- Consider triggered mode with intervals (not fully continuous)
- Monitor idle time between micro-batches
- Right-size cluster based on event rate
"""
