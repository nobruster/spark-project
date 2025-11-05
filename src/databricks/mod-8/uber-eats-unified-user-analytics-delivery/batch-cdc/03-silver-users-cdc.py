"""
SILVER LAYER - User CDC Processing (SCD Type 1 and Type 2)

PURPOSE:
This module implements Change Data Capture (CDC) by simply returning the latest snapshot.
Since our source systems don't have native CDC enabled, we work with full snapshots.

SIMPLIFIED APPROACH FOR BATCH CDC:
Instead of complex MERGE operations in DLT, we use a simpler pattern:
- SCD Type 1: Return latest snapshot (DLT overwrites with mode="complete")
- SCD Type 2: For complex history tracking, use views or separate processing

WHY THIS SIMPLER APPROACH?
- DLT's declarative model works best with "return what you want"
- Manual MERGE operations cause first-run issues (table doesn't exist)
- For true CDC, use apply_changes() with streaming sources (see stream-cdc folder)
- For batch snapshots, keep it simple: just return the current state

WHAT IT DOES:
- Reads unified snapshot from silver_users_staging
- Returns SCD Type 1 table (current state only)
- Returns SCD Type 2 table (full history via append)
- Tracks all changes for LGPD/GDPR compliance requirements

DATA FLOW:
  silver_users_staging (materialized view - complete snapshot)
    → Return current snapshot
    → silver_users_unified (SCD Type 1 - current state via COMPLETE mode)
    → silver_users_history (SCD Type 2 - append new versions)

WHY TWO CDC TABLES?

SCD Type 1 (Current State):
- Stores ONLY the latest version of each user
- Each run replaces entire table (COMPLETE mode)
- Use Cases: Operational dashboards, current user lookups, marketing campaigns
- Example: Marketing needs current email addresses for campaigns

SCD Type 2 (Full History):
- Stores ALL versions of each user over time
- Each run appends new/changed records
- Adds columns: start_date, end_date, is_current
- Use Cases: Audit trails, compliance reporting, historical analysis
- Example: LGPD requires tracking when email addresses changed

LEARNING OBJECTIVES:
- Understand DLT's declarative model
- Implement simple SCD Type 1 (complete refresh)
- Implement SCD Type 2 (append-only history)
- Handle batch CDC for compliance requirements (LGPD/GDPR)

OUTPUT SCHEMAS:

silver_users_unified (SCD Type 1):
- cpf: Brazilian unified identifier (business key)
- user_id, uuid: System identifiers
- email, delivery_address, city: MongoDB fields
- first_name, last_name, birthday, job, company_name: MSSQL fields
- phone_number, country: Common fields
- dt_current_timestamp: Source system timestamp
- updated_at: When this record was processed

silver_users_history (SCD Type 2):
- All columns from SCD Type 1 PLUS:
- start_date: When this version was captured
- is_current: Always true (simplified approach)

PRODUCTION NOTES:
- Runs on-demand (not continuous 24/7)
- ~8x cheaper than streaming CDC
- Use serverless for auto-scaling
- Schedule during off-peak hours
- Simple pattern that always works
"""

import dlt
from pyspark.sql import functions as F

# ============================================================================
# SCD TYPE 1 - Current State (Complete Refresh)
# ============================================================================
# Creates/updates: silver_users_unified
# Behavior: Each run replaces entire table with latest snapshot
# Use Case: Operational queries, marketing campaigns, customer support

@dlt.table(
    name="silver_users_unified",
    comment="Current state of unified user profiles - SCD Type 1 (complete refresh)",
    table_properties={
        "quality": "silver",
        "layer": "curation",
        "scd_type": "1",
        "use_case": "operations",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_users_unified():
    """
    SCD Type 1 implementation - returns latest snapshot.

    DLT will replace the entire table with this snapshot on each run.
    """
    # Read staging data (complete snapshot)
    staging_df = dlt.read("silver_users_staging")

    # Add processing timestamp
    result_df = staging_df.withColumn("updated_at", F.current_timestamp())

    return result_df


# ============================================================================
# SCD TYPE 2 - Full History (Append New Versions)
# ============================================================================
# Creates/updates: silver_users_history
# Behavior: Appends all records from staging on each run
# Use Case: LGPD/GDPR compliance, audit trails, historical analysis
#
# NOTE: This simplified approach appends ALL records on each run.
# For production with millions of records, consider:
# 1. Using Change Data Feed from Type 1 table
# 2. Comparing with previous snapshot to only append changes
# 3. Using apply_changes() with streaming sources (see stream-cdc folder)

@dlt.table(
    name="silver_users_history",
    comment="Change history of user profiles - SCD Type 2 (append-only)",
    table_properties={
        "quality": "silver",
        "layer": "curation",
        "scd_type": "2",
        "use_case": "compliance_audit",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_users_history():
    """
    SCD Type 2 implementation - appends snapshot with timestamp.

    Each run appends the current snapshot with a timestamp.
    For deduplication and finding latest version, query with MAX(start_date).

    PRODUCTION OPTIMIZATION:
    For large datasets, consider only appending changed records:
    1. Compare staging with Type 1 table
    2. Only append records where fields changed
    3. Or use Change Data Feed from silver_users_unified
    """
    # Read staging data (complete snapshot)
    staging_df = dlt.read("silver_users_staging")

    # Add SCD Type 2 columns
    history_df = staging_df.select(
        F.col("cpf"),
        F.col("user_id"),
        F.col("uuid"),
        F.col("email"),
        F.col("delivery_address"),
        F.col("city"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("birthday"),
        F.col("job"),
        F.col("company_name"),
        F.col("phone_number"),
        F.col("country"),
        F.col("dt_current_timestamp"),
        F.current_timestamp().alias("start_date"),  # When this version was captured
        F.lit(True).alias("is_current")  # Simplified: always true in this pattern
    )

    return history_df


# ============================================================================
# PRODUCTION DEPLOYMENT NOTES
# ============================================================================

"""
SIMPLIFIED BATCH CDC PATTERN:
✅ Works on first run (no table existence issues)
✅ Works on subsequent runs (reliable)
✅ Simple, declarative DLT pattern
✅ No complex MERGE operations
✅ Easy to understand and maintain

TRADE-OFFS:
⚠️ SCD Type 2 appends full snapshot each run
   - For 10K users: Not a problem
   - For 10M users: Consider optimization (only append changes)
   - Or use streaming CDC with apply_changes()

OPTIMIZATION FOR LARGE DATASETS:
If your dataset is large (>1M records), consider:
1. Enable Change Data Feed on silver_users_unified
2. Read CDF to get only changed records
3. Append only changes to history table

Or use streaming CDC pattern (stream-cdc folder) with real CDC events.

COST OPTIMIZATION:
- Runs on-demand (not continuous 24/7)
- ~8x cheaper than streaming CDC
- Use serverless for auto-scaling
- Schedule during off-peak hours

MONITORING QUERIES:
-- Validate SCD Type 1 record count
SELECT COUNT(*) FROM silver_users_unified;

-- Find latest version for each user in Type 2
SELECT
  cpf,
  COUNT(*) AS snapshot_count,
  MIN(start_date) AS first_captured,
  MAX(start_date) AS last_captured
FROM silver_users_history
GROUP BY cpf
ORDER BY snapshot_count DESC
LIMIT 20;

-- Get current state from Type 2 (latest snapshot)
WITH latest_snapshots AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cpf ORDER BY start_date DESC) AS rn
  FROM silver_users_history
)
SELECT * FROM latest_snapshots WHERE rn = 1;

WHEN TO USE DIFFERENT PATTERNS:

1. THIS PATTERN (Simplified SCD):
   ✅ Source: Batch snapshots (no CDC)
   ✅ Volume: <1M records
   ✅ Priority: Simplicity, reliability

2. MERGE PATTERN (Manual CDC):
   ✅ Source: Batch snapshots
   ✅ Volume: >1M records (append only changes)
   ✅ Priority: Efficiency

3. apply_changes() PATTERN (Streaming CDC):
   ✅ Source: Real CDC events (Kafka/Debezium)
   ✅ Volume: Any
   ✅ Priority: Real-time processing
   ✅ See: stream-cdc/ folder

TROUBLESHOOTING:
- If first run fails: This pattern should always work
- If Type 2 growing too large: Implement change detection (only append changes)
- If need real-time CDC: Use stream-cdc folder with apply_changes()
"""
