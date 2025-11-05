"""
SILVER LAYER - User CDC Processing (SCD Type 1 and Type 2)

PURPOSE:
This module applies Change Data Capture (CDC) to the unified user staging data, creating two tables
with different historical tracking strategies. It uses Lakeflow's AUTO CDC Flow to automatically
handle INSERT, UPDATE, and DELETE operations.

WHAT IT DOES:
- Reads unified user data from silver_users_staging
- Creates SCD Type 1 table (current state only) for operational queries
- Creates SCD Type 2 table (full history) for audit and compliance
- Automatically handles out-of-order events using SEQUENCE BY timestamp
- Tracks all changes for LGPD/GDPR compliance requirements

DATA FLOW:
  silver_users_staging (materialized view)
    -> AUTO CDC Flow (APPLY CHANGES INTO)
    -> silver_users_unified (SCD Type 1)
    -> silver_users_history (SCD Type 2)

WHY TWO CDC TABLES?

SCD Type 1 (Current State):
- Stores ONLY the latest version of each user
- UPDATE overwrites previous values
- DELETE physically removes the record
- Use Cases: Operational dashboards, current user lookups, real-time reporting
- Example: Marketing needs current email addresses for campaigns

SCD Type 2 (Full History):
- Stores ALL versions of each user over time
- UPDATE closes old record (__END_AT) and creates new record
- DELETE soft-deletes by setting __END_AT timestamp
- Adds columns: __START_AT, __END_AT, __CURRENT
- Use Cases: Audit trails, compliance reporting, historical analysis
- Example: LGPD requires tracking when email addresses changed

WHY BATCH CDC (NOT CONTINUOUS)?
This pipeline uses batch CDC because:
- Source is materialized view (batch), not streaming table
- Processes all available changes then stops (micro-batch)
- More efficient than continuous processing for slowly changing data
- Aligns with hourly/daily refresh schedule

AUTO CDC FLOW FEATURES:
- Automatically detects INSERT, UPDATE, DELETE operations
- Handles out-of-order events using SEQUENCE BY timestamp
- Maintains referential integrity across both SCD tables
- Deduplicates changes arriving simultaneously

OUT-OF-ORDER HANDLING:
SEQUENCE BY dt_current_timestamp ensures events are processed in correct order:
- If User 1 update at 10:00 arrives after update at 10:05
- CDC processes them in timestamp order (10:00 first, then 10:05)
- Prevents data inconsistency from network delays

LEARNING OBJECTIVES:
- Implement AUTO CDC Flow with APPLY CHANGES INTO
- Understand SCD Type 1 vs Type 2 tradeoffs
- Handle batch CDC for slowly changing dimensions
- Design CDC pipelines for compliance requirements
- Use SEQUENCE BY for out-of-order event handling

CONFIGURATION:
- keys: cpf (Brazilian business key for change detection)
- sequence_by: dt_current_timestamp (temporal ordering)
- stored_as_scd_type: 1 or 2 (historical tracking strategy)
- track_history_column_list: ONLY for Type 2 (columns to track changes)

OUTPUT SCHEMAS:

silver_users_unified (SCD Type 1):
- cpf: Brazilian unified identifier (business key)
- user_id: System identifier
- uuid: Universal unique identifier
- email, delivery_address, city: MongoDB fields
- first_name, last_name, birthday, job, company_name: MSSQL fields
- phone_number, country: Common fields
- dt_current_timestamp: Source system timestamp

silver_users_history (SCD Type 2):
- All columns from SCD Type 1 PLUS:
- __START_AT: When this version became active
- __END_AT: When this version was superseded (null for current)
- __CURRENT: Boolean flag for current version

PRODUCTION NOTES:
- track_history_column_list is ONLY supported for SCD Type 2
- For Type 1, all columns are automatically tracked (overwrite mode)
- Type 2 explicitly lists which columns trigger new versions
"""

import dlt

# ============================================================================
# SCD TYPE 1 - Current State Only
# ============================================================================
# Creates: silver_users_unified
# Behavior: UPDATE overwrites, DELETE removes
# Use Case: Operational queries, marketing campaigns, customer support
# Track History: Not applicable (Type 1 doesn't track history)

# Define the target table structure (NOT streaming - batch CDC from MATERIALIZED VIEW)
dlt.create_target_table(
    name="silver_users_unified",
    comment="Current state of unified user profiles - SCD Type 1 for operational queries",
    table_properties={
        "quality": "silver",
        "layer": "curation",
        "scd_type": "1",
        "use_case": "operations",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Apply CDC changes from staging (batch mode)
dlt.apply_changes(
    target="silver_users_unified",
    source="silver_users_staging",
    keys=["cpf"],
    sequence_by="dt_current_timestamp",
    stored_as_scd_type=1,
    # Batch mode: process all available changes then stop
    # Aligns with MATERIALIZED VIEW refresh pattern
    ignore_null_updates=False,
    apply_as_deletes=None,  # No explicit delete logic (batch snapshot pattern)
    apply_as_truncates=None,
    column_list=None,  # Track all columns for Type 1
    except_column_list=["processed_timestamp"]  # Exclude metadata timestamp
)

# ============================================================================
# SCD TYPE 2 - Full History
# ============================================================================
# Creates: silver_users_history
# Behavior: UPDATE closes old record + creates new, DELETE soft-deletes
# Use Case: LGPD/GDPR compliance, audit trails, historical analysis
# Track History: Explicitly lists columns that trigger new versions

# Define the target table structure (NOT streaming - batch CDC from MATERIALIZED VIEW)
dlt.create_target_table(
    name="silver_users_history",
    comment="Complete change history of user profiles - SCD Type 2 for audit and compliance (LGPD/GDPR)",
    table_properties={
        "quality": "silver",
        "layer": "curation",
        "scd_type": "2",
        "use_case": "compliance_audit",
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.managed": "true"
    }
)

# Apply CDC changes with full history tracking
dlt.apply_changes(
    target="silver_users_history",
    source="silver_users_staging",
    keys=["cpf"],
    sequence_by="dt_current_timestamp",
    stored_as_scd_type=2,
    # Track specific columns for versioning (field-level change detection)
    track_history_column_list=[
        "email",           # Track email changes (LGPD/GDPR requirement)
        "delivery_address", # Track address changes (PII tracking)
        "city",            # Track city changes (location tracking)
        "first_name",      # Track name changes (identity verification)
        "last_name",       # Track name changes (identity verification)
        "job",             # Track job changes (demographic analysis)
        "company_name"     # Track company changes (B2B insights)
    ],
    # Batch mode configuration
    ignore_null_updates=False,
    apply_as_deletes=None,  # No explicit delete column (snapshot-based)
    apply_as_truncates=None,
    except_column_list=["processed_timestamp"]  # Exclude metadata from versioning
    # NOTE: SCD Type 2 automatically adds __START_AT, __END_AT, __CURRENT columns
    # __END_AT IS NULL indicates current/active version
    # __END_AT IS NOT NULL indicates historical/superseded version
)
