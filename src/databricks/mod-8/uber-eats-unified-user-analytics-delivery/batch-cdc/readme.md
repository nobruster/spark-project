# Batch CDC Implementation (Snapshot-based Auto CDC)

## 📋 What's in This Folder

This folder contains the **Batch CDC** implementation using `dlt.apply_changes()` for snapshot-based change detection.

## 🔄 CDC Pattern: Snapshot Comparison

**How it works:**
1. Source systems export **full snapshots** to blob storage (hourly/daily)
2. Bronze layer ingests complete snapshots as MATERIALIZED VIEWS
3. `dlt.apply_changes()` compares current snapshot with previous to detect changes
4. MERGE INTO logic (behind the scenes) identifies INSERT/UPDATE/DELETE

**Key Characteristic:** You have **full data dumps**, not individual CDC events.

---

## 📁 Files in This Implementation

| File | Layer | Purpose |
|------|-------|---------|
| `01-bronze-mongodb-users.sql` | Bronze | Raw MongoDB snapshot ingestion |
| `01-bronze-mssql-users.sql` | Bronze | Raw MSSQL snapshot ingestion |
| `02-silver-users-staging.sql` | Silver | FULL OUTER JOIN unification |
| `03-silver-users-cdc.py` | Silver | **Batch CDC with apply_changes()** |
| `04-gold-user-analytics.sql` | Gold | **Unified analytics (Marketing + BI + Compliance)** |
| `analysis-cdc-updates.sql` | Analysis | CDC validation queries |
| `databricks.yml` | Config | Pipeline orchestration |
| `QUICKSTART.md` | Docs | Quick start guide |

---

## 🎯 When to Use Batch CDC

✅ **Use Batch CDC when you have:**
- Full snapshots exported from source systems (JSON, CSV, Parquet dumps)
- Hourly/daily data refreshes (not real-time)
- Slowly changing dimension data (users, products, customers)
- Lower volume data (<10M records per refresh)
- Cost optimization priority (batch is 60-80% cheaper)

❌ **Don't use Batch CDC when:**
- You have true CDC streams (Debezium, SQL Server CDC, Oracle GoldenGate)
- You need sub-second latency
- You have high-volume transactional data
- Source systems can send only changed records

---

## 🏗️ Architecture

```
SOURCE SYSTEMS (MongoDB, MSSQL)
    ↓ Export full snapshots hourly
AZURE BLOB STORAGE
    ↓ read_files()
🟤 BRONZE - Materialized Views (full snapshots)
    ↓ FULL OUTER JOIN
🥈 SILVER STAGING - Unified snapshot
    ↓ dlt.apply_changes() - Compares snapshots
🥈 SILVER CDC - SCD Type 1 & Type 2
    ↓ Aggregations
🥇 GOLD - Unified Analytics (1 comprehensive table)
```

---

## 🥇 Gold Layer: Unified Analytics Approach

Instead of creating multiple Gold tables (one per use case), we've consolidated into **one comprehensive analytics table** that serves all teams:

```sql
CREATE OR REFRESH MATERIALIZED VIEW gold_user_analytics AS
WITH user_demographics AS (
  -- Calculate age groups, job categories, profile completeness
  SELECT * FROM silver_users_unified  -- SCD Type 1 (current state)
),
change_metrics AS (
  -- Calculate change frequency for compliance
  SELECT * FROM silver_users_history  -- SCD Type 2 (full history)
),
city_analytics AS (
  -- Aggregate by city and age_group with all metrics
  SELECT
    city,
    age_group,
    user_count,                    -- Marketing
    avg_age,                       -- Marketing
    profile_completeness_pct,      -- BI
    data_quality_grade,            -- BI
    top_job_category,              -- BI
    avg_changes_per_user,          -- Compliance
    high_activity_users            -- Compliance
  FROM ...
)
```

**Benefits:**
- ✅ **Simpler queries** - All metrics in one place
- ✅ **Lower cost** - 1 table vs 3 tables to refresh
- ✅ **Better performance** - Single aggregation pass
- ✅ **Easier maintenance** - One table to monitor

**Serves Multiple Teams:**
- **Marketing**: `user_count`, `age_group` for campaign targeting
- **BI**: `profile_completeness_pct`, `data_quality_grade` for dashboards
- **Compliance**: `avg_changes_per_user`, `high_activity_users` for audit

---

## 🔧 Key Implementation Details

### Bronze Layer (Batch Ingestion)
```sql
CREATE OR REFRESH MATERIALIZED VIEW bronze_mongodb_users AS
SELECT *
FROM read_files('path/to/snapshots/*.json', format := 'json');
```
- **Pattern**: MATERIALIZED VIEW (not streaming)
- **Refresh**: On-demand or scheduled
- **Cost**: Lower (batch processing)

### Silver CDC (Batch CDC)
```python
dlt.create_target_table(name="silver_users_unified")  # NOT create_streaming_table!

dlt.apply_changes(
    target="silver_users_unified",
    source="silver_users_staging",  # MATERIALIZED VIEW
    keys=["cpf"],
    sequence_by="dt_current_timestamp",
    stored_as_scd_type=1  # Integer, not string!
)
```

**Critical Differences from Streaming CDC:**
- ✅ `create_target_table()` (batch) instead of `create_streaming_table()`
- ✅ Source is MATERIALIZED VIEW (snapshot)
- ✅ No `dlt.read_stream()` - reads from batch table
- ✅ Triggered mode (not continuous)

---

## 📊 How Changes Are Detected

`dlt.apply_changes()` uses **sequence_by** to order events and detect changes:

```python
sequence_by="dt_current_timestamp"  # Must be TIMESTAMP type
```

**Example:**
```
Snapshot 1 (2025-01-01 10:00):
  cpf: 123, email: old@email.com, timestamp: 2025-01-01 09:00

Snapshot 2 (2025-01-01 11:00):
  cpf: 123, email: new@email.com, timestamp: 2025-01-01 10:30

apply_changes() detects:
- Email changed from old@email.com → new@email.com
- Uses timestamp to order events correctly
- Creates new version in SCD Type 2
```

---

## 💰 Cost Comparison

| Aspect | Batch CDC | Streaming CDC |
|--------|-----------|---------------|
| **Compute** | On-demand (1 hour/day) | Continuous (24 hours/day) |
| **DBU Cost** | ~$50/month | ~$400/month |
| **Latency** | Minutes to hours | Seconds |
| **Use Case** | Slowly changing data | Real-time events |

**For user profile data:** Batch CDC saves 60-80% vs streaming.

---

## 🚀 Deployment

See [QUICKSTART.md](QUICKSTART.md) for detailed deployment instructions.

**Quick Deploy:**
```bash
cd batch-cdc
databricks bundle validate
databricks bundle deploy --target production
```

---

## 📈 Performance Optimization

1. **Partition Bronze tables** by date if data volume is high:
   ```sql
   PARTITION BY (DATE(dt_current_timestamp))
   ```

2. **Z-Order by business key:**
   ```sql
   OPTIMIZE bronze_mongodb_users ZORDER BY (cpf);
   ```

3. **Use serverless compute** for auto-scaling

4. **Schedule refreshes** during off-peak hours

---

## 🎓 Key Learnings

This implementation teaches:
- ✅ Batch CDC with `apply_changes()` for snapshots
- ✅ `create_target_table()` vs `create_streaming_table()`
- ✅ SCD Type 1 (current state) vs Type 2 (full history)
- ✅ FULL OUTER JOIN for multi-source unification
- ✅ Deduplication with `ROW_NUMBER()`
- ✅ Cost optimization with batch processing
- ✅ LGPD/GDPR compliance with audit trails

---

## 📚 Related Documentation

- [Databricks Auto CDC Documentation](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture.html)
- [apply_changes() API Reference](https://docs.databricks.com/delta-live-tables/cdc.html)
- [SCD Type 2 Implementation](https://docs.databricks.com/delta-live-tables/cdc.html#scd-type-2)

---

**This is the production implementation - fully tested and ready to deploy! ✅**
