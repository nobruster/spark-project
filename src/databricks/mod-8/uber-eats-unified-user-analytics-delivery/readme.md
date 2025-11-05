# UberEats Unified User Analytics - CDC Implementations

## 📚 Overview

This directory contains **two CDC (Change Data Capture) implementation patterns** for the UberEats unified user analytics pipeline, demonstrating different approaches to tracking data changes in Databricks Delta Live Tables.

---

## 🔄 CDC Pattern Comparison

### Pattern 1: Batch CDC (Snapshot-based) ✅ **IMPLEMENTED**
- **Location:** [`batch-cdc/`](batch-cdc/)
- **Method:** `dlt.apply_changes()` with MATERIALIZED VIEWS
- **Use Case:** Snapshot-based change detection from periodic exports
- **Status:** ✅ Production-ready

### Pattern 2: Streaming CDC (Event-based) 🚧 **PLANNED**
- **Location:** [`stream-cdc/`](stream-cdc/)
- **Method:** `dlt.read_stream()` with Change Data Feed
- **Use Case:** Real-time CDC events from streaming sources
- **Status:** 🚧 Template/documentation only

---

## 📊 When to Use Each Pattern

| Criteria | Batch CDC | Streaming CDC |
|----------|-----------|---------------|
| **Source Data** | Full snapshots (JSON/CSV exports) | CDC events (Debezium, CDC logs) |
| **Update Frequency** | Hourly/Daily | Real-time (seconds) |
| **Data Volume** | < 10M records/refresh | High-volume transactional |
| **Latency** | Minutes to hours | Seconds |
| **Cost** | ~$50/month | ~$400/month |
| **Complexity** | Lower | Higher |
| **Infrastructure** | File storage | Kafka/Event Hub + CDC connectors |
| **Best For** | User profiles, products, customers | Transactions, IoT, fraud detection |

---

## 🎯 Quick Decision Matrix

**Choose Batch CDC if:**
- ✅ Source systems export full snapshots periodically
- ✅ Data changes slowly (user profiles, product catalogs)
- ✅ Hourly/daily freshness is acceptable
- ✅ Cost optimization is priority
- ✅ Simpler implementation is preferred

**Choose Streaming CDC if:**
- ✅ Source systems can produce CDC events
- ✅ Sub-second latency is required
- ✅ High-volume transactional data
- ✅ Event-driven architecture
- ✅ Budget allows 8x higher compute costs

---

## 📁 Directory Structure

```
uber-eats-unified-user-analytics-delivery/
│
├── batch-cdc/                              ✅ Production Implementation
│   ├── 01-bronze-mongodb-users.sql        # Snapshot ingestion
│   ├── 01-bronze-mssql-users.sql          # Snapshot ingestion
│   ├── 02-silver-users-staging.sql        # FULL OUTER JOIN
│   ├── 03-silver-users-cdc.py             # apply_changes() - Batch CDC
│   ├── 04-gold-*.sql                      # Analytics tables
│   ├── databricks.yml                     # Pipeline config
│   ├── QUICKSTART.md                      # Quick start guide
│   └── README.md                          # Batch CDC docs
│
├── stream-cdc/                             🚧 Future Implementation
│   └── README.md                          # Streaming CDC docs & templates
│
├── readme.md                               📄 This file (main overview)
└── use-case.md                            📄 Business use case details
```

---

## 🚀 Getting Started

### For Current Implementation (Batch CDC):

```bash
# Navigate to batch CDC implementation
cd batch-cdc/

# Review quick start guide
cat QUICKSTART.md

# Deploy the pipeline
databricks bundle validate
databricks bundle deploy --target production
databricks bundle run uber_eats_user_pipeline
```

See [`batch-cdc/QUICKSTART.md`](batch-cdc/QUICKSTART.md) for detailed deployment instructions.

### For Future Streaming Implementation:

See [`stream-cdc/README.md`](stream-cdc/README.md) for requirements and architecture planning.

---

## 🏗️ Architecture Comparison

### Batch CDC Architecture (Current)
```
MongoDB/MSSQL (Full Exports)
    ↓ Hourly/Daily dumps to Blob Storage
Bronze Layer (MATERIALIZED VIEWS)
    ↓ read_files() - Batch ingestion
Silver Staging (MATERIALIZED VIEW)
    ↓ FULL OUTER JOIN
Silver CDC (create_target_table)
    ↓ apply_changes() compares snapshots
    ├─ SCD Type 1 (current state)
    └─ SCD Type 2 (full history)
Gold Layer (Analytics)
```

### Streaming CDC Architecture (Future)
```
MongoDB/MSSQL (CDC Enabled)
    ↓ Change events to Kafka
Bronze Layer (STREAMING TABLES)
    ↓ read_stream() - Real-time
Silver Staging (STREAMING TABLE)
    ↓ Stream-stream join
Silver CDC (create_streaming_table)
    ↓ apply_changes() from CDC stream
    ├─ SCD Type 1 (real-time)
    └─ SCD Type 2 (real-time history)
Gold Layer (Real-time analytics)
```

---

## 💡 Key Technical Differences

### Batch CDC Implementation
```python
# Bronze: MATERIALIZED VIEW (snapshot)
CREATE OR REFRESH MATERIALIZED VIEW bronze_mongodb_users AS
SELECT * FROM read_files('path/*.json')

# CDC: create_target_table (batch)
dlt.create_target_table(name="silver_users_unified")
dlt.apply_changes(
    target="silver_users_unified",
    source="silver_users_staging",  # MATERIALIZED VIEW
    stored_as_scd_type=1
)
```

### Streaming CDC Implementation
```python
# Bronze: STREAMING TABLE (CDC events)
CREATE OR REFRESH STREAMING TABLE bronze_mongodb_cdc AS
SELECT * FROM cloud_files('path/cdc-events/', 'json')

# CDC: create_streaming_table (streaming)
dlt.create_streaming_table(name="silver_users_unified")
dlt.apply_changes(
    target="silver_users_unified",
    source=dlt.read_stream("silver_users_staging"),  # STREAMING
    stored_as_scd_type=1
)
```

---

## 📖 Business Use Case

**Problem:** UberEats has user data fragmented across:
- **MongoDB:** Operational data (email, delivery address, city)
- **MSSQL:** CRM data (name, birthday, job, company)

**Solution:** Unified user domain with complete profiles and change tracking

**Business Value:**
- 📈 Marketing: 95% profile completeness (vs 50% before)
- 🎯 Customer Support: 30-second resolution (vs 5 minutes)
- ⚖️ Compliance: 1-hour DSAR response (vs 2 weeks)
- 📊 Analytics: 85% data quality score (vs 60%)

See [`use-case.md`](use-case.md) for detailed business context.

---

## 🎓 Learning Outcomes

By studying both implementations, you'll understand:

### Batch CDC (Current):
- ✅ Snapshot-based CDC with `apply_changes()`
- ✅ `create_target_table()` for batch processing
- ✅ MATERIALIZED VIEW pattern
- ✅ Cost optimization strategies
- ✅ SCD Type 1 & Type 2 implementation
- ✅ FULL OUTER JOIN for multi-source unification

### Streaming CDC (Future):
- 🔄 Event-based CDC processing
- 🔄 `create_streaming_table()` for real-time
- 🔄 Change Data Feed consumption
- 🔄 Auto Loader for incremental ingestion
- 🔄 Stream-stream joins
- 🔄 Continuous pipeline patterns

---

## 💰 Cost Analysis

### Monthly Compute Costs (Azure Databricks)

**Batch CDC (Current Implementation):**
- Execution: 2 hours/day × 30 days = 60 hours/month
- Serverless DBUs: ~100 DBUs/month
- Cost: ~$50/month ✅

**Streaming CDC (If Implemented):**
- Execution: 24 hours/day × 30 days = 720 hours/month
- Serverless DBUs: ~800 DBUs/month
- Cost: ~$400/month ⚠️

**Trade-off:** Real-time capability = 8x cost increase

For user profile data that changes slowly, **Batch CDC provides 60-80% cost savings** with acceptable latency.

---

## 🔧 Migration Path

If business needs evolve from batch to streaming:

1. **Enable CDC on sources** (MongoDB change streams, MSSQL CDC)
2. **Set up streaming infrastructure** (Kafka, Event Hub, Debezium)
3. **Convert Bronze to STREAMING TABLES**
4. **Update CDC to use `create_streaming_table()`**
5. **Enable continuous mode** in pipeline config
6. **Monitor costs** and performance

---

## 📚 Documentation Links

### Databricks Resources:
- [Batch CDC (apply_changes)](https://docs.databricks.com/aws/en/ldp/what-is-change-data-capture.html)
- [Streaming CDC (CDF)](https://docs.databricks.com/aws/en/ldp/cdc?language=Python)
- [Change Data Feed](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)

### CDC Technologies:
- [Debezium](https://debezium.io/)
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [SQL Server CDC](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server)

---

## 🤝 Support & Contributions

- **Current Implementation:** See [`batch-cdc/QUICKSTART.md`](batch-cdc/QUICKSTART.md)
- **Questions:** Review README files in each folder
- **Issues:** Check troubleshooting sections in documentation

---

## 🎯 Recommendation

**For UberEats user analytics use case:**
- ✅ Use **Batch CDC** (current implementation)
- ✅ User profiles change slowly (perfect for hourly/daily refresh)
- ✅ 60-80% cost savings vs streaming
- ✅ Production-ready, fully tested

**When to consider Streaming CDC:**
- Real-time fraud detection is added
- Order processing needs sub-second updates
- Business can justify 8x higher costs
- CDC infrastructure is already in place

---

**Choose the right pattern for your data velocity and business needs! 🚀**
