# Streaming CDC Implementation (Change Data Feed based)

## 📋 What Will Go in This Folder

This folder is prepared for a **Streaming CDC** implementation using Change Data Feed (CDF) from Delta tables or CDC streams from source systems.

## 🔄 CDC Pattern: True CDC Streams

**How it works:**
1. Source systems produce **CDC events** (INSERT/UPDATE/DELETE operations)
2. Events are streamed to Delta tables with Change Data Feed enabled
3. `dlt.read_stream()` reads the CDC events in real-time
4. Process only changed records (not full snapshots)

**Key Characteristic:** You have **individual change events**, not full snapshots.

---

## 🎯 When to Use Streaming CDC

✅ **Use Streaming CDC when you have:**
- True CDC streams from source systems:
  - Debezium capturing MongoDB oplog
  - SQL Server CDC enabled
  - Oracle GoldenGate
  - Kafka CDC topics
- Delta tables with Change Data Feed enabled
- Need for real-time processing (seconds latency)
- High-volume transactional data
- Event-driven architecture

❌ **Don't use Streaming CDC when:**
- You only have periodic snapshots/exports
- Data changes slowly (hourly/daily updates)
- Cost optimization is priority
- Source systems can't produce CDC events

---

## 🏗️ Architecture (Planned)

```
SOURCE SYSTEMS (MongoDB, MSSQL with CDC enabled)
    ↓ CDC Events (oplog, transaction log)
KAFKA / EVENT HUB
    ↓ Auto Loader / read_stream()
🟤 BRONZE - Streaming Tables (CDC events)
    ↓ Stream processing
🥈 SILVER STAGING - Unified CDC stream
    ↓ read_stream() from CDF
🥈 SILVER CDC - SCD Type 1 & Type 2 (streaming)
    ↓ Stream aggregations
🥇 GOLD - Real-time analytics
```

---

## 📝 Example Implementation Pattern

### Bronze Layer (Streaming Ingestion)

```sql
-- Would use STREAMING TABLE instead of MATERIALIZED VIEW
CREATE OR REFRESH STREAMING TABLE bronze_mongodb_cdc_stream
COMMENT 'Real-time CDC events from MongoDB'
TBLPROPERTIES (
  'quality' = 'bronze',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,
  current_timestamp() AS ingestion_timestamp
FROM cloud_files(
  'abfss://path/to/cdc/events/',
  'json',
  map('cloudFiles.schemaLocation', '/schemas/mongodb_cdc')
);
```

### Silver CDC (Streaming CDC)

```python
import dlt

# Read CDC stream from Bronze
@dlt.table(
    name="silver_users_staging_stream",
    comment="Unified CDC stream"
)
def staging_stream():
    mongo_stream = dlt.read_stream("bronze_mongodb_cdc_stream")
    mssql_stream = dlt.read_stream("bronze_mssql_cdc_stream")

    # Stream-stream join or union logic
    return unified_stream

# Apply changes from streaming source
dlt.create_streaming_table(name="silver_users_unified")

dlt.apply_changes(
    target="silver_users_unified",
    source="silver_users_staging_stream",  # STREAMING source
    keys=["cpf"],
    sequence_by="event_timestamp",
    stored_as_scd_type=1
)
```

**Critical Differences from Batch CDC:**
- ✅ `create_streaming_table()` (streaming) instead of `create_target_table()`
- ✅ Source is STREAMING TABLE or `read_stream()`
- ✅ `dlt.read_stream()` for continuous processing
- ✅ Continuous mode (always running)
- ✅ Auto Loader for file-based CDC

---

## 🔧 Key Components Needed

To implement streaming CDC, you would need:

1. **CDC-Enabled Sources:**
   - MongoDB with oplog/change streams
   - MSSQL with CDC or Change Tracking enabled
   - Debezium connectors
   - Kafka topics with CDC events

2. **Streaming Infrastructure:**
   - Azure Event Hub or Kafka
   - Auto Loader for file-based CDC
   - Delta tables with CDF enabled

3. **DLT Pipeline Configuration:**
   ```yaml
   continuous: true  # NOT false like batch CDC
   serverless: true
   edition: "ADVANCED"
   ```

4. **Stream Processing:**
   ```python
   # Read from CDC stream
   dlt.read_stream("table_with_cdf_enabled")

   # Enable CDF on output
   table_properties={"delta.enableChangeDataFeed": "true"}
   ```

---

## 💰 Cost Considerations

| Aspect | Streaming CDC | Batch CDC |
|--------|---------------|-----------|
| **Compute** | Continuous (24/7) | On-demand (1-2 hrs/day) |
| **DBU Cost** | ~$400/month | ~$50/month |
| **Latency** | Seconds | Minutes to hours |
| **Complexity** | Higher | Lower |
| **Infrastructure** | CDC connectors, Kafka | File exports |

**Trade-off:** Real-time capability vs 8x higher cost.

---

## 📊 When This Pattern Makes Sense

### Perfect Use Cases:
- **Fraud detection:** Real-time user behavior analysis
- **Inventory systems:** Stock levels must update instantly
- **Financial transactions:** Account balances in real-time
- **IoT data:** Sensor readings streaming continuously

### Not Ideal Use Cases:
- **User profiles:** Change infrequently (batch is better)
- **Product catalogs:** Update daily (batch is sufficient)
- **Marketing segments:** Recalculated hourly (batch is fine)

---

## 🔄 Migration Path: Batch → Streaming

If you need to migrate from batch to streaming CDC:

1. **Enable CDC on source systems:**
   ```sql
   -- MSSQL example
   EXEC sys.sp_cdc_enable_table
       @source_schema = 'dbo',
       @source_name = 'users',
       @role_name = NULL;
   ```

2. **Set up CDC pipeline:**
   - Deploy Debezium connectors
   - Configure Kafka topics
   - Enable Change Data Feed on Bronze tables

3. **Convert Bronze to streaming:**
   ```sql
   -- Change from MATERIALIZED VIEW to STREAMING TABLE
   CREATE OR REFRESH STREAMING TABLE bronze_mongodb_users
   ```

4. **Update CDC layer:**
   ```python
   # Change from create_target_table to create_streaming_table
   dlt.create_streaming_table(name="silver_users_unified")
   ```

5. **Enable continuous mode:**
   ```yaml
   continuous: true  # In databricks.yml
   ```

---

## 🎓 What You'll Learn (Future Implementation)

When streaming CDC is implemented here, you'll understand:
- 🔄 Real-time CDC event processing
- 📡 Auto Loader for incremental ingestion
- 🌊 Stream-stream joins in DLT
- ⚡ Change Data Feed consumption
- 🔧 Continuous vs triggered pipelines
- 💰 Cost trade-offs: real-time vs batch
- 🏗️ CDC infrastructure setup (Debezium, Kafka)

---

## 📚 Reference Documentation

- [Databricks Streaming CDC](https://docs.databricks.com/aws/en/ldp/cdc?language=Python)
- [Change Data Feed](https://docs.databricks.com/delta/delta-change-data-feed.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Debezium Connectors](https://debezium.io/documentation/reference/connectors/)

---

## 📝 Current Status

**This folder is currently empty** - it's prepared for future streaming CDC implementation when:
1. Business requirements demand real-time processing
2. Cost-benefit analysis justifies 8x higher compute costs
3. Source systems are configured to produce CDC events
4. Streaming infrastructure (Kafka/Event Hub) is in place

For now, **use the `batch-cdc/` implementation** which handles snapshot-based CDC efficiently.

---

**Future implementation coming soon! For current needs, use `../batch-cdc/`** 🚀
