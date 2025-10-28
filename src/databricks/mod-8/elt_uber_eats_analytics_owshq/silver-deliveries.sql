CREATE OR REFRESH STREAMING LIVE TABLE silver_deliveries(
  -- Primary key constraint
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,

  -- Date validation
  CONSTRAINT valid_order_date EXPECT (order_date IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT reasonable_order_date EXPECT (order_date >= '2020-01-01' AND order_date <= current_timestamp()) ON VIOLATION DROP ROW,

  -- Foreign key constraints
  CONSTRAINT valid_restaurant_key EXPECT (restaurant_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_driver_key EXPECT (driver_key IS NOT NULL) ON VIOLATION DROP ROW,

  -- Business logic validation
  CONSTRAINT valid_total_amount EXPECT (total_amount IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount EXPECT (total_amount > 0) ON VIOLATION DROP ROW,
  CONSTRAINT reasonable_amount EXPECT (total_amount < 10000) ON VIOLATION WARN,

  -- Status validation
  CONSTRAINT valid_status EXPECT (status_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT known_status EXPECT (status_name IN ('pending', 'confirmed', 'preparing', 'out_for_delivery', 'delivered', 'cancelled')) ON VIOLATION WARN
)
COMMENT "Silver layer: Cleaned delivery data with quality checks applied"
TBLPROPERTIES (
  "quality" = "silver",
  "pipeline.layer" = "silver"
)
AS
SELECT
  o.order_id,
  CAST(o.order_date AS TIMESTAMP) AS order_date,
  o.restaurant_key,
  o.driver_key,
  CAST(o.total_amount AS FLOAT) AS total_amount,
  s.status.status_name AS status_name
FROM STREAM(LIVE.bronze_orders) o
JOIN STREAM(LIVE.bronze_status) s
ON o.order_id = s.order_identifier;