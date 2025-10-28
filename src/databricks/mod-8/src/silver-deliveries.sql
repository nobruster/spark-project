CREATE OR REFRESH STREAMING LIVE TABLE silver_deliveries
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