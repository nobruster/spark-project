CREATE OR REFRESH LIVE TABLE gold_kpis
AS
SELECT
  DATE(order_date) AS date,
  status_name,
  COUNT(*) AS total_orders,
  SUM(total_amount) AS revenue,
  AVG(total_amount) AS avg_ticket
FROM LIVE.silver_deliveries
GROUP BY DATE(order_date), status_name;