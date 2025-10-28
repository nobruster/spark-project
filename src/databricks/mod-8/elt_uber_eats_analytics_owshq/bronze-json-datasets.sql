CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT * 
FROM STREAM read_files('dbfs:/mnt/owshq-shadow-traffic/kafka/orders/', format => 'json');

CREATE OR REFRESH STREAMING TABLE bronze_status
AS SELECT * 
FROM STREAM read_files('dbfs:/mnt/owshq-shadow-traffic/kafka/status/', format => 'json');