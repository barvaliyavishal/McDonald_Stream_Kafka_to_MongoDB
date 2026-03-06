-- Mock data
{
  "order_id" : "od_001",
  "customer_id" : "cust_001",
  "item" : "Shirt",
  "amount" : 100.5
}

-- Create stream for specific kafka topic
CREATE STREAM orders_raw_stream
( 
  order_id STRING,
  customer_id STRING,
  item STRING,
  amount DOUBLE
) WITH (
  KAFKA_TOPIC='orders_raw',
  VALUE_FORMAT='JSON'
);

-- Example for background running query
SELECT * FROM orders_raw_stream EMIT CHANGES;

-- Create stream from query as example of persistant query
-- This will write data in default created topic
CREATE STREAM orders_preprocessed AS
SELECT * FROM orders_raw_stream WHERE item = 'Shirt' EMIT CHANGES;

-- this one will write data in user defined topic
CREATE STREAM orders_preprocessed_new
WITH (KAFKA_TOPIC='my_filtered_orders', VALUE_FORMAT='JSON') AS
SELECT * 
FROM orders_raw_stream 
WHERE item = 'Shirt' 
EMIT CHANGES;

-- Create table
CREATE TABLE item_aggregation AS 
SELECT 
item,
count(*) as total_count
FROM orders_raw_stream GROUP BY item;

-- Delete stream
DROP STREAM orders_filtered;