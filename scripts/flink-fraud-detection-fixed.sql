-- ============================================
-- Flink SQL Fraud Detection Statements (FIXED)
-- For Confluent Cloud Flink SQL
-- Run each statement ONE AT A TIME
-- ============================================

-- Step 1: Create table for input orders
-- This reads from the postgres-transactions topic
CREATE TABLE `postgres-transactions` (
    orderid INT,
    ordertime BIGINT,
    itemid STRING,
    orderunits DOUBLE,
    address ROW<
        city STRING,
        state STRING,
        zipcode BIGINT
    >,
    event_time AS TO_TIMESTAMP_LTZ(ordertime, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECONDS
) WITH (
    'value.format' = 'json-registry'
);

-- Step 2: Create table for fraud alerts output
-- This writes to the fraud-alerts-flink topic
CREATE TABLE `fraud-alerts-flink` (
    alert_id STRING,
    alert_timestamp TIMESTAMP(3),
    orderid INT,
    itemid STRING,
    amount DOUBLE,
    city STRING,
    state STRING,
    zipcode BIGINT,
    fraud_score INT,
    decision STRING,
    reason STRING
);

-- Step 3: Verify tables created
-- SHOW TABLES;

-- Step 4: Test - View sample orders
-- SELECT * FROM orders LIMIT 10;

-- Step 5: Start fraud detection (runs continuously)
INSERT INTO `fraud-alerts-flink`
SELECT
    CONCAT('flink_alert_', CAST(orderid AS STRING), '_', CAST(UNIX_TIMESTAMP() AS STRING)) as alert_id,
    CURRENT_TIMESTAMP as alert_timestamp,
    orderid,
    itemid,
    orderunits as amount,
    address.city as city,
    address.state as state,
    address.zipcode as zipcode,
    CASE
        WHEN orderunits > 500 THEN 80
        WHEN orderunits > 200 THEN 50
        ELSE 20
    END as fraud_score,
    CASE
        WHEN orderunits > 500 THEN 'BLOCK'
        WHEN orderunits > 200 THEN 'REVIEW'
        ELSE 'APPROVE'
    END as decision,
    CASE
        WHEN orderunits > 500 THEN 'High order value detected by Flink'
        WHEN orderunits > 200 THEN 'Medium-high order value detected by Flink'
        ELSE 'Normal order'
    END as reason
FROM `postgres-transactions`
WHERE orderunits > 200;

-- Step 6: View fraud alerts (run in new SQL tab)
-- SELECT * FROM `fraud-alerts-flink`;

-- Step 7: View statistics (run in new SQL tab)
-- SELECT decision, COUNT(*) as alert_count, AVG(amount) as avg_amount, MAX(amount) as max_amount
-- FROM `fraud-alerts-flink`
-- GROUP BY decision;

-- Made with Bob
