-- ============================================
-- Flink SQL Fraud Detection Statements
-- Copy and paste these into Confluent Cloud Flink SQL Workspace
-- ============================================

-- Step 1: Create table for input orders
-- This maps to the postgres-transactions topic
CREATE TABLE orders (
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
    'kafka.topic' = 'postgres-transactions',
    'value.format' = 'json-registry'
);

-- Step 2: Create table for fraud alerts output
CREATE TABLE fraud_alerts_flink (
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
) WITH (
    'kafka.topic' = 'fraud-alerts-flink',
    'value.format' = 'json'
);

-- Step 3: Simple fraud detection - High value orders
-- This query runs continuously and detects orders over $200
INSERT INTO fraud_alerts_flink
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
FROM orders
WHERE orderunits > 200;  -- Only output suspicious orders

-- ============================================
-- ADVANCED: Velocity-based fraud detection
-- Uncomment and run this after the basic query is working
-- ============================================

/*
-- Create view for order velocity by location
CREATE VIEW order_velocity AS
SELECT 
    address.city as city,
    address.state as state,
    address.zipcode as zipcode,
    COUNT(*) as order_count,
    SUM(orderunits) as total_amount,
    MAX(orderunits) as max_order,
    TUMBLE_START(event_time, INTERVAL '5' MINUTES) as window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTES) as window_end
FROM orders
GROUP BY 
    address.city,
    address.state,
    address.zipcode,
    TUMBLE(event_time, INTERVAL '5' MINUTES);

-- Flag high-velocity locations
INSERT INTO fraud_alerts_flink
SELECT 
    CONCAT('velocity_', city, '_', CAST(UNIX_TIMESTAMP() AS STRING)) as alert_id,
    CURRENT_TIMESTAMP as alert_timestamp,
    0 as orderid,
    'MULTIPLE_ORDERS' as itemid,
    total_amount as amount,
    city,
    state,
    zipcode,
    CASE 
        WHEN order_count > 10 THEN 90
        WHEN order_count > 5 THEN 60
        ELSE 30
    END as fraud_score,
    CASE 
        WHEN order_count > 10 THEN 'BLOCK'
        WHEN order_count > 5 THEN 'REVIEW'
        ELSE 'MONITOR'
    END as decision,
    CONCAT('High velocity: ', CAST(order_count AS STRING), ' orders in 5 minutes, total: $', CAST(total_amount AS STRING)) as reason
FROM order_velocity
WHERE order_count > 5;
*/

-- ============================================
-- To view results, run in a separate SQL tab:
-- ============================================
-- SELECT * FROM fraud_alerts_flink;

-- Made with Bob
