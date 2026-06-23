-- ============================================================================
-- Advanced ML-Powered Fraud Detection with Apache Flink SQL
-- ============================================================================
-- This file contains Flink SQL queries for advanced fraud detection using
-- machine learning models, complex event processing, and graph analytics
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CATALOG AND TABLE DEFINITIONS
-- ----------------------------------------------------------------------------

-- Create catalog for Kafka topics
CREATE CATALOG kafka_catalog WITH (
    'type' = 'generic_in_memory'
);

USE CATALOG kafka_catalog;

-- Transaction source table with watermark for event time processing
CREATE TABLE transactions (
    transaction_id BIGINT,
    user_id BIGINT,
    merchant_id BIGINT,
    amount DECIMAL(15,2),
    currency STRING,
    payment_method STRING,
    card_last_four STRING,
    transaction_type STRING,
    ip_address STRING,
    device_id STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    created_at BIGINT,
    event_time AS TO_TIMESTAMP(FROM_UNIXTIME(created_at / 1000)),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'postgres-transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-fraud-detection',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- User profiles dimension table
CREATE TABLE user_profiles (
    user_id BIGINT PRIMARY KEY NOT ENFORCED,
    email STRING,
    account_age_days INT,
    kyc_verified BOOLEAN,
    risk_score INT,
    country STRING,
    total_transactions INT,
    total_amount_lifetime DECIMAL(15,2)
) WITH (
    'connector' = 'kafka',
    'topic' = 'mongo-user_profiles',
    'properties.bootstrap.servers' = 'kafka:9092',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- ML model predictions table (from TensorFlow Serving)
CREATE TABLE ml_predictions (
    transaction_id BIGINT,
    model_name STRING,
    fraud_probability DOUBLE,
    prediction_class STRING,
    feature_importance MAP<STRING, DOUBLE>,
    model_version STRING,
    prediction_timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'ml-predictions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

-- ----------------------------------------------------------------------------
-- 2. COMPLEX EVENT PROCESSING - Pattern Detection
-- ----------------------------------------------------------------------------

-- Detect rapid-fire transaction patterns (5+ transactions in 30 seconds)
CREATE VIEW rapid_fire_pattern AS
SELECT
    user_id,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_amount,
    COLLECT(transaction_id) AS transaction_ids,
    MIN(event_time) AS pattern_start,
    MAX(event_time) AS pattern_end,
    'RAPID_FIRE' AS pattern_type
FROM transactions
GROUP BY 
    user_id,
    HOP(event_time, INTERVAL '10' SECOND, INTERVAL '30' SECOND)
HAVING COUNT(*) >= 5;

-- Detect amount escalation pattern (increasing transaction amounts)
CREATE VIEW amount_escalation_pattern AS
SELECT
    user_id,
    transaction_id,
    amount,
    LAG(amount, 1) OVER w AS prev_amount_1,
    LAG(amount, 2) OVER w AS prev_amount_2,
    LAG(amount, 3) OVER w AS prev_amount_3,
    event_time,
    CASE 
        WHEN amount > LAG(amount, 1) OVER w 
         AND LAG(amount, 1) OVER w > LAG(amount, 2) OVER w
         AND LAG(amount, 2) OVER w > LAG(amount, 3) OVER w
        THEN TRUE
        ELSE FALSE
    END AS is_escalating
FROM transactions
WINDOW w AS (
    PARTITION BY user_id 
    ORDER BY event_time
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
);

-- Detect merchant hopping (multiple different merchants in short time)
CREATE VIEW merchant_hopping_pattern AS
SELECT
    user_id,
    COUNT(DISTINCT merchant_id) AS unique_merchants,
    COUNT(*) AS txn_count,
    COLLECT(DISTINCT merchant_id) AS merchant_list,
    MIN(event_time) AS window_start,
    MAX(event_time) AS window_end
FROM transactions
GROUP BY 
    user_id,
    TUMBLE(event_time, INTERVAL '5' MINUTE)
HAVING COUNT(DISTINCT merchant_id) >= 5;

-- ----------------------------------------------------------------------------
-- 3. ADVANCED VELOCITY CHECKS WITH SLIDING WINDOWS
-- ----------------------------------------------------------------------------

-- Multi-timeframe velocity analysis
CREATE VIEW velocity_analysis AS
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.event_time,
    -- 1 minute velocity
    COUNT(*) OVER w1 AS txn_count_1m,
    SUM(amount) OVER w1 AS total_amount_1m,
    -- 5 minute velocity
    COUNT(*) OVER w5 AS txn_count_5m,
    SUM(amount) OVER w5 AS total_amount_5m,
    COUNT(DISTINCT merchant_id) OVER w5 AS unique_merchants_5m,
    -- 1 hour velocity
    COUNT(*) OVER w60 AS txn_count_1h,
    SUM(amount) OVER w60 AS total_amount_1h,
    COUNT(DISTINCT merchant_id) OVER w60 AS unique_merchants_1h,
    COUNT(DISTINCT ip_address) OVER w60 AS unique_ips_1h,
    -- 24 hour velocity
    COUNT(*) OVER w1440 AS txn_count_24h,
    SUM(amount) OVER w1440 AS total_amount_24h
FROM transactions t
WINDOW 
    w1 AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '1' MINUTE PRECEDING AND CURRENT ROW),
    w5 AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW),
    w60 AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW),
    w1440 AS (PARTITION BY user_id ORDER BY event_time RANGE BETWEEN INTERVAL '24' HOUR PRECEDING AND CURRENT ROW);

-- ----------------------------------------------------------------------------
-- 4. GEOSPATIAL ANALYSIS
-- ----------------------------------------------------------------------------

-- Calculate distance between consecutive transactions
CREATE VIEW geospatial_analysis AS
SELECT
    transaction_id,
    user_id,
    latitude,
    longitude,
    event_time,
    LAG(latitude) OVER w AS prev_latitude,
    LAG(longitude) OVER w AS prev_longitude,
    LAG(event_time) OVER w AS prev_event_time,
    -- Haversine distance in kilometers
    CASE 
        WHEN LAG(latitude) OVER w IS NOT NULL THEN
            2 * 6371 * ASIN(SQRT(
                POWER(SIN(RADIANS((latitude - LAG(latitude) OVER w) / 2)), 2) +
                COS(RADIANS(LAG(latitude) OVER w)) * COS(RADIANS(latitude)) *
                POWER(SIN(RADIANS((longitude - LAG(longitude) OVER w) / 2)), 2)
            ))
        ELSE 0
    END AS distance_km,
    -- Time difference in hours
    CASE
        WHEN LAG(event_time) OVER w IS NOT NULL THEN
            TIMESTAMPDIFF(SECOND, LAG(event_time) OVER w, event_time) / 3600.0
        ELSE 0
    END AS time_diff_hours,
    -- Velocity in km/h
    CASE
        WHEN LAG(event_time) OVER w IS NOT NULL 
         AND TIMESTAMPDIFF(SECOND, LAG(event_time) OVER w, event_time) > 0 THEN
            (2 * 6371 * ASIN(SQRT(
                POWER(SIN(RADIANS((latitude - LAG(latitude) OVER w) / 2)), 2) +
                COS(RADIANS(LAG(latitude) OVER w)) * COS(RADIANS(latitude)) *
                POWER(SIN(RADIANS((longitude - LAG(longitude) OVER w) / 2)), 2)
            ))) / (TIMESTAMPDIFF(SECOND, LAG(event_time) OVER w, event_time) / 3600.0)
        ELSE 0
    END AS velocity_kmh
FROM transactions
WINDOW w AS (PARTITION BY user_id ORDER BY event_time);

-- Impossible travel detection
CREATE VIEW impossible_travel AS
SELECT
    transaction_id,
    user_id,
    distance_km,
    time_diff_hours,
    velocity_kmh,
    'IMPOSSIBLE_TRAVEL' AS alert_type,
    CASE
        WHEN velocity_kmh > 1000 THEN 'CRITICAL'
        WHEN velocity_kmh > 800 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity
FROM geospatial_analysis
WHERE velocity_kmh > 800;

-- ----------------------------------------------------------------------------
-- 5. BEHAVIORAL ANALYSIS - User Profiling
-- ----------------------------------------------------------------------------

-- Calculate user behavioral baseline
CREATE VIEW user_behavior_baseline AS
SELECT
    user_id,
    -- Transaction patterns
    AVG(amount) AS avg_amount,
    STDDEV_POP(amount) AS stddev_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COUNT(*) AS total_transactions,
    -- Temporal patterns
    COUNT(DISTINCT DATE_FORMAT(event_time, 'HH')) AS active_hours,
    COUNT(DISTINCT DATE_FORMAT(event_time, 'u')) AS active_days_of_week,
    -- Merchant patterns
    COUNT(DISTINCT merchant_id) AS unique_merchants,
    -- Payment patterns
    COUNT(DISTINCT payment_method) AS unique_payment_methods,
    -- Geographic patterns
    COUNT(DISTINCT CONCAT(CAST(ROUND(latitude, 1) AS STRING), ',', CAST(ROUND(longitude, 1) AS STRING))) AS unique_locations
FROM transactions
GROUP BY 
    user_id,
    TUMBLE(event_time, INTERVAL '30' DAY);

-- Detect behavioral anomalies
CREATE VIEW behavioral_anomalies AS
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.merchant_id,
    t.event_time,
    b.avg_amount,
    b.stddev_amount,
    -- Z-score for amount
    CASE 
        WHEN b.stddev_amount > 0 THEN
            (t.amount - b.avg_amount) / b.stddev_amount
        ELSE 0
    END AS amount_z_score,
    -- Anomaly flags
    CASE WHEN t.amount > b.max_amount * 2 THEN TRUE ELSE FALSE END AS is_amount_anomaly,
    CASE WHEN t.merchant_id NOT IN (
        SELECT DISTINCT merchant_id 
        FROM transactions t2 
        WHERE t2.user_id = t.user_id 
          AND t2.event_time >= t.event_time - INTERVAL '30' DAY
    ) THEN TRUE ELSE FALSE END AS is_new_merchant,
    'BEHAVIORAL_ANOMALY' AS alert_type
FROM transactions t
LEFT JOIN user_behavior_baseline b ON t.user_id = b.user_id
WHERE 
    (b.stddev_amount > 0 AND ABS((t.amount - b.avg_amount) / b.stddev_amount) > 3)
    OR t.amount > b.max_amount * 2;

-- ----------------------------------------------------------------------------
-- 6. ML MODEL INTEGRATION
-- ----------------------------------------------------------------------------

-- Enrich transactions with ML predictions
CREATE VIEW ml_enriched_transactions AS
SELECT
    t.transaction_id,
    t.user_id,
    t.merchant_id,
    t.amount,
    t.event_time,
    v.txn_count_5m,
    v.total_amount_5m,
    v.unique_merchants_1h,
    g.distance_km,
    g.velocity_kmh,
    ml.fraud_probability,
    ml.prediction_class,
    ml.model_name,
    ml.model_version,
    -- Risk score combining rule-based and ML
    CASE
        WHEN ml.fraud_probability >= 0.8 THEN 90
        WHEN ml.fraud_probability >= 0.6 THEN 70
        WHEN ml.fraud_probability >= 0.4 THEN 50
        ELSE 30
    END + 
    CASE WHEN v.txn_count_5m > 10 THEN 10 ELSE 0 END +
    CASE WHEN g.velocity_kmh > 800 THEN 10 ELSE 0 END AS combined_risk_score
FROM transactions t
LEFT JOIN velocity_analysis v ON t.transaction_id = v.transaction_id
LEFT JOIN geospatial_analysis g ON t.transaction_id = g.transaction_id
LEFT JOIN ml_predictions ml ON t.transaction_id = ml.transaction_id;

-- ----------------------------------------------------------------------------
-- 7. GRAPH-BASED FRAUD DETECTION
-- ----------------------------------------------------------------------------

-- Detect fraud rings (users sharing devices/IPs)
CREATE VIEW fraud_ring_detection AS
SELECT
    device_id,
    COUNT(DISTINCT user_id) AS unique_users,
    COLLECT(DISTINCT user_id) AS user_list,
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_amount,
    MIN(event_time) AS first_seen,
    MAX(event_time) AS last_seen,
    'FRAUD_RING' AS pattern_type
FROM transactions
WHERE device_id IS NOT NULL
GROUP BY 
    device_id,
    TUMBLE(event_time, INTERVAL '1' HOUR)
HAVING COUNT(DISTINCT user_id) >= 3;

-- Detect account takeover patterns
CREATE VIEW account_takeover_detection AS
SELECT
    t.user_id,
    t.transaction_id,
    t.device_id,
    t.ip_address,
    t.event_time,
    -- Check if device/IP is new for this user
    CASE WHEN t.device_id NOT IN (
        SELECT DISTINCT device_id 
        FROM transactions t2 
        WHERE t2.user_id = t.user_id 
          AND t2.event_time < t.event_time - INTERVAL '1' DAY
          AND t2.event_time >= t.event_time - INTERVAL '30' DAY
    ) THEN TRUE ELSE FALSE END AS is_new_device,
    CASE WHEN t.ip_address NOT IN (
        SELECT DISTINCT ip_address 
        FROM transactions t2 
        WHERE t2.user_id = t.user_id 
          AND t2.event_time < t.event_time - INTERVAL '1' DAY
          AND t2.event_time >= t.event_time - INTERVAL '30' DAY
    ) THEN TRUE ELSE FALSE END AS is_new_ip,
    'ACCOUNT_TAKEOVER' AS alert_type
FROM transactions t
WHERE 
    t.device_id NOT IN (
        SELECT DISTINCT device_id 
        FROM transactions t2 
        WHERE t2.user_id = t.user_id 
          AND t2.event_time < t.event_time - INTERVAL '1' DAY
          AND t2.event_time >= t.event_time - INTERVAL '30' DAY
    )
    AND t.amount > 100;

-- ----------------------------------------------------------------------------
-- 8. REAL-TIME FEATURE ENGINEERING FOR ML
-- ----------------------------------------------------------------------------

-- Generate features for ML model inference
CREATE VIEW ml_features AS
SELECT
    t.transaction_id,
    t.user_id,
    t.amount,
    t.event_time,
    -- Transaction features
    HOUR(t.event_time) AS hour_of_day,
    DAYOFWEEK(t.event_time) AS day_of_week,
    CASE WHEN DAYOFWEEK(t.event_time) IN (6, 7) THEN 1 ELSE 0 END AS is_weekend,
    -- User features
    u.account_age_days,
    u.kyc_verified,
    u.risk_score AS user_risk_score,
    u.total_transactions AS user_lifetime_txns,
    -- Velocity features
    v.txn_count_1m,
    v.txn_count_5m,
    v.txn_count_1h,
    v.total_amount_1m,
    v.total_amount_5m,
    v.total_amount_1h,
    v.unique_merchants_5m,
    v.unique_ips_1h,
    -- Geospatial features
    g.distance_km,
    g.velocity_kmh,
    -- Behavioral features
    b.avg_amount AS user_avg_amount,
    b.stddev_amount AS user_stddev_amount,
    CASE WHEN b.stddev_amount > 0 THEN (t.amount - b.avg_amount) / b.stddev_amount ELSE 0 END AS amount_z_score,
    -- Derived features
    CASE WHEN u.total_transactions > 0 THEN t.amount / (u.total_amount_lifetime / u.total_transactions) ELSE 0 END AS amount_vs_avg_ratio,
    CASE WHEN v.txn_count_1h > 0 THEN v.total_amount_1h / v.txn_count_1h ELSE 0 END AS avg_amount_1h
FROM transactions t
LEFT JOIN user_profiles u ON t.user_id = u.user_id
LEFT JOIN velocity_analysis v ON t.transaction_id = v.transaction_id
LEFT JOIN geospatial_analysis g ON t.transaction_id = g.transaction_id
LEFT JOIN user_behavior_baseline b ON t.user_id = b.user_id;

-- ----------------------------------------------------------------------------
-- 9. CONSOLIDATED FRAUD ALERTS
-- ----------------------------------------------------------------------------

-- Combine all fraud signals
CREATE TABLE fraud_alerts_comprehensive (
    alert_id STRING,
    transaction_id BIGINT,
    user_id BIGINT,
    alert_type STRING,
    severity STRING,
    fraud_score DOUBLE,
    ml_probability DOUBLE,
    risk_factors ARRAY<STRING>,
    alert_timestamp TIMESTAMP(3),
    PRIMARY KEY (alert_id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'fraud-alerts-comprehensive',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'avro-confluent',
    'avro-confluent.url' = 'http://schema-registry:8081'
);

-- Insert high-risk ML predictions
INSERT INTO fraud_alerts_comprehensive
SELECT
    CONCAT('ML-', CAST(transaction_id AS STRING), '-', CAST(UNIX_TIMESTAMP(event_time) AS STRING)) AS alert_id,
    transaction_id,
    user_id,
    'ML_HIGH_RISK' AS alert_type,
    CASE
        WHEN fraud_probability >= 0.9 THEN 'CRITICAL'
        WHEN fraud_probability >= 0.7 THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS severity,
    combined_risk_score AS fraud_score,
    fraud_probability AS ml_probability,
    ARRAY['HIGH_ML_SCORE'] AS risk_factors,
    event_time AS alert_timestamp
FROM ml_enriched_transactions
WHERE fraud_probability >= 0.7;

-- Insert velocity violations
INSERT INTO fraud_alerts_comprehensive
SELECT
    CONCAT('VEL-', CAST(transaction_id AS STRING), '-', CAST(UNIX_TIMESTAMP(event_time) AS STRING)) AS alert_id,
    transaction_id,
    user_id,
    'VELOCITY_VIOLATION' AS alert_type,
    'HIGH' AS severity,
    70.0 AS fraud_score,
    0.0 AS ml_probability,
    ARRAY['HIGH_VELOCITY'] AS risk_factors,
    event_time AS alert_timestamp
FROM velocity_analysis
WHERE txn_count_5m > 10;

-- Insert impossible travel alerts
INSERT INTO fraud_alerts_comprehensive
SELECT
    CONCAT('GEO-', CAST(transaction_id AS STRING), '-', CAST(CURRENT_TIMESTAMP AS STRING)) AS alert_id,
    transaction_id,
    user_id,
    alert_type,
    severity,
    80.0 AS fraud_score,
    0.0 AS ml_probability,
    ARRAY['IMPOSSIBLE_TRAVEL'] AS risk_factors,
    CURRENT_TIMESTAMP AS alert_timestamp
FROM impossible_travel;

-- ----------------------------------------------------------------------------
-- 10. PERFORMANCE MONITORING
-- ----------------------------------------------------------------------------

-- Track processing metrics
CREATE VIEW processing_metrics AS
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS transactions_processed,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount
FROM transactions
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE);

-- Track fraud detection metrics
CREATE VIEW fraud_detection_metrics AS
SELECT
    TUMBLE_START(alert_timestamp, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS total_alerts,
    COUNT(DISTINCT user_id) AS unique_users_flagged,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_alerts,
    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) AS high_alerts,
    SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_alerts,
    AVG(fraud_score) AS avg_fraud_score
FROM fraud_alerts_comprehensive
GROUP BY TUMBLE(alert_timestamp, INTERVAL '1' MINUTE);

-- ============================================================================
-- DEPLOYMENT CONFIGURATION
-- ============================================================================

-- Set Flink configuration for optimal performance
SET 'execution.checkpointing.interval' = '60s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'table.exec.state.ttl' = '24h';
SET 'pipeline.max-parallelism' = '128';
SET 'taskmanager.memory.managed.fraction' = '0.4';

-- ============================================================================
-- NOTES
-- ============================================================================
-- 1. This Flink SQL implementation provides advanced fraud detection with:
--    - Complex event processing for pattern detection
--    - Multi-timeframe velocity analysis
--    - Geospatial impossible travel detection
--    - Behavioral anomaly detection
--    - ML model integration
--    - Graph-based fraud ring detection
--    - Real-time feature engineering
--
-- 2. Performance considerations:
--    - Use RocksDB state backend for large state
--    - Enable incremental checkpointing
--    - Set appropriate TTL for state
--    - Monitor backpressure and adjust parallelism
--
-- 3. Integration with ML:
--    - Deploy TensorFlow Serving or similar for model inference
--    - Use Kafka topics for model predictions
--    - Implement A/B testing for model versions
--    - Monitor model drift and retrain regularly
--
-- 4. Scaling:
--    - Increase parallelism for higher throughput
--    - Use Flink's auto-scaling capabilities
--    - Partition data by user_id for better distribution
--    - Monitor resource utilization
-- ============================================================================

-- Made with Bob
