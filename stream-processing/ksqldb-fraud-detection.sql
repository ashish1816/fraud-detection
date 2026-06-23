-- ============================================================================
-- Real-Time Fraud Detection with ksqlDB
-- ============================================================================
-- This file contains ksqlDB queries for real-time fraud detection and prevention
-- Deploy these queries in order to build the complete fraud detection pipeline
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. CREATE STREAMS FROM SOURCE TOPICS
-- ----------------------------------------------------------------------------

-- Raw transaction stream from PostgreSQL
CREATE STREAM transactions_raw (
    transaction_id BIGINT KEY,
    user_id BIGINT,
    merchant_id BIGINT,
    amount DECIMAL(15,2),
    currency VARCHAR,
    payment_method VARCHAR,
    card_last_four VARCHAR,
    transaction_type VARCHAR,
    status VARCHAR,
    ip_address VARCHAR,
    device_id VARCHAR,
    user_agent VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    created_at BIGINT
) WITH (
    KAFKA_TOPIC='postgres-transactions',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='created_at'
);

-- User profile stream from MongoDB
CREATE STREAM user_profiles_raw (
    user_id BIGINT KEY,
    email VARCHAR,
    phone VARCHAR,
    account_age_days INT,
    kyc_verified BOOLEAN,
    risk_score INT,
    country VARCHAR,
    preferred_currency VARCHAR,
    total_transactions INT,
    total_amount_lifetime DECIMAL(15,2),
    last_login_at BIGINT,
    updated_at BIGINT
) WITH (
    KAFKA_TOPIC='mongo-user_profiles',
    VALUE_FORMAT='AVRO',
    TIMESTAMP='updated_at'
);

-- Device fingerprint stream
CREATE STREAM device_fingerprints (
    device_id VARCHAR KEY,
    user_id BIGINT,
    device_type VARCHAR,
    os VARCHAR,
    browser VARCHAR,
    is_vpn BOOLEAN,
    is_proxy BOOLEAN,
    is_tor BOOLEAN,
    risk_level VARCHAR,
    first_seen_at BIGINT,
    last_seen_at BIGINT
) WITH (
    KAFKA_TOPIC='device-fingerprints',
    VALUE_FORMAT='AVRO'
);

-- External fraud intelligence
CREATE STREAM fraud_intel (
    indicator_id VARCHAR KEY,
    indicator_type VARCHAR, -- IP, EMAIL, CARD, DEVICE
    indicator_value VARCHAR,
    threat_level VARCHAR, -- LOW, MEDIUM, HIGH, CRITICAL
    threat_category VARCHAR,
    first_reported_at BIGINT,
    confidence_score DOUBLE
) WITH (
    KAFKA_TOPIC='external-fraud-intel',
    VALUE_FORMAT='AVRO'
);

-- ----------------------------------------------------------------------------
-- 2. CREATE TABLES FOR LOOKUPS AND AGGREGATIONS
-- ----------------------------------------------------------------------------

-- User profile table for enrichment
CREATE TABLE user_profiles AS
    SELECT 
        user_id,
        LATEST_BY_OFFSET(email) AS email,
        LATEST_BY_OFFSET(account_age_days) AS account_age_days,
        LATEST_BY_OFFSET(kyc_verified) AS kyc_verified,
        LATEST_BY_OFFSET(risk_score) AS risk_score,
        LATEST_BY_OFFSET(country) AS country,
        LATEST_BY_OFFSET(total_transactions) AS total_transactions,
        LATEST_BY_OFFSET(total_amount_lifetime) AS total_amount_lifetime
    FROM user_profiles_raw
    GROUP BY user_id
    EMIT CHANGES;

-- Device reputation table
CREATE TABLE device_reputation AS
    SELECT
        device_id,
        LATEST_BY_OFFSET(is_vpn) AS is_vpn,
        LATEST_BY_OFFSET(is_proxy) AS is_proxy,
        LATEST_BY_OFFSET(is_tor) AS is_tor,
        LATEST_BY_OFFSET(risk_level) AS risk_level
    FROM device_fingerprints
    GROUP BY device_id
    EMIT CHANGES;

-- Fraud indicator lookup table
CREATE TABLE fraud_indicators AS
    SELECT
        indicator_value AS indicator_key,
        LATEST_BY_OFFSET(indicator_type) AS indicator_type,
        LATEST_BY_OFFSET(threat_level) AS threat_level,
        LATEST_BY_OFFSET(confidence_score) AS confidence_score
    FROM fraud_intel
    GROUP BY indicator_value
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 3. VELOCITY CHECKS - Detect High-Frequency Patterns
-- ----------------------------------------------------------------------------

-- Transaction count per user in last 5 minutes
CREATE TABLE user_transaction_velocity_5m AS
    SELECT
        user_id,
        COUNT(*) AS txn_count_5m,
        SUM(amount) AS total_amount_5m,
        COLLECT_LIST(merchant_id) AS merchants_5m
    FROM transactions_raw
    WINDOW TUMBLING (SIZE 5 MINUTES)
    GROUP BY user_id
    EMIT CHANGES;

-- Transaction count per user in last 1 hour
CREATE TABLE user_transaction_velocity_1h AS
    SELECT
        user_id,
        COUNT(*) AS txn_count_1h,
        SUM(amount) AS total_amount_1h,
        COUNT_DISTINCT(merchant_id) AS unique_merchants_1h,
        COUNT_DISTINCT(ip_address) AS unique_ips_1h
    FROM transactions_raw
    WINDOW TUMBLING (SIZE 1 HOUR)
    GROUP BY user_id
    EMIT CHANGES;

-- Card usage velocity (same card, different users)
CREATE TABLE card_velocity AS
    SELECT
        card_last_four,
        COUNT_DISTINCT(user_id) AS unique_users,
        COUNT(*) AS txn_count,
        SUM(amount) AS total_amount
    FROM transactions_raw
    WINDOW TUMBLING (SIZE 10 MINUTES)
    GROUP BY card_last_four
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 4. GEOLOCATION ANOMALY DETECTION
-- ----------------------------------------------------------------------------

-- Detect impossible travel (same user, different locations in short time)
CREATE STREAM user_locations AS
    SELECT
        user_id,
        transaction_id,
        latitude,
        longitude,
        created_at,
        LAG(latitude, 1) OVER (PARTITION BY user_id) AS prev_latitude,
        LAG(longitude, 1) OVER (PARTITION BY user_id) AS prev_longitude,
        LAG(created_at, 1) OVER (PARTITION BY user_id) AS prev_timestamp
    FROM transactions_raw
    PARTITION BY user_id
    EMIT CHANGES;

-- Calculate distance and time between transactions
CREATE STREAM impossible_travel AS
    SELECT
        user_id,
        transaction_id,
        latitude,
        longitude,
        created_at,
        prev_latitude,
        prev_longitude,
        prev_timestamp,
        -- Haversine distance in km (simplified)
        (6371 * ACOS(
            COS(RADIANS(prev_latitude)) * COS(RADIANS(latitude)) * 
            COS(RADIANS(longitude) - RADIANS(prev_longitude)) + 
            SIN(RADIANS(prev_latitude)) * SIN(RADIANS(latitude))
        )) AS distance_km,
        ((created_at - prev_timestamp) / 1000.0 / 3600.0) AS time_hours,
        -- Speed in km/h
        (6371 * ACOS(
            COS(RADIANS(prev_latitude)) * COS(RADIANS(latitude)) * 
            COS(RADIANS(longitude) - RADIANS(prev_longitude)) + 
            SIN(RADIANS(prev_latitude)) * SIN(RADIANS(latitude))
        )) / ((created_at - prev_timestamp) / 1000.0 / 3600.0) AS speed_kmh
    FROM user_locations
    WHERE prev_latitude IS NOT NULL
        AND prev_longitude IS NOT NULL
        AND (created_at - prev_timestamp) > 0
    EMIT CHANGES;

-- Flag impossible travel (speed > 1000 km/h)
CREATE STREAM impossible_travel_alerts AS
    SELECT
        user_id,
        transaction_id,
        distance_km,
        time_hours,
        speed_kmh,
        'IMPOSSIBLE_TRAVEL' AS alert_type,
        'HIGH' AS severity
    FROM impossible_travel
    WHERE speed_kmh > 1000
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 5. ENRICHED TRANSACTION STREAM
-- ----------------------------------------------------------------------------

-- Enrich transactions with user profile and device data
CREATE STREAM transactions_enriched AS
    SELECT
        t.transaction_id,
        t.user_id,
        t.merchant_id,
        t.amount,
        t.currency,
        t.payment_method,
        t.card_last_four,
        t.transaction_type,
        t.ip_address,
        t.device_id,
        t.latitude,
        t.longitude,
        t.created_at,
        -- User profile enrichment
        u.account_age_days,
        u.kyc_verified,
        u.risk_score AS user_risk_score,
        u.country AS user_country,
        u.total_transactions AS user_total_txns,
        u.total_amount_lifetime AS user_lifetime_amount,
        -- Device enrichment
        d.is_vpn,
        d.is_proxy,
        d.is_tor,
        d.risk_level AS device_risk_level,
        -- Velocity metrics
        v5.txn_count_5m,
        v5.total_amount_5m,
        v1.txn_count_1h,
        v1.total_amount_1h,
        v1.unique_merchants_1h,
        v1.unique_ips_1h,
        -- Card velocity
        cv.unique_users AS card_unique_users,
        cv.txn_count AS card_txn_count
    FROM transactions_raw t
    LEFT JOIN user_profiles u ON t.user_id = u.user_id
    LEFT JOIN device_reputation d ON t.device_id = d.device_id
    LEFT JOIN user_transaction_velocity_5m v5 ON t.user_id = v5.user_id
    LEFT JOIN user_transaction_velocity_1h v1 ON t.user_id = v1.user_id
    LEFT JOIN card_velocity cv ON t.card_last_four = cv.card_last_four
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 6. RULE-BASED FRAUD DETECTION
-- ----------------------------------------------------------------------------

-- High-risk transaction patterns
CREATE STREAM high_risk_transactions AS
    SELECT
        transaction_id,
        user_id,
        amount,
        'HIGH_RISK_PATTERN' AS alert_type,
        CASE
            WHEN amount > 10000 THEN 'CRITICAL'
            WHEN amount > 5000 THEN 'HIGH'
            WHEN amount > 1000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS severity,
        ARRAY[
            CASE WHEN amount > 5000 THEN 'LARGE_AMOUNT' ELSE NULL END,
            CASE WHEN NOT kyc_verified THEN 'UNVERIFIED_USER' ELSE NULL END,
            CASE WHEN account_age_days < 7 THEN 'NEW_ACCOUNT' ELSE NULL END,
            CASE WHEN is_vpn OR is_proxy OR is_tor THEN 'SUSPICIOUS_DEVICE' ELSE NULL END,
            CASE WHEN txn_count_5m > 10 THEN 'HIGH_VELOCITY' ELSE NULL END,
            CASE WHEN user_risk_score > 70 THEN 'HIGH_USER_RISK' ELSE NULL END,
            CASE WHEN card_unique_users > 3 THEN 'CARD_SHARING' ELSE NULL END
        ] AS risk_factors,
        created_at
    FROM transactions_enriched
    WHERE 
        -- Large amount
        (amount > 5000)
        -- OR unverified user with significant transaction
        OR (NOT kyc_verified AND amount > 1000)
        -- OR new account with large transaction
        OR (account_age_days < 7 AND amount > 500)
        -- OR suspicious device
        OR (is_vpn OR is_proxy OR is_tor)
        -- OR high velocity
        OR (txn_count_5m > 10)
        -- OR high user risk score
        OR (user_risk_score > 70)
        -- OR card sharing
        OR (card_unique_users > 3)
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 7. ANOMALY DETECTION - Statistical Outliers
-- ----------------------------------------------------------------------------

-- Calculate user's average transaction amount
CREATE TABLE user_avg_amount AS
    SELECT
        user_id,
        AVG(amount) AS avg_amount,
        STDDEV_SAMP(amount) AS stddev_amount,
        COUNT(*) AS sample_size
    FROM transactions_raw
    WINDOW TUMBLING (SIZE 30 DAYS)
    GROUP BY user_id
    HAVING COUNT(*) >= 10  -- Need sufficient history
    EMIT CHANGES;

-- Detect transactions that are statistical outliers
CREATE STREAM amount_anomalies AS
    SELECT
        t.transaction_id,
        t.user_id,
        t.amount,
        u.avg_amount,
        u.stddev_amount,
        -- Z-score
        (t.amount - u.avg_amount) / u.stddev_amount AS z_score,
        'AMOUNT_ANOMALY' AS alert_type,
        CASE
            WHEN ABS((t.amount - u.avg_amount) / u.stddev_amount) > 4 THEN 'CRITICAL'
            WHEN ABS((t.amount - u.avg_amount) / u.stddev_amount) > 3 THEN 'HIGH'
            ELSE 'MEDIUM'
        END AS severity
    FROM transactions_enriched t
    LEFT JOIN user_avg_amount u ON t.user_id = u.user_id
    WHERE u.avg_amount IS NOT NULL
        AND u.stddev_amount > 0
        AND ABS((t.amount - u.avg_amount) / u.stddev_amount) > 3
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 8. EXTERNAL THREAT INTELLIGENCE MATCHING
-- ----------------------------------------------------------------------------

-- Match transactions against known fraud indicators
CREATE STREAM threat_intel_matches AS
    SELECT
        t.transaction_id,
        t.user_id,
        t.ip_address,
        t.device_id,
        fi.indicator_type,
        fi.threat_level,
        fi.confidence_score,
        'THREAT_INTEL_MATCH' AS alert_type,
        fi.threat_level AS severity
    FROM transactions_enriched t
    LEFT JOIN fraud_indicators fi ON t.ip_address = fi.indicator_key
    WHERE fi.indicator_key IS NOT NULL
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 9. CONSOLIDATED FRAUD ALERTS
-- ----------------------------------------------------------------------------

-- Combine all fraud signals into a single alert stream
CREATE STREAM fraud_alerts AS
    SELECT 
        transaction_id,
        user_id,
        alert_type,
        severity,
        CAST(NULL AS ARRAY<VARCHAR>) AS risk_factors,
        CAST(NULL AS DOUBLE) AS z_score,
        CAST(NULL AS VARCHAR) AS indicator_type,
        created_at AS timestamp
    FROM high_risk_transactions
    EMIT CHANGES;

INSERT INTO fraud_alerts
    SELECT
        transaction_id,
        user_id,
        alert_type,
        severity,
        CAST(NULL AS ARRAY<VARCHAR>) AS risk_factors,
        z_score,
        CAST(NULL AS VARCHAR) AS indicator_type,
        created_at AS timestamp
    FROM amount_anomalies
    EMIT CHANGES;

INSERT INTO fraud_alerts
    SELECT
        transaction_id,
        user_id,
        alert_type,
        'HIGH' AS severity,
        CAST(NULL AS ARRAY<VARCHAR>) AS risk_factors,
        CAST(NULL AS DOUBLE) AS z_score,
        CAST(NULL AS VARCHAR) AS indicator_type,
        created_at AS timestamp
    FROM impossible_travel_alerts
    EMIT CHANGES;

INSERT INTO fraud_alerts
    SELECT
        transaction_id,
        user_id,
        alert_type,
        severity,
        CAST(NULL AS ARRAY<VARCHAR>) AS risk_factors,
        CAST(NULL AS DOUBLE) AS z_score,
        indicator_type,
        CAST(UNIX_TIMESTAMP() AS BIGINT) AS timestamp
    FROM threat_intel_matches
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 10. FRAUD SCORE CALCULATION
-- ----------------------------------------------------------------------------

-- Calculate comprehensive fraud score for each transaction
CREATE STREAM fraud_scores AS
    SELECT
        t.transaction_id,
        t.user_id,
        t.amount,
        t.merchant_id,
        -- Base score components (0-100 scale)
        CASE WHEN t.amount > 10000 THEN 30
             WHEN t.amount > 5000 THEN 20
             WHEN t.amount > 1000 THEN 10
             ELSE 0 END AS amount_score,
        
        CASE WHEN NOT t.kyc_verified THEN 15 ELSE 0 END AS kyc_score,
        
        CASE WHEN t.account_age_days < 1 THEN 20
             WHEN t.account_age_days < 7 THEN 15
             WHEN t.account_age_days < 30 THEN 10
             ELSE 0 END AS account_age_score,
        
        CASE WHEN t.is_tor THEN 25
             WHEN t.is_vpn OR t.is_proxy THEN 15
             ELSE 0 END AS device_score,
        
        CASE WHEN t.txn_count_5m > 20 THEN 20
             WHEN t.txn_count_5m > 10 THEN 15
             WHEN t.txn_count_5m > 5 THEN 10
             ELSE 0 END AS velocity_score,
        
        COALESCE(t.user_risk_score, 0) / 5 AS user_risk_score_normalized,
        
        -- Total fraud score
        (
            CASE WHEN t.amount > 10000 THEN 30
                 WHEN t.amount > 5000 THEN 20
                 WHEN t.amount > 1000 THEN 10
                 ELSE 0 END +
            CASE WHEN NOT t.kyc_verified THEN 15 ELSE 0 END +
            CASE WHEN t.account_age_days < 1 THEN 20
                 WHEN t.account_age_days < 7 THEN 15
                 WHEN t.account_age_days < 30 THEN 10
                 ELSE 0 END +
            CASE WHEN t.is_tor THEN 25
                 WHEN t.is_vpn OR t.is_proxy THEN 15
                 ELSE 0 END +
            CASE WHEN t.txn_count_5m > 20 THEN 20
                 WHEN t.txn_count_5m > 10 THEN 15
                 WHEN t.txn_count_5m > 5 THEN 10
                 ELSE 0 END +
            COALESCE(t.user_risk_score, 0) / 5
        ) AS fraud_score,
        
        -- Decision
        CASE 
            WHEN (
                CASE WHEN t.amount > 10000 THEN 30
                     WHEN t.amount > 5000 THEN 20
                     WHEN t.amount > 1000 THEN 10
                     ELSE 0 END +
                CASE WHEN NOT t.kyc_verified THEN 15 ELSE 0 END +
                CASE WHEN t.account_age_days < 1 THEN 20
                     WHEN t.account_age_days < 7 THEN 15
                     WHEN t.account_age_days < 30 THEN 10
                     ELSE 0 END +
                CASE WHEN t.is_tor THEN 25
                     WHEN t.is_vpn OR t.is_proxy THEN 15
                     ELSE 0 END +
                CASE WHEN t.txn_count_5m > 20 THEN 20
                     WHEN t.txn_count_5m > 10 THEN 15
                     WHEN t.txn_count_5m > 5 THEN 10
                     ELSE 0 END +
                COALESCE(t.user_risk_score, 0) / 5
            ) >= 70 THEN 'BLOCK'
            WHEN (
                CASE WHEN t.amount > 10000 THEN 30
                     WHEN t.amount > 5000 THEN 20
                     WHEN t.amount > 1000 THEN 10
                     ELSE 0 END +
                CASE WHEN NOT t.kyc_verified THEN 15 ELSE 0 END +
                CASE WHEN t.account_age_days < 1 THEN 20
                     WHEN t.account_age_days < 7 THEN 15
                     WHEN t.account_age_days < 30 THEN 10
                     ELSE 0 END +
                CASE WHEN t.is_tor THEN 25
                     WHEN t.is_vpn OR t.is_proxy THEN 15
                     ELSE 0 END +
                CASE WHEN t.txn_count_5m > 20 THEN 20
                     WHEN t.txn_count_5m > 10 THEN 15
                     WHEN t.txn_count_5m > 5 THEN 10
                     ELSE 0 END +
                COALESCE(t.user_risk_score, 0) / 5
            ) >= 50 THEN 'REVIEW'
            WHEN (
                CASE WHEN t.amount > 10000 THEN 30
                     WHEN t.amount > 5000 THEN 20
                     WHEN t.amount > 1000 THEN 10
                     ELSE 0 END +
                CASE WHEN NOT t.kyc_verified THEN 15 ELSE 0 END +
                CASE WHEN t.account_age_days < 1 THEN 20
                     WHEN t.account_age_days < 7 THEN 15
                     WHEN t.account_age_days < 30 THEN 10
                     ELSE 0 END +
                CASE WHEN t.is_tor THEN 25
                     WHEN t.is_vpn OR t.is_proxy THEN 15
                     ELSE 0 END +
                CASE WHEN t.txn_count_5m > 20 THEN 20
                     WHEN t.txn_count_5m > 10 THEN 15
                     WHEN t.txn_count_5m > 5 THEN 10
                     ELSE 0 END +
                COALESCE(t.user_risk_score, 0) / 5
            ) >= 30 THEN 'CHALLENGE'
            ELSE 'APPROVE'
        END AS decision,
        
        t.created_at
    FROM transactions_enriched t
    EMIT CHANGES;

-- ----------------------------------------------------------------------------
-- 11. PERFORMANCE METRICS
-- ----------------------------------------------------------------------------

-- Track fraud detection performance
CREATE TABLE fraud_detection_metrics AS
    SELECT
        TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss') AS window_start,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN decision = 'BLOCK' THEN 1 ELSE 0 END) AS blocked_count,
        SUM(CASE WHEN decision = 'REVIEW' THEN 1 ELSE 0 END) AS review_count,
        SUM(CASE WHEN decision = 'CHALLENGE' THEN 1 ELSE 0 END) AS challenge_count,
        SUM(CASE WHEN decision = 'APPROVE' THEN 1 ELSE 0 END) AS approved_count,
        AVG(fraud_score) AS avg_fraud_score,
        MAX(fraud_score) AS max_fraud_score
    FROM fraud_scores
    WINDOW TUMBLING (SIZE 1 MINUTE)
    GROUP BY 1
    EMIT CHANGES;

-- ============================================================================
-- DEPLOYMENT NOTES
-- ============================================================================
-- 1. Deploy queries in order (dependencies matter)
-- 2. Monitor query performance and adjust window sizes as needed
-- 3. Tune parallelism for high-throughput scenarios
-- 4. Set up alerting on fraud_alerts and fraud_scores topics
-- 5. Regularly review and update fraud rules based on feedback
-- 6. Consider adding ML model integration for advanced detection
-- ============================================================================

-- Made with Bob
