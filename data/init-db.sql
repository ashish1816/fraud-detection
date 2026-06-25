-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    merchant_id INTEGER NOT NULL,
    merchant_name VARCHAR(255) NOT NULL,
    merchant_category VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    location VARCHAR(255) NOT NULL,
    device_id VARCHAR(100) NOT NULL,
    ip_address VARCHAR(45) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create payment_methods table
CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    card_type VARCHAR(50) NOT NULL,
    card_last_four VARCHAR(4) NOT NULL,
    expiry_date VARCHAR(7) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create merchant_info table
CREATE TABLE IF NOT EXISTS merchant_info (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name VARCHAR(255) NOT NULL,
    merchant_category VARCHAR(100) NOT NULL,
    risk_level VARCHAR(20) DEFAULT 'LOW',
    country VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_amount ON transactions(amount);
CREATE INDEX idx_payment_methods_user_id ON payment_methods(user_id);
CREATE INDEX idx_merchant_info_category ON merchant_info(merchant_category);

-- Insert sample merchants
INSERT INTO merchant_info (merchant_id, merchant_name, merchant_category, risk_level, country) VALUES
(1001, 'Amazon', 'E-commerce', 'LOW', 'USA'),
(1002, 'Walmart', 'Retail', 'LOW', 'USA'),
(1003, 'Shell Gas Station', 'Gas Station', 'LOW', 'USA'),
(1004, 'Starbucks', 'Restaurant', 'LOW', 'USA'),
(1005, 'Best Buy', 'Electronics', 'MEDIUM', 'USA'),
(1006, 'Luxury Casino Online', 'Gambling', 'HIGH', 'Malta'),
(1007, 'Crypto Exchange XYZ', 'Cryptocurrency', 'HIGH', 'Cayman Islands'),
(1008, 'Target', 'Retail', 'LOW', 'USA'),
(1009, 'Apple Store', 'Electronics', 'LOW', 'USA'),
(1010, 'Netflix', 'Streaming', 'LOW', 'USA');

-- Insert sample payment methods
INSERT INTO payment_methods (user_id, card_type, card_last_four, expiry_date, is_active) VALUES
(5001, 'Visa', '1234', '12/2025', TRUE),
(5002, 'Mastercard', '5678', '06/2026', TRUE),
(5003, 'Amex', '9012', '03/2027', TRUE),
(5004, 'Visa', '3456', '09/2025', TRUE),
(5005, 'Mastercard', '7890', '11/2026', TRUE);

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger for transactions table
CREATE TRIGGER update_transactions_updated_at BEFORE UPDATE ON transactions
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO frauduser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO frauduser;

-- Made with Bob
