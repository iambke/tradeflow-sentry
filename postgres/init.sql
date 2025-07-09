CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    user_id_hash VARCHAR(64),
    trade_type VARCHAR(10),
    amount NUMERIC(12, 2),
    asset VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    user_id_hash VARCHAR(64),
    alert_msg TEXT,
    anomaly_score NUMERIC(5, 2),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);