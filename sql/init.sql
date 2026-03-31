CREATE TABLE IF NOT EXISTS raw_crypto_prices (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(100) NOT NULL,
    coin_id VARCHAR(100) NOT NULL,
    currency VARCHAR(20) NOT NULL,
    price NUMERIC(18,8),
    market_cap NUMERIC(30,2),
    total_volume NUMERIC(30,2),
    api_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_payload JSONB
);