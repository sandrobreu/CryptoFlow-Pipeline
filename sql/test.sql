SELECT coin_id, currency, price, market_cap, total_volume, load_timestamp
FROM raw_crypto_prices
ORDER BY load_timestamp DESC;

SELECT *
FROM raw_crypto_prices
ORDER BY load_timestamp DESC;