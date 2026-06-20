CREATE DATABASE IF NOT EXISTS analytics_db;
USE analytics_db;

CREATE TABLE IF NOT EXISTS transactions_raw (
    transaction_id STRING,
    account_id STRING,
    tx_timestamp TIMESTAMP,
    amount DOUBLE,
    merchant STRING,
    category STRING,
    location_country STRING,
    device_id STRING,
    is_fraud_flag INT
)
PARTITIONED BY (tx_date STRING)
STORED AS PARQUET; -- Parquet storage ensures high-speed compression and execution

