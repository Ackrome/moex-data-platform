-- File: docker/init.sql

CREATE DATABASE prefect;

-- --- AUTH ---
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(20) NOT NULL DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE user_charts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    name VARCHAR(100) NOT NULL,
    code TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, name)
);

-- Users
INSERT INTO users (username, password_hash, role) 
VALUES ('admin', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxwKc.6IymVFt7H.8W0kM.y.uAIiG', 'admin');

INSERT INTO users (username, password_hash, role) 
VALUES ('user', '$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxwKc.6IymVFt7H.8W0kM.y.uAIiG', 'user');

-- --- DATA WAREHOUSE (GOLD LAYER) ---
\c moex_dw

CREATE TABLE IF NOT EXISTS stock_metrics (
    ticker TEXT NOT NULL,
    interval TEXT NOT NULL,
    ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    sma_20 DOUBLE PRECISION,
    rsi_14 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, interval, ts)
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_ticker_interval ON stock_metrics(ticker, interval);
CREATE INDEX IF NOT EXISTS idx_ts ON stock_metrics(ts);