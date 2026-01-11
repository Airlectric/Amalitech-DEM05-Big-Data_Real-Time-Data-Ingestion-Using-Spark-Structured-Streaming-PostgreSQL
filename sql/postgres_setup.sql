-- Create database
CREATE DATABASE ecommerce_db;


-- Create events table
CREATE TABLE events (
    user_id BIGINT NOT NULL,
    action VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    product_name VARCHAR(100),
    price NUMERIC(10,2),
    timestamp TIMESTAMP NOT NULL
);

-- Index for fast queries 
CREATE INDEX idx_timestamp ON events (timestamp);
CREATE INDEX idx_user_id ON events (user_id);