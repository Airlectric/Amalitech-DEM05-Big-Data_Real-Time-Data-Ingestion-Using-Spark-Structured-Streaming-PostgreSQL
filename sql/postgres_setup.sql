-- Step 1: Create the database 
CREATE DATABASE ecommerce_database;

-- Step 2: Connect to the new database
\c ecommerce_database;


-- Step 3: Create tables inside the new database
CREATE TABLE events (
    user_id BIGINT NOT NULL,
    action VARCHAR(50) NOT NULL,
    product_id BIGINT NOT NULL,
    product_name VARCHAR(100),
    price NUMERIC(10,2),
    timestamp TIMESTAMP NOT NULL
);

-- Step 4: Create indexes to optimize queries
CREATE INDEX idx_timestamp ON events (timestamp);
CREATE INDEX idx_user_id ON events (user_id);
CREATE INDEX idx_action ON events (action);