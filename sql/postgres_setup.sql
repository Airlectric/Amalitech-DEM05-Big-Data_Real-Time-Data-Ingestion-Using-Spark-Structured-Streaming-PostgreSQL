
-- Step 1: Terminate all existing connections to the database
-- (required before dropping in PostgreSQL)

SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'ecommerce_database'
  AND pid <> pg_backend_pid();


-- Step 2: Drop the database if it already exists

DROP DATABASE IF EXISTS ecommerce_database;


-- Step 3: Create fresh database

CREATE DATABASE ecommerce_database;


-- Step 4: Connect to the new database

\c ecommerce_database


-- Step 5: Create the improved events table

CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,              
    
    user_id         BIGINT,
    action          VARCHAR(50) NOT NULL,
    product_id      BIGINT,
    product_name    VARCHAR(100) NOT NULL,
    price           NUMERIC(12,2) NOT NULL DEFAULT 0,
    event_time      TIMESTAMP WITH TIME ZONE NOT NULL,
    session_id      VARCHAR(50) NOT NULL DEFAULT 'unknown',
    ingestion_time  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraint to enforce valid action values
    CONSTRAINT valid_action 
        CHECK (action IN ('VIEW', 'PURCHASE', 'ADD_TO_CART', 'REMOVE_FROM_CART', 'UNKNOWN'))
);


-- Step 6: Creating useful indexes


-- For time-based queries and recent data
CREATE INDEX idx_event_time ON events (event_time DESC);

-- For ingestion monitoring / auditing
CREATE INDEX idx_ingestion_time ON events (ingestion_time DESC);

-- For filtering by action
CREATE INDEX idx_action ON events (action);

-- For user behavior analysis
CREATE INDEX idx_user_action ON events (user_id, action);

-- composite index useful for product performance
CREATE INDEX idx_product_action ON events (product_id, action);
