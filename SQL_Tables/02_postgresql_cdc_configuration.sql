-- ============================================================
-- PostgreSQL CDC Configuration
-- Project: Debezium + Kafka + Spark Structured Streaming CDC
-- Description:
-- Configures PostgreSQL Logical Replication for Debezium.
-- ============================================================

-- ============================================================
-- STEP 1
-- Enable Logical Replication
-- ============================================================

ALTER SYSTEM SET wal_level = logical;

-- Restart PostgreSQL after changing wal_level.

-- ============================================================
-- STEP 2
-- Create Publication
-- ============================================================

CREATE PUBLICATION dbz_publication
FOR TABLE public.person;

-- ============================================================
-- STEP 3
-- Add Additional Tables
-- ============================================================

ALTER PUBLICATION dbz_publication
ADD TABLE public.weather_data;

-- ============================================================
-- STEP 4
-- Verify Publication
-- ============================================================

SELECT *
FROM pg_publication_tables
WHERE pubname = 'dbz_publication';
