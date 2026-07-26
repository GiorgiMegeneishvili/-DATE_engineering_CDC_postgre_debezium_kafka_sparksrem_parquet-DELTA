-- ============================================================
-- PostgreSQL Tables & Sample CDC Operations
-- Project: Debezium + Kafka + Spark Structured Streaming CDC
-- Description:
-- Creates sample tables and executes INSERT, UPDATE,
-- and DELETE operations to generate CDC events.
-- ============================================================

-- ============================================================
-- PERSON TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS public.person
(
    business_entity_id INTEGER PRIMARY KEY,
    person_type CHAR(2),
    name_style BOOLEAN,
    title VARCHAR(20),
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    suffix VARCHAR(20),
    email_promotion INTEGER,
    modified_date DATE,
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- WEATHER_DATA TABLE
-- ============================================================

CREATE TABLE IF NOT EXISTS public.weather_data
(
    id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    temperature NUMERIC(5,2) NOT NULL,
    humidity INTEGER NOT NULL,
    weather VARCHAR(100),
    event_time TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INSERT Operations
-- ============================================================

INSERT INTO public.person
(
    business_entity_id,
    person_type,
    first_name,
    last_name,
    modified_date,
    inserted_date
)
VALUES
(
    10000579,
    'IN',
    'Gio_mege',
    'Python CDC',
    CURRENT_DATE,
    CURRENT_TIMESTAMP
);

-- ============================================================
-- UPDATE Operations
-- ============================================================

UPDATE public.person
SET first_name = 'Gio_gio'
WHERE business_entity_id = 10000579;

UPDATE public.weather_data
SET city = 'Tbilisi'
WHERE id = 743;

-- ============================================================
-- DELETE Operations
-- ============================================================

DELETE FROM public.person
WHERE business_entity_id = 10000579;

-- ============================================================
-- Notes
-- ============================================================
-- Every INSERT, UPDATE and DELETE operation is captured
-- by Debezium and published to Apache Kafka.
--
-- Spark Structured Streaming consumes these events
-- and stores them in Delta Lake (Bronze layer).
-- ============================================================
