-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================
-- DIMENSION TABLES
-- ============================================
-- Note: Tables created in order to satisfy foreign key dependencies

-- Simple tariff rates (no dependencies)
CREATE TABLE IF NOT EXISTS dim_tariff_rates (
    tariff_type VARCHAR(20) PRIMARY KEY,
    base_rate_per_kwh DECIMAL(6,4) NOT NULL,  -- $ per kWh
    peak_rate_per_kwh DECIMAL(6,4),           -- For time_of_use plans
    off_peak_rate_per_kwh DECIMAL(6,4),       -- For time_of_use plans
    peak_start_hour INTEGER DEFAULT 16 CHECK (peak_start_hour >= 0 AND peak_start_hour < 24),
    peak_end_hour INTEGER DEFAULT 21 CHECK (peak_end_hour >= 0 AND peak_end_hour < 24)
);

-- Insert default tariff rates
INSERT INTO dim_tariff_rates (tariff_type, base_rate_per_kwh, peak_rate_per_kwh, off_peak_rate_per_kwh)
VALUES
    ('standard', 0.28, NULL, NULL),
    ('time_of_use', 0.26, 0.30, 0.22),
    ('ev_rate', 0.27, 0.32, 0.20)
ON CONFLICT (tariff_type) DO NOTHING;

-- Grid zones for regional aggregation (no dependencies)
CREATE TABLE IF NOT EXISTS dim_grid_zones (
    grid_zone_id INTEGER PRIMARY KEY,
    zone_name VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL,
    zone_type VARCHAR(20) CHECK (zone_type IN ('urban', 'suburban', 'rural')),
    max_capacity_megawatts INTEGER,  -- Renamed for clarity (was max_capacity_mw)
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_dim_grid_zones_region ON dim_grid_zones(region);

-- Customer dimension table (references dim_tariff_rates)
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200),
    customer_type VARCHAR(20) CHECK (customer_type IN ('residential', 'commercial', 'industrial')),
    account_status VARCHAR(20) DEFAULT 'active' CHECK (account_status IN ('active', 'inactive', 'suspended')),
    tariff_type VARCHAR(20) DEFAULT 'standard' CHECK (tariff_type IN ('standard', 'time_of_use', 'ev_rate')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT fk_customers_tariff FOREIGN KEY (tariff_type) REFERENCES dim_tariff_rates(tariff_type)
);

-- Meter metadata/dimension table (references dim_customers and dim_grid_zones)
CREATE TABLE IF NOT EXISTS dim_meters (
    meter_id INTEGER PRIMARY KEY,           -- Metering point ID
    meter_idn VARCHAR(50) NOT NULL,         -- Manufacturer assigned meter identification number
    customer_id INTEGER NOT NULL,
    melo VARCHAR(50),                       -- Messlokation (metering location identifier)
    malo_cons INTEGER,                      -- Marktlokation consumption
    malo_prod INTEGER,                      -- Marktlokation production

    -- Meter specifications
    meter_type VARCHAR(20) NOT NULL CHECK (meter_type IN ('residential', 'commercial', 'industrial')),
    current_transformation_factor INTEGER DEFAULT 1,   -- e.g., 200 means actual current is 200x measured
    voltage_transformation_factor INTEGER DEFAULT 1,

    -- Gateway information
    gateway_idn VARCHAR(50),                -- Smart meter gateway identifier

    -- Geographic assignment
    grid_zone_id INTEGER,                   -- Foreign key to dim_grid_zones

    -- Metadata
    installation_date TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Foreign key constraints
    CONSTRAINT fk_meters_customer FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    CONSTRAINT fk_meters_grid_zone FOREIGN KEY (grid_zone_id) REFERENCES dim_grid_zones(grid_zone_id)
);

CREATE INDEX idx_dim_meters_customer_id ON dim_meters(customer_id);
CREATE INDEX idx_dim_meters_type ON dim_meters(meter_type);
CREATE INDEX idx_dim_meters_gateway ON dim_meters(gateway_idn);
CREATE INDEX idx_dim_meters_melo ON dim_meters(melo);
CREATE INDEX idx_dim_meters_grid_zone ON dim_meters(grid_zone_id);

-- ============================================
-- FACT TABLES (Time-series data)
-- ============================================

-- Raw meter readings table (main fact table)
-- Storing readings in milliwatts to avoid decimals and maintain precision
-- Supports both consumption and production (e.g., solar panels)
CREATE TABLE IF NOT EXISTS raw_meter_readings (
    reading_timestamp TIMESTAMPTZ NOT NULL,
    meter_id INTEGER NOT NULL,
    reading_consumption_milliwatts BIGINT CHECK (reading_consumption_milliwatts >= 0),  -- Energy consumed from grid
    reading_production_milliwatts BIGINT CHECK (reading_production_milliwatts >= 0),    -- Energy produced (e.g., solar)
    status VARCHAR(20) DEFAULT 'valid' CHECK (status IN ('valid', 'estimated', 'error')),
    arrived_at TIMESTAMPTZ DEFAULT NOW(),  -- When reading was received/ingested

    -- At least one reading must be present
    CONSTRAINT chk_readings_present CHECK (
        reading_consumption_milliwatts IS NOT NULL OR
        reading_production_milliwatts IS NOT NULL
    )
);

-- Create hypertable for time-series optimization
-- Using 1-day chunks for better daily analytics performance (changed from 7 days)
SELECT create_hypertable('raw_meter_readings', 'reading_timestamp',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Add composite primary key AFTER hypertable creation
-- This ensures uniqueness: one reading per meter per timestamp
ALTER TABLE raw_meter_readings
    ADD CONSTRAINT pk_raw_meter_readings
    PRIMARY KEY (reading_timestamp, meter_id);

-- Foreign key constraint intentionally omitted for raw_meter_readings
-- Rationale: High-volume streaming data should be ingested without referential integrity checks
-- Data quality and referential integrity will be enforced in the transformation layer (dbt)
-- This ensures maximum ingestion performance and prevents data loss from missing dimension records

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_raw_readings_meter_time
    ON raw_meter_readings (meter_id, reading_timestamp DESC);

-- Index for data quality monitoring
CREATE INDEX IF NOT EXISTS idx_raw_readings_status
    ON raw_meter_readings (status, reading_timestamp DESC)
    WHERE status != 'valid';

-- ============================================
-- COMPRESSION POLICY
-- ============================================

-- Enable compression on the hypertable with optimization
ALTER TABLE raw_meter_readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'meter_id',
    timescaledb.compress_orderby = 'reading_timestamp DESC'  -- Added for better compression
);

-- Compress data older than 7 days to save storage
SELECT add_compression_policy('raw_meter_readings',
    INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ============================================
-- GRANTS
-- ============================================

-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO meter_admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO meter_admin;
