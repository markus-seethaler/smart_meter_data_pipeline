-- ============================================
-- POPULATE DIMENSION TABLES
-- ============================================
-- This script runs after 01_create_schema.sql
-- Generates realistic dimension data for 1M meters with proper relationships:
--   - Residential ONLY: 1 customer = 1 meter (1:1)

-- ============================================
-- GRID ZONES
-- ============================================
-- Create 20 grid zones across different regions
INSERT INTO dim_grid_zones (grid_zone_id, zone_name, region, zone_type, max_capacity_megawatts)
VALUES
    -- Urban zones (high capacity)
    (1, 'Downtown Core', 'Central', 'urban', 500),
    (2, 'North Business District', 'Central', 'urban', 450),
    (3, 'South Commercial Hub', 'Central', 'urban', 480),
    (4, 'East Financial District', 'East', 'urban', 420),

    -- Suburban zones (medium capacity)
    (5, 'North Residential Area', 'North', 'suburban', 300),
    (6, 'Northeast Suburbs', 'North', 'suburban', 280),
    (7, 'Northwest Suburbs', 'North', 'suburban', 290),
    (8, 'South Residential Area', 'South', 'suburban', 320),
    (9, 'Southeast Suburbs', 'South', 'suburban', 310),
    (10, 'Southwest Suburbs', 'South', 'suburban', 300),
    (11, 'West Residential Area', 'West', 'suburban', 340),
    (12, 'East Residential Area', 'East', 'suburban', 330),

    -- Rural zones (lower capacity)
    (13, 'North Rural District', 'North', 'rural', 150),
    (14, 'South Rural District', 'South', 'rural', 140),
    (15, 'East Rural District', 'East', 'rural', 160),
    (16, 'West Rural District', 'West', 'rural', 155),

    -- Industrial zones (high capacity)
    (17, 'Industrial Park North', 'North', 'urban', 600),
    (18, 'Industrial Park South', 'South', 'urban', 580),
    (19, 'Port Industrial Zone', 'East', 'urban', 550),
    (20, 'Airport Industrial Zone', 'West', 'urban', 520)
ON CONFLICT (grid_zone_id) DO NOTHING;

-- ============================================
-- CUSTOMERS
-- ============================================
-- Generate 1,000,000 residential customers (1:1 with meters)

INSERT INTO dim_customers (customer_id, customer_name, account_status)
SELECT
    gs AS customer_id,
    'Customer-' || LPAD(gs::TEXT, 7, '0') AS customer_name,
    CASE
        WHEN gs % 200 = 0 THEN 'inactive'    -- 0.5% inactive
        WHEN gs % 500 = 0 THEN 'suspended'   -- 0.2% suspended
        ELSE 'active'                         -- 99.3% active
    END AS account_status
FROM generate_series(1, 1000000) AS gs
ON CONFLICT (customer_id) DO NOTHING;

-- ============================================
-- METERS
-- ============================================
-- Generate 1M residential meters (1:1 with customers)
-- 50% of ALL meters have solar production capability

INSERT INTO dim_meters (
    meter_id,
    meter_idn,
    customer_id,
    melo,
    malo_cons,
    malo_prod,
    gateway_idn,
    grid_zone_id,
    installation_date
)
SELECT
    gs AS meter_id,
    'MTR-' || LPAD(gs::TEXT, 10, '0') AS meter_idn,
    gs AS customer_id,  -- 1:1 relationship
    'MELO-' || LPAD(gs::TEXT, 10, '0') AS melo,
    10000000 + gs AS malo_cons,
    CASE
        WHEN gs % 2 = 0 THEN 20000000 + gs  -- 50% have solar production
        ELSE NULL
    END AS malo_prod,
    'GW-' || LPAD((gs % 5000 + 1)::TEXT, 6, '0') AS gateway_idn,  -- ~200 meters per gateway
    (gs % 16) + 5 AS grid_zone_id,  -- Zones 5-20 (suburban, rural, and some urban)
    NOW() - (random() * INTERVAL '10 years') AS installation_date
FROM generate_series(1, 1000000) AS gs
ON CONFLICT (meter_id) DO NOTHING;

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

DO $$
DECLARE
    grid_zone_count INTEGER;
    customer_count INTEGER;
    meter_count INTEGER;
    solar_meter_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO grid_zone_count FROM dim_grid_zones;
    SELECT COUNT(*) INTO customer_count FROM dim_customers;
    SELECT COUNT(*) INTO meter_count FROM dim_meters;
    SELECT COUNT(*) INTO solar_meter_count FROM dim_meters WHERE malo_prod IS NOT NULL;

    RAISE NOTICE '';
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'DIMENSION TABLE POPULATION COMPLETE';
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Grid Zones:              %', grid_zone_count;
    RAISE NOTICE '';
    RAISE NOTICE 'CUSTOMERS (Residential): %', customer_count;
    RAISE NOTICE '';
    RAISE NOTICE 'METERS (Residential):    %', meter_count;
    RAISE NOTICE '  - With Solar/Prod:     % (%.1f%%) ← SOLAR COMPANY!', solar_meter_count, solar_meter_count::NUMERIC / meter_count * 100;
    RAISE NOTICE '';
    RAISE NOTICE 'RELATIONSHIP MODEL:';
    RAISE NOTICE '  ✓ 1 customer = 1 meter (residential only)';
    RAISE NOTICE '  ✓ Solar:       50%% of all meters have production capability';
    RAISE NOTICE '================================================================';
    RAISE NOTICE '';
END $$;