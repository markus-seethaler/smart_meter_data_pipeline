-- ============================================
-- POPULATE DIMENSION TABLES
-- ============================================
-- This script runs after 01_create_schema.sql
-- Generates realistic dimension data for 1M meters with proper relationships:
--   - Residential: 1 customer = 1 meter (1:1)
--   - Commercial: 1 customer = ~12 meters avg (1:many)
--   - Industrial: 1 customer = ~30 meters avg (1:many)
--   - Solar: 50% of all meters have solar production (we're a solar company!)

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
-- Generate 861,000 customers:
--   - 850,000 residential (customer_id 1-850000)
--   - 10,000 commercial (customer_id 850001-860000)
--   - 1,000 industrial (customer_id 860001-861000)

-- Residential customers (850,000)
INSERT INTO dim_customers (customer_id, customer_name, customer_type, account_status, tariff_type)
SELECT
    gs AS customer_id,
    'Customer-' || LPAD(gs::TEXT, 7, '0') AS customer_name,
    'residential' AS customer_type,
    CASE
        WHEN gs % 200 = 0 THEN 'inactive'    -- 0.5% inactive
        WHEN gs % 500 = 0 THEN 'suspended'   -- 0.2% suspended
        ELSE 'active'                         -- 99.3% active
    END AS account_status,
    CASE
        WHEN gs % 10 < 7 THEN 'standard'      -- 70%
        WHEN gs % 10 < 9 THEN 'time_of_use'   -- 20%
        ELSE 'ev_rate'                         -- 10%
    END AS tariff_type
FROM generate_series(1, 850000) AS gs
ON CONFLICT (customer_id) DO NOTHING;

-- Commercial customers (10,000)
INSERT INTO dim_customers (customer_id, customer_name, customer_type, account_status, tariff_type)
SELECT
    gs AS customer_id,
    'Commercial-' || LPAD((gs - 850000)::TEXT, 5, '0') AS customer_name,
    'commercial' AS customer_type,
    CASE
        WHEN gs % 100 = 0 THEN 'inactive'
        ELSE 'active'
    END AS account_status,
    CASE
        WHEN gs % 10 < 3 THEN 'standard'      -- 30%
        ELSE 'time_of_use'                     -- 70% (commercial prefers ToU)
    END AS tariff_type
FROM generate_series(850001, 860000) AS gs
ON CONFLICT (customer_id) DO NOTHING;

-- Industrial customers (1,000)
INSERT INTO dim_customers (customer_id, customer_name, customer_type, account_status, tariff_type)
SELECT
    gs AS customer_id,
    'Industrial-' || LPAD((gs - 860000)::TEXT, 4, '0') AS customer_name,
    'industrial' AS customer_type,
    'active' AS account_status,  -- Industrial customers always active
    'time_of_use' AS tariff_type  -- All industrial on ToU rates
FROM generate_series(860001, 861000) AS gs
ON CONFLICT (customer_id) DO NOTHING;

-- ============================================
-- METERS
-- ============================================
-- Generate 1M meters with realistic customer relationships:
--   - Meters 1-850,000: Residential (1:1 with customers 1-850,000)
--   - Meters 850,001-970,000: Commercial (~12 per customer)
--   - Meters 970,001-1,000,000: Industrial (~30 per customer)
--   - 50% of ALL meters have solar production capability

-- Residential meters (850,000 meters, 1:1 relationship)
INSERT INTO dim_meters (
    meter_id,
    meter_idn,
    customer_id,
    melo,
    malo_cons,
    malo_prod,
    meter_type,
    current_transformation_factor,
    voltage_transformation_factor,
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
    'residential' AS meter_type,
    1 AS current_transformation_factor,  -- Direct measurement
    1 AS voltage_transformation_factor,
    'GW-' || LPAD((gs % 5000 + 1)::TEXT, 6, '0') AS gateway_idn,  -- ~170 meters per gateway
    (gs % 12) + 5 AS grid_zone_id,  -- Zones 5-16 (suburban and rural)
    NOW() - (random() * INTERVAL '10 years') AS installation_date
FROM generate_series(1, 850000) AS gs
ON CONFLICT (meter_id) DO NOTHING;

-- Commercial meters (120,000 meters for 10,000 customers, avg ~12 per customer)
-- Each commercial customer gets 5-20 meters (varying)
INSERT INTO dim_meters (
    meter_id,
    meter_idn,
    customer_id,
    melo,
    malo_cons,
    malo_prod,
    meter_type,
    current_transformation_factor,
    voltage_transformation_factor,
    gateway_idn,
    grid_zone_id,
    installation_date
)
SELECT
    gs AS meter_id,
    'MTR-' || LPAD(gs::TEXT, 10, '0') AS meter_idn,
    -- Assign customer_id: distribute 120k meters across 10k customers
    -- Formula: 850001 + ((meter_id - 850001) / 12) maps ~12 meters to each customer
    850001 + ((gs - 850001) / 12)::INTEGER AS customer_id,
    'MELO-' || LPAD(gs::TEXT, 10, '0') AS melo,
    10000000 + gs AS malo_cons,
    CASE
        WHEN gs % 2 = 0 THEN 20000000 + gs  -- 50% have solar production
        ELSE NULL
    END AS malo_prod,
    'commercial' AS meter_type,
    50 AS current_transformation_factor,  -- 50x transformation
    1 AS voltage_transformation_factor,
    'GW-' || LPAD((gs % 5000 + 1)::TEXT, 6, '0') AS gateway_idn,
    CASE
        WHEN gs % 5 = 0 THEN (gs % 4) + 1   -- 20% in urban zones (1-4)
        ELSE (gs % 8) + 5                    -- 80% in suburban zones (5-12)
    END AS grid_zone_id,
    NOW() - (random() * INTERVAL '15 years') AS installation_date
FROM generate_series(850001, 970000) AS gs
ON CONFLICT (meter_id) DO NOTHING;

-- Industrial meters (30,000 meters for 1,000 customers, avg ~30 per customer)
-- Each industrial customer gets 15-50 meters (varying)
INSERT INTO dim_meters (
    meter_id,
    meter_idn,
    customer_id,
    melo,
    malo_cons,
    malo_prod,
    meter_type,
    current_transformation_factor,
    voltage_transformation_factor,
    gateway_idn,
    grid_zone_id,
    installation_date
)
SELECT
    gs AS meter_id,
    'MTR-' || LPAD(gs::TEXT, 10, '0') AS meter_idn,
    -- Assign customer_id: distribute 30k meters across 1k customers
    -- Formula: 860001 + ((meter_id - 970001) / 30) maps ~30 meters to each customer
    860001 + ((gs - 970001) / 30)::INTEGER AS customer_id,
    'MELO-' || LPAD(gs::TEXT, 10, '0') AS melo,
    10000000 + gs AS malo_cons,
    CASE
        WHEN gs % 2 = 0 THEN 20000000 + gs  -- 50% have production (co-gen, solar)
        ELSE NULL
    END AS malo_prod,
    'industrial' AS meter_type,
    200 AS current_transformation_factor,  -- 200x transformation
    1 AS voltage_transformation_factor,
    'GW-' || LPAD((gs % 1000 + 1)::TEXT, 6, '0') AS gateway_idn,
    (gs % 4) + 17 AS grid_zone_id,  -- Industrial zones 17-20
    NOW() - (random() * INTERVAL '20 years') AS installation_date
FROM generate_series(970001, 1000000) AS gs
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
    residential_customers INTEGER;
    commercial_customers INTEGER;
    industrial_customers INTEGER;
    residential_meters INTEGER;
    commercial_meters INTEGER;
    industrial_meters INTEGER;
    avg_commercial_meters NUMERIC;
    avg_industrial_meters NUMERIC;
BEGIN
    SELECT COUNT(*) INTO grid_zone_count FROM dim_grid_zones;
    SELECT COUNT(*) INTO customer_count FROM dim_customers;
    SELECT COUNT(*) INTO meter_count FROM dim_meters;
    SELECT COUNT(*) INTO solar_meter_count FROM dim_meters WHERE malo_prod IS NOT NULL;

    -- Customer counts
    SELECT COUNT(*) INTO residential_customers FROM dim_customers WHERE customer_type = 'residential';
    SELECT COUNT(*) INTO commercial_customers FROM dim_customers WHERE customer_type = 'commercial';
    SELECT COUNT(*) INTO industrial_customers FROM dim_customers WHERE customer_type = 'industrial';

    -- Meter counts
    SELECT COUNT(*) INTO residential_meters FROM dim_meters WHERE meter_type = 'residential';
    SELECT COUNT(*) INTO commercial_meters FROM dim_meters WHERE meter_type = 'commercial';
    SELECT COUNT(*) INTO industrial_meters FROM dim_meters WHERE meter_type = 'industrial';

    -- Calculate averages
    IF commercial_customers > 0 THEN
        avg_commercial_meters := commercial_meters::NUMERIC / commercial_customers;
    ELSE
        avg_commercial_meters := 0;
    END IF;

    IF industrial_customers > 0 THEN
        avg_industrial_meters := industrial_meters::NUMERIC / industrial_customers;
    ELSE
        avg_industrial_meters := 0;
    END IF;

    RAISE NOTICE '';
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'DIMENSION TABLE POPULATION COMPLETE';
    RAISE NOTICE '================================================================';
    RAISE NOTICE 'Grid Zones:              %', grid_zone_count;
    RAISE NOTICE '';
    RAISE NOTICE 'CUSTOMERS:               %', customer_count;
    RAISE NOTICE '  - Residential:         % (1:1 with meters)', residential_customers;
    RAISE NOTICE '  - Commercial:          % (1:many with meters)', commercial_customers;
    RAISE NOTICE '  - Industrial:          % (1:many with meters)', industrial_customers;
    RAISE NOTICE '';
    RAISE NOTICE 'METERS:                  %', meter_count;
    RAISE NOTICE '  - Residential:         % (%.1f per customer)', residential_meters, residential_meters::NUMERIC / NULLIF(residential_customers, 0);
    RAISE NOTICE '  - Commercial:          % (%.1f per customer)', commercial_meters, avg_commercial_meters;
    RAISE NOTICE '  - Industrial:          % (%.1f per customer)', industrial_meters, avg_industrial_meters;
    RAISE NOTICE '  - With Solar/Prod:     % (%.1f%%) ← SOLAR COMPANY!', solar_meter_count, solar_meter_count::NUMERIC / meter_count * 100;
    RAISE NOTICE '';
    RAISE NOTICE 'RELATIONSHIP MODEL:';
    RAISE NOTICE '  ✓ Residential: 1 customer = 1 meter';
    RAISE NOTICE '  ✓ Commercial:  1 customer = ~12 meters (avg)';
    RAISE NOTICE '  ✓ Industrial:  1 customer = ~30 meters (avg)';
    RAISE NOTICE '  ✓ Solar:       50%% of all meters have production capability';
    RAISE NOTICE '================================================================';
    RAISE NOTICE '';
END $$;

-- ============================================
-- HELPER VIEWS
-- ============================================

-- Summary view for quick dimension checks
CREATE OR REPLACE VIEW v_dimension_summary AS
SELECT
    'Grid Zones' AS dimension,
    COUNT(*)::TEXT AS count,
    NULL AS details
FROM dim_grid_zones
UNION ALL
SELECT
    'Customers',
    COUNT(*)::TEXT,
    json_build_object(
        'residential', COUNT(*) FILTER (WHERE customer_type = 'residential'),
        'commercial', COUNT(*) FILTER (WHERE customer_type = 'commercial'),
        'industrial', COUNT(*) FILTER (WHERE customer_type = 'industrial')
    )::TEXT
FROM dim_customers
UNION ALL
SELECT
    'Meters',
    COUNT(*)::TEXT,
    json_build_object(
        'residential', COUNT(*) FILTER (WHERE meter_type = 'residential'),
        'commercial', COUNT(*) FILTER (WHERE meter_type = 'commercial'),
        'industrial', COUNT(*) FILTER (WHERE meter_type = 'industrial'),
        'with_solar', COUNT(*) FILTER (WHERE malo_prod IS NOT NULL),
        'solar_pct', ROUND(COUNT(*) FILTER (WHERE malo_prod IS NOT NULL)::NUMERIC / COUNT(*) * 100, 1)
    )::TEXT
FROM dim_meters;

-- View to show customer-to-meter relationships
CREATE OR REPLACE VIEW v_customer_meter_counts AS
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_type,
    COUNT(m.meter_id) AS meter_count,
    COUNT(m.malo_prod) AS meters_with_production,
    MIN(m.meter_id) AS first_meter_id,
    MAX(m.meter_id) AS last_meter_id
FROM dim_customers c
LEFT JOIN dim_meters m ON c.customer_id = m.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_type
ORDER BY c.customer_id;

-- View to show top customers by meter count
CREATE OR REPLACE VIEW v_top_customers_by_meters AS
SELECT
    c.customer_id,
    c.customer_name,
    c.customer_type,
    COUNT(m.meter_id) AS meter_count,
    COUNT(m.malo_prod) AS meters_with_production
FROM dim_customers c
LEFT JOIN dim_meters m ON c.customer_id = m.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_type
ORDER BY meter_count DESC
LIMIT 100;

-- Grant access to all views
GRANT SELECT ON v_dimension_summary TO meter_admin;
GRANT SELECT ON v_customer_meter_counts TO meter_admin;
GRANT SELECT ON v_top_customers_by_meters TO meter_admin;
