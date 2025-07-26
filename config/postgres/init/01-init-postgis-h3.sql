-- GeoRetail PostGIS Database Initialization with H3
-- File: config/postgres/init/01-init-postgis-h3.sql

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS h3;

-- Create schemas for organized data storage
CREATE SCHEMA IF NOT EXISTS georetail;
CREATE SCHEMA IF NOT EXISTS h3_data;
CREATE SCHEMA IF NOT EXISTS osm_cache;
CREATE SCHEMA IF NOT EXISTS demographics;
CREATE SCHEMA IF NOT EXISTS traffic;

-- Grant permissions to georetail_user
GRANT ALL PRIVILEGES ON SCHEMA georetail TO georetail_user;
GRANT ALL PRIVILEGES ON SCHEMA h3_data TO georetail_user;
GRANT ALL PRIVILEGES ON SCHEMA osm_cache TO georetail_user;
GRANT ALL PRIVILEGES ON SCHEMA demographics TO georetail_user;
GRANT ALL PRIVILEGES ON SCHEMA traffic TO georetail_user;

-- ==========================================
-- H3 GRID TABLES
-- ==========================================

-- Ukraine H3 Grid (multi-resolution)
CREATE TABLE IF NOT EXISTS h3_data.ukraine_grid (
    id SERIAL PRIMARY KEY,
    hex_id TEXT NOT NULL UNIQUE,
    resolution INTEGER NOT NULL,
    center_point GEOMETRY(POINT, 4326) NOT NULL,
    boundary GEOMETRY(POLYGON, 4326) NOT NULL,
    area_km2 DECIMAL(10,6),
    city_name VARCHAR(100),
    region_name VARCHAR(100),
    oblast_name VARCHAR(100),
    country_code VARCHAR(2) DEFAULT 'UA',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- H3 Computed Metrics (main analytical table)  
CREATE TABLE IF NOT EXISTS h3_data.hexagon_metrics (
    hex_id TEXT PRIMARY KEY REFERENCES h3_data.ukraine_grid(hex_id),
    
    -- Basic Geographic Metrics
    poi_count INTEGER DEFAULT 0,
    poi_density DECIMAL(8,2) DEFAULT 0,
    retail_poi_count INTEGER DEFAULT 0,
    retail_density DECIMAL(8,2) DEFAULT 0,
    food_poi_count INTEGER DEFAULT 0,
    food_density DECIMAL(8,2) DEFAULT 0,
    
    -- Transport & Accessibility
    transport_poi_count INTEGER DEFAULT 0,
    transport_accessibility DECIMAL(8,2) DEFAULT 0,
    road_density DECIMAL(8,2) DEFAULT 0,
    parking_availability DECIMAL(6,3) DEFAULT 0,
    
    -- Competition Metrics
    competition_intensity DECIMAL(8,3) DEFAULT 0,
    market_saturation DECIMAL(8,3) DEFAULT 0,
    competitor_count INTEGER DEFAULT 0,
    direct_competitor_count INTEGER DEFAULT 0,
    substitute_competitor_count INTEGER DEFAULT 0,
    avg_competitor_distance DECIMAL(8,3) DEFAULT 0,
    
    -- Traffic Metrics (TomTom API)
    avg_traffic_flow DECIMAL(10,2),
    peak_congestion_index DECIMAL(6,3),
    accessibility_score DECIMAL(8,3),
    rush_hour_multiplier DECIMAL(5,3),
    weekend_traffic_ratio DECIMAL(5,3),
    
    -- Demographics
    population_count INTEGER DEFAULT 0,
    population_density DECIMAL(10,2) DEFAULT 0,
    income_level_index DECIMAL(6,3),
    age_diversity_score DECIMAL(6,3),
    household_count INTEGER DEFAULT 0,
    
    -- Business Environment
    business_diversity_index DECIMAL(6,3),
    foot_traffic_estimate DECIMAL(10,2),
    commercial_activity_score DECIMAL(8,3),
    
    -- ML Predictions & Scoring
    predicted_revenue DECIMAL(12,2),
    revenue_confidence_score DECIMAL(5,3),
    location_attractiveness_score DECIMAL(8,3),
    risk_category VARCHAR(20),
    investment_priority_rank INTEGER,
    
    -- Graph Embeddings (stored as JSON arrays)
    fastrp_embedding JSONB,
    graphsage_embedding JSONB,
    node2vec_embedding JSONB,
    combined_embedding JSONB,
    
    -- Metadata
    last_calculated TIMESTAMP DEFAULT NOW(),
    data_sources JSONB,
    calculation_version VARCHAR(10) DEFAULT '1.0',
    
    -- Constraints
    CONSTRAINT valid_scores CHECK (
        predicted_revenue >= 0 AND
        revenue_confidence_score BETWEEN 0 AND 1 AND
        location_attractiveness_score >= 0
    )
);

-- ==========================================
-- STORE DATA TABLES
-- ==========================================

-- Current Network Stores (flexible schema for different retail chains)
CREATE TABLE IF NOT EXISTS georetail.stores (
    shop_number INTEGER PRIMARY KEY,
    shop_name VARCHAR(255),
    region VARCHAR(100),
    locality VARCHAR(100),
    opening_date DATE,
    
    -- Location
    lat DECIMAL(10,8) NOT NULL,
    lon DECIMAL(11,8) NOT NULL,
    point GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (ST_Point(lon, lat)) STORED,
    address_full TEXT,
    
    -- Store Characteristics  
    format VARCHAR(50),
    location_features VARCHAR(100),
    
    -- Area & Layout
    square_trade DECIMAL(8,2),
    square_total DECIMAL(8,2),
    qntty_sku INTEGER,
    qntty_clusters INTEGER,
    
    -- Specialized Departments
    bakery_full_cycle BOOLEAN DEFAULT FALSE,
    bakery_short_cycle BOOLEAN DEFAULT FALSE,
    meat_kg BOOLEAN DEFAULT FALSE,
    meat_pcs BOOLEAN DEFAULT FALSE,
    pizza_available BOOLEAN DEFAULT FALSE,
    
    -- Performance Metrics
    avg_month_n_checks DECIMAL(10,2),
    avg_check_sum DECIMAL(8,2),
    monthly_revenue DECIMAL(12,2),
    
    -- Department Revenues
    bakery_revenue DECIMAL(12,2),
    food_to_go_revenue DECIMAL(12,2),
    meat_revenue DECIMAL(12,2),
    pizza_revenue DECIMAL(12,2),
    
    -- H3 Assignments (multi-resolution) - will be calculated
    h3_res7 TEXT,
    h3_res8 TEXT, 
    h3_res9 TEXT,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,
    
    -- Performance tracking
    performance_category VARCHAR(20), -- 'high', 'medium', 'low'
    benchmark_score DECIMAL(6,3),
    
    -- Additional attributes (flexible JSON)
    custom_attributes JSONB
);

-- ==========================================
-- DEMOGRAPHICS DATA
-- ==========================================

-- Demographics from H3 .gpkg files
CREATE TABLE IF NOT EXISTS demographics.h3_population (
    hex_id TEXT PRIMARY KEY,
    resolution INTEGER NOT NULL,
    population_count INTEGER NOT NULL DEFAULT 0,
    population_density DECIMAL(10,2),
    
    -- Future demographic fields
    age_0_14 INTEGER,
    age_15_64 INTEGER,
    age_65_plus INTEGER,
    household_count INTEGER,
    avg_household_size DECIMAL(4,2),
    income_median DECIMAL(10,2),
    education_higher_pct DECIMAL(5,2),
    
    -- Spatial reference
    geometry GEOMETRY(POLYGON, 4326),
    center_point GEOMETRY(POINT, 4326),
    
    -- Metadata
    data_source VARCHAR(100),
    data_year INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ==========================================
-- OSM CACHE TABLES
-- ==========================================

-- OSM Data Cache for performance
CREATE TABLE IF NOT EXISTS osm_cache.osm_extracts (
    cache_id SERIAL PRIMARY KEY,
    bbox GEOMETRY(POLYGON, 4326) NOT NULL,
    center_lat DECIMAL(10,8),
    center_lon DECIMAL(11,8),
    radius_meters INTEGER,
    
    -- Cached OSM data
    osm_data JSONB NOT NULL,
    pois_count INTEGER,
    roads_count INTEGER,
    buildings_count INTEGER,
    
    -- Cache management
    extracted_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '7 days',
    cache_hits INTEGER DEFAULT 0,
    
    -- Index for spatial queries
    CONSTRAINT valid_bbox CHECK (ST_IsValid(bbox))
);

-- ==========================================
-- TRAFFIC DATA TABLES
-- ==========================================

-- TomTom Traffic Data Cache
CREATE TABLE IF NOT EXISTS traffic.tomtom_traffic (
    id SERIAL PRIMARY KEY,
    hex_id TEXT REFERENCES h3_data.ukraine_grid(hex_id),
    
    -- Traffic Metrics
    current_speed_kmh DECIMAL(6,2),
    free_flow_speed_kmh DECIMAL(6,2),
    congestion_ratio DECIMAL(5,3),
    travel_time_minutes DECIMAL(8,2),
    
    -- Time-based patterns
    hour_of_day INTEGER CHECK (hour_of_day BETWEEN 0 AND 23),
    day_of_week INTEGER CHECK (day_of_week BETWEEN 1 AND 7),
    is_weekend BOOLEAN,
    
    -- Seasonal patterns
    month_of_year INTEGER CHECK (month_of_year BETWEEN 1 AND 12),
    is_holiday BOOLEAN DEFAULT FALSE,
    weather_condition VARCHAR(50),
    
    -- Data source
    measured_at TIMESTAMP DEFAULT NOW(),
    api_response JSONB,
    
    -- Aggregation support
    is_aggregated BOOLEAN DEFAULT FALSE,
    sample_count INTEGER DEFAULT 1
);

-- ==========================================
-- SPATIAL INDEXES
-- ==========================================

-- H3 Grid Indexes
CREATE INDEX IF NOT EXISTS idx_ukraine_grid_spatial ON h3_data.ukraine_grid USING GIST (boundary);
CREATE INDEX IF NOT EXISTS idx_ukraine_grid_center ON h3_data.ukraine_grid USING GIST (center_point);
CREATE INDEX IF NOT EXISTS idx_ukraine_grid_resolution ON h3_data.ukraine_grid (resolution);
CREATE INDEX IF NOT EXISTS idx_ukraine_grid_hex_id ON h3_data.ukraine_grid (hex_id);

-- Store Indexes
CREATE INDEX IF NOT EXISTS idx_stores_spatial ON georetail.stores USING GIST (point);
CREATE INDEX IF NOT EXISTS idx_stores_h3_res7 ON georetail.stores (h3_res7);
CREATE INDEX IF NOT EXISTS idx_stores_h3_res8 ON georetail.stores (h3_res8);
CREATE INDEX IF NOT EXISTS idx_stores_format ON georetail.stores (format);
CREATE INDEX IF NOT EXISTS idx_stores_performance ON georetail.stores (performance_category);
CREATE INDEX IF NOT EXISTS idx_stores_revenue ON georetail.stores (monthly_revenue DESC);

-- Metrics Indexes  
CREATE INDEX IF NOT EXISTS idx_metrics_revenue ON h3_data.hexagon_metrics (predicted_revenue DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_competition ON h3_data.hexagon_metrics (competition_intensity);
CREATE INDEX IF NOT EXISTS idx_metrics_calculated ON h3_data.hexagon_metrics (last_calculated DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_priority ON h3_data.hexagon_metrics (investment_priority_rank);

-- Demographics Indexes
CREATE INDEX IF NOT EXISTS idx_demographics_spatial ON demographics.h3_population USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_demographics_hex_id ON demographics.h3_population (hex_id);
CREATE INDEX IF NOT EXISTS idx_demographics_population ON demographics.h3_population (population_count DESC);

-- OSM Cache Indexes
CREATE INDEX IF NOT EXISTS idx_osm_cache_spatial ON osm_cache.osm_extracts USING GIST (bbox);
CREATE INDEX IF NOT EXISTS idx_osm_cache_expires ON osm_cache.osm_extracts (expires_at);
CREATE INDEX IF NOT EXISTS idx_osm_cache_center ON osm_cache.osm_extracts (center_lat, center_lon);

-- Traffic Indexes
CREATE INDEX IF NOT EXISTS idx_traffic_hex_id ON traffic.tomtom_traffic (hex_id);
CREATE INDEX IF NOT EXISTS idx_traffic_time ON traffic.tomtom_traffic (measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_patterns ON traffic.tomtom_traffic (hour_of_day, day_of_week);

-- ==========================================
-- H3 HELPER FUNCTIONS
-- ==========================================

-- Function to assign H3 values to stores
CREATE OR REPLACE FUNCTION assign_h3_to_stores() 
RETURNS INTEGER AS $$
DECLARE
    updated_count INTEGER := 0;
BEGIN
    UPDATE georetail.stores SET
        h3_res7 = h3_lat_lng_to_cell(POINT(lon, lat), 7),
        h3_res8 = h3_lat_lng_to_cell(POINT(lon, lat), 8),
        h3_res9 = h3_lat_lng_to_cell(POINT(lon, lat), 9),
        updated_at = NOW()
    WHERE h3_res7 IS NULL OR h3_res8 IS NULL OR h3_res9 IS NULL;
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- Function to generate H3 grid for bounding box
CREATE OR REPLACE FUNCTION generate_h3_grid_for_bbox(
    min_lat DECIMAL, min_lon DECIMAL, 
    max_lat DECIMAL, max_lon DECIMAL, 
    resolution INTEGER DEFAULT 8
) 
RETURNS TABLE(hex_id TEXT, center_point GEOMETRY, boundary GEOMETRY) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h3_lat_lng_to_cell(ST_GeomFromText('POINT(' || x.lon || ' ' || x.lat || ')', 4326), resolution)::TEXT as hex_id,
        ST_GeomFromText('POINT(' || x.lon || ' ' || x.lat || ')', 4326) as center_point,
        h3_cell_to_boundary(h3_lat_lng_to_cell(ST_GeomFromText('POINT(' || x.lon || ' ' || x.lat || ')', 4326), resolution)) as boundary
    FROM (
        SELECT 
            generate_series(min_lat::numeric, max_lat::numeric, 0.01)::DECIMAL as lat,
            generate_series(min_lon::numeric, max_lon::numeric, 0.01)::DECIMAL as lon
    ) x;
END;
$$ LANGUAGE plpgsql;

-- Update triggers
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply update triggers
DROP TRIGGER IF EXISTS update_stores_modtime ON georetail.stores;
CREATE TRIGGER update_stores_modtime 
    BEFORE UPDATE ON georetail.stores 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

DROP TRIGGER IF EXISTS update_ukraine_grid_modtime ON h3_data.ukraine_grid;
CREATE TRIGGER update_ukraine_grid_modtime 
    BEFORE UPDATE ON h3_data.ukraine_grid 
    FOR EACH ROW EXECUTE FUNCTION update_modified_column();

-- ==========================================
-- DATA QUALITY VIEWS
-- ==========================================

-- Create view for data quality monitoring
CREATE OR REPLACE VIEW georetail.data_quality_dashboard AS
SELECT 
    'ukraine_grid' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT resolution) as resolutions_count,
    MIN(created_at) as oldest_record,
    MAX(created_at) as newest_record
FROM h3_data.ukraine_grid

UNION ALL

SELECT 
    'stores' as table_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT format) as formats_count,
    MIN(opening_date::timestamp) as oldest_record,
    MAX(opening_date::timestamp) as newest_record
FROM georetail.stores

UNION ALL

SELECT 
    'demographics' as table_name,
    COUNT(*) as record_count,
    SUM(population_count) as total_population,
    MIN(created_at) as oldest_record,
    MAX(created_at) as newest_record
FROM demographics.h3_population;

-- Grant permissions
GRANT SELECT ON georetail.data_quality_dashboard TO georetail_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA georetail TO georetail_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA h3_data TO georetail_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA demographics TO georetail_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA osm_cache TO georetail_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA traffic TO georetail_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA georetail TO georetail_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA h3_data TO georetail_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA demographics TO georetail_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA osm_cache TO georetail_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA traffic TO georetail_user;

-- Success message
DO $$
BEGIN
    RAISE NOTICE 'GeoRetail PostGIS + H3 database initialized successfully!';
    RAISE NOTICE 'Schemas created: georetail, h3_data, osm_cache, demographics, traffic';
    RAISE NOTICE 'H3 functions available for spatial indexing';
    RAISE NOTICE 'Next steps: 1) Import demographics data 2) Import store data 3) Generate H3 grid';
END $$;