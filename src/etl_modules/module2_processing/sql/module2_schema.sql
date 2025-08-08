-- =================================================================
-- GeoRetail Module 2: Database Schema
-- =================================================================
-- Цей скрипт створює всі необхідні таблиці для Модуля 2
-- Включає: poi_processed, h3_analytics_current, h3_analytics_changes
-- =================================================================

-- Переконуємося, що schema існує
CREATE SCHEMA IF NOT EXISTS osm_ukraine;

-- =================================================================
-- 1. ТАБЛИЦЯ ОБРОБЛЕНИХ СУТНОСТЕЙ (Universal Entity Table)
-- =================================================================
-- Зберігає всі типи сутностей: POI, transport nodes, road segments
-- з нормалізованими брендами та функціональною класифікацією

DROP TABLE IF EXISTS osm_ukraine.poi_processed CASCADE;

CREATE TABLE osm_ukraine.poi_processed (
    -- Primary key
    entity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- OSM reference
    osm_id BIGINT NOT NULL,
    osm_raw_id BIGINT, -- посилання на osm_ukraine.osm_raw.id
    
    -- Entity classification
    entity_type VARCHAR(20) NOT NULL CHECK (entity_type IN ('poi', 'transport_node', 'road_segment')),
    primary_category VARCHAR(50) NOT NULL,   -- 'retail', 'transport', 'infrastructure', 'road'
    secondary_category VARCHAR(50) NOT NULL, -- 'supermarket', 'bus_stop', 'primary_road'
    
    -- Naming and branding (для POI та transport)
    name_original VARCHAR(200),
    name_standardized VARCHAR(200),
    brand_normalized VARCHAR(100),
    brand_confidence DECIMAL(3,2) DEFAULT 0.0 CHECK (brand_confidence BETWEEN 0 AND 1),
    brand_match_type VARCHAR(20) DEFAULT 'none' CHECK (brand_match_type IN ('none', 'exact', 'fuzzy', 'osm_tag', 'keyword', 'generic')),
    
    -- Functional impact classification
    functional_group VARCHAR(50) CHECK (functional_group IN ('competitor', 'traffic_generator', 'accessibility', 'residential_indicator', 'neutral')),
    influence_weight DECIMAL(3,2) DEFAULT 0.0 CHECK (influence_weight BETWEEN -1.0 AND 1.0),
    
    -- Geometry (flexible for points/lines)
    geom GEOMETRY NOT NULL,
    geom_type VARCHAR(20) GENERATED ALWAYS AS (GeometryType(geom)) STORED,
    
    -- H3 spatial indexing (all resolutions)
    h3_res_7 VARCHAR(15),
    h3_res_8 VARCHAR(15),
    h3_res_9 VARCHAR(15), 
    h3_res_10 VARCHAR(15),
    h3_coverage JSONB, -- For road segments covering multiple hexes {"res_9": ["hex1", "hex2"], "res_10": [...]}
    
    -- Transport/Road specific attributes
    highway_type VARCHAR(30), -- 'primary', 'residential', 'service' (for roads)
    max_speed INTEGER,        -- Speed limit for roads
    lanes INTEGER,            -- Number of lanes for roads
    accessibility_score DECIMAL(3,2) CHECK (accessibility_score BETWEEN 0 AND 1),
    
    -- Quality assessment
    quality_score DECIMAL(3,2) DEFAULT 0.0 CHECK (quality_score BETWEEN 0 AND 1),
    completeness_score DECIMAL(3,2) DEFAULT 0.0 CHECK (completeness_score BETWEEN 0 AND 1),
    consistency_score DECIMAL(3,2) DEFAULT 0.0 CHECK (consistency_score BETWEEN 0 AND 1),
    
    -- Additional attributes (flexible JSONB)
    attributes JSONB DEFAULT '{}',
    
    -- Metadata
    region_name VARCHAR(100),
    processing_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processing_version VARCHAR(20) DEFAULT '2.0.0',
    data_source VARCHAR(50) DEFAULT 'OpenStreetMap',
    
    -- Update tracking
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    update_count INTEGER DEFAULT 0
);

-- Створюємо індекси для швидкого пошуку
CREATE INDEX idx_poi_processed_osm_id ON osm_ukraine.poi_processed(osm_id);
CREATE INDEX idx_poi_processed_entity_type ON osm_ukraine.poi_processed(entity_type);
CREATE INDEX idx_poi_processed_functional_group ON osm_ukraine.poi_processed(functional_group);
CREATE INDEX idx_poi_processed_brand ON osm_ukraine.poi_processed(brand_normalized) WHERE brand_normalized IS NOT NULL;
CREATE INDEX idx_poi_processed_h3_res_7 ON osm_ukraine.poi_processed(h3_res_7) WHERE h3_res_7 IS NOT NULL;
CREATE INDEX idx_poi_processed_h3_res_8 ON osm_ukraine.poi_processed(h3_res_8) WHERE h3_res_8 IS NOT NULL;
CREATE INDEX idx_poi_processed_h3_res_9 ON osm_ukraine.poi_processed(h3_res_9) WHERE h3_res_9 IS NOT NULL;
CREATE INDEX idx_poi_processed_h3_res_10 ON osm_ukraine.poi_processed(h3_res_10) WHERE h3_res_10 IS NOT NULL;

-- Spatial index
CREATE INDEX idx_poi_processed_geom ON osm_ukraine.poi_processed USING GIST(geom);

-- Composite index для частих запитів
CREATE INDEX idx_poi_processed_category_group ON osm_ukraine.poi_processed(primary_category, functional_group);

-- =================================================================
-- 2. ТАБЛИЦЯ ПОТОЧНИХ H3 АНАЛІТИЧНИХ МЕТРИК
-- =================================================================
-- Зберігає агреговані метрики для кожного H3 hexagon
-- Оновлюється інкрементально через change detection

DROP TABLE IF EXISTS osm_ukraine.h3_analytics_current CASCADE;

CREATE TABLE osm_ukraine.h3_analytics_current (
    -- Primary key
    h3_index VARCHAR(15) NOT NULL,
    resolution INTEGER NOT NULL CHECK (resolution BETWEEN 7 AND 10),
    
    -- Update tracking
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_change_id UUID, -- посилання на останню зміну
    
    -- POI Counts
    poi_total_count INTEGER DEFAULT 0,
    retail_count INTEGER DEFAULT 0,
    competitor_count INTEGER DEFAULT 0,
    traffic_generator_count INTEGER DEFAULT 0,
    infrastructure_count INTEGER DEFAULT 0,
    
    -- Detailed competitor breakdown
    competitor_breakdown JSONB DEFAULT '{}', -- {"atb": 2, "silpo": 1, "novus": 1}
    
    -- Density Metrics (per km²)
    poi_density DECIMAL(8,2) DEFAULT 0.0,
    retail_density DECIMAL(8,2) DEFAULT 0.0,
    competition_intensity DECIMAL(5,2) DEFAULT 0.0,
    
    -- Influence Metrics
    total_positive_influence DECIMAL(6,2) DEFAULT 0.0,
    total_negative_influence DECIMAL(6,2) DEFAULT 0.0,
    net_influence_score DECIMAL(6,2) DEFAULT 0.0,
    
    -- Transport Accessibility
    transport_accessibility_score DECIMAL(3,2) DEFAULT 0.0 CHECK (transport_accessibility_score BETWEEN 0 AND 1),
    nearest_metro_distance_km DECIMAL(5,2),
    nearest_bus_stop_distance_km DECIMAL(5,2),
    public_transport_density DECIMAL(6,2) DEFAULT 0.0,
    
    -- Road Network Metrics
    road_density_km_per_km2 DECIMAL(6,2) DEFAULT 0.0,
    primary_road_access BOOLEAN DEFAULT FALSE,
    average_road_quality DECIMAL(3,2) DEFAULT 0.0,
    
    -- Quality Metrics
    avg_poi_quality DECIMAL(3,2) DEFAULT 0.0,
    branded_poi_ratio DECIMAL(3,2) DEFAULT 0.0,
    data_completeness DECIMAL(3,2) DEFAULT 0.0,
    
    -- Demographic proxies (from POI analysis)
    residential_indicator_score DECIMAL(3,2) DEFAULT 0.0,
    commercial_activity_score DECIMAL(3,2) DEFAULT 0.0,
    
    -- Market metrics
    market_saturation_index DECIMAL(3,2) DEFAULT 0.0,
    opportunity_score DECIMAL(3,2) DEFAULT 0.0,
    
    -- Additional analytics
    analytics_metadata JSONB DEFAULT '{}',
    
    -- Geometry for visualization
    hex_geometry GEOMETRY(POLYGON, 4326),
    hex_center GEOMETRY(POINT, 4326),
    hex_area_km2 DECIMAL(6,3),
    
    -- Processing metadata
    calculation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processing_version VARCHAR(20) DEFAULT '2.0.0',
    
    -- Constraints
    PRIMARY KEY (h3_index, resolution)
);

-- Створюємо індекси для аналітики
CREATE INDEX idx_h3_analytics_resolution ON osm_ukraine.h3_analytics_current(resolution);
CREATE INDEX idx_h3_analytics_competitor_count ON osm_ukraine.h3_analytics_current(competitor_count) WHERE competitor_count > 0;
CREATE INDEX idx_h3_analytics_opportunity ON osm_ukraine.h3_analytics_current(opportunity_score) WHERE opportunity_score > 0.5;
CREATE INDEX idx_h3_analytics_updated ON osm_ukraine.h3_analytics_current(last_updated);

-- Spatial index для hex geometry
CREATE INDEX idx_h3_analytics_hex_geom ON osm_ukraine.h3_analytics_current USING GIST(hex_geometry);
CREATE INDEX idx_h3_analytics_hex_center ON osm_ukraine.h3_analytics_current USING GIST(hex_center);

-- =================================================================
-- 3. ТАБЛИЦЯ ІСТОРІЇ ЗМІН (Change Detection & Incremental Updates)
-- =================================================================
-- Зберігає всі зміни для incremental updates та time-series analysis

DROP TABLE IF EXISTS osm_ukraine.h3_analytics_changes CASCADE;

CREATE TABLE osm_ukraine.h3_analytics_changes (
    -- Primary key
    change_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Change metadata
    change_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    change_type VARCHAR(30) NOT NULL CHECK (change_type IN (
        'poi_added', 'poi_removed', 'poi_updated', 
        'competitor_opened', 'competitor_closed',
        'transport_added', 'transport_removed',
        'road_added', 'road_updated',
        'bulk_recalculation'
    )),
    
    -- Affected hexagons
    h3_index VARCHAR(15) NOT NULL,
    resolution INTEGER NOT NULL CHECK (resolution BETWEEN 7 AND 10),
    affected_neighbors VARCHAR(15)[] DEFAULT '{}', -- сусідні hexagons що потребують оновлення
    
    -- Entity reference
    entity_id UUID REFERENCES osm_ukraine.poi_processed(entity_id) ON DELETE SET NULL,
    entity_type VARCHAR(20),
    
    -- Change details
    change_description JSONB NOT NULL, -- {"action": "opened", "brand": "АТБ", "influence": -0.9}
    
    -- Metric deltas (incremental changes)
    poi_total_delta INTEGER DEFAULT 0,
    retail_count_delta INTEGER DEFAULT 0,
    competitor_count_delta INTEGER DEFAULT 0,
    traffic_generator_delta INTEGER DEFAULT 0,
    net_influence_delta DECIMAL(6,2) DEFAULT 0.0,
    
    -- Before/After values for key metrics
    metrics_before JSONB DEFAULT '{}',
    metrics_after JSONB DEFAULT '{}',
    
    -- Processing metadata
    requires_full_recalc BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE,
    processing_batch_id UUID,
    processing_duration_ms INTEGER,
    
    -- Error handling
    processing_status VARCHAR(20) DEFAULT 'pending' CHECK (processing_status IN ('pending', 'processing', 'completed', 'failed')),
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);

-- Створюємо індекси для change tracking
CREATE INDEX idx_h3_changes_timestamp ON osm_ukraine.h3_analytics_changes(change_timestamp);
CREATE INDEX idx_h3_changes_h3_index ON osm_ukraine.h3_analytics_changes(h3_index, resolution);
CREATE INDEX idx_h3_changes_entity ON osm_ukraine.h3_analytics_changes(entity_id) WHERE entity_id IS NOT NULL;
CREATE INDEX idx_h3_changes_type ON osm_ukraine.h3_analytics_changes(change_type);
CREATE INDEX idx_h3_changes_status ON osm_ukraine.h3_analytics_changes(processing_status) WHERE processing_status != 'completed';
CREATE INDEX idx_h3_changes_batch ON osm_ukraine.h3_analytics_changes(processing_batch_id) WHERE processing_batch_id IS NOT NULL;

-- =================================================================
-- 4. ДОПОМІЖНІ ТАБЛИЦІ
-- =================================================================

-- Таблиця для H3 grid reference (опціонально)
-- DROP TABLE IF EXISTS osm_ukraine.h3_grid CASCADE;

-- CREATE TABLE osm_ukraine.h3_grid (
--    h3_index VARCHAR(15) PRIMARY KEY,
--    resolution INTEGER NOT NULL CHECK (resolution BETWEEN 7 AND 10),
--    hex_geometry GEOMETRY(POLYGON, 4326) NOT NULL,
--    hex_center GEOMETRY(POINT, 4326) NOT NULL,
--    area_km2 DECIMAL(6,3),
--    parent_index VARCHAR(15),
--    children_indexes VARCHAR(15)[],
--    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
--);

--CREATE INDEX idx_h3_grid_resolution ON osm_ukraine.h3_grid(resolution);
--CREATE INDEX idx_h3_grid_geom ON osm_ukraine.h3_grid USING GIST(hex_geometry);

-- =================================================================
-- 5. VIEWS ДЛЯ АНАЛІТИКИ
-- =================================================================

-- View для швидкого доступу до конкурентів
CREATE OR REPLACE VIEW osm_ukraine.v_competitors AS
SELECT 
    entity_id,
    osm_id,
    brand_normalized,
    name_standardized,
    geom,
    h3_res_9,
    influence_weight,
    quality_score,
    region_name
FROM osm_ukraine.poi_processed
WHERE functional_group = 'competitor'
  AND brand_normalized IS NOT NULL;

-- View для H3 метрик з високими можливостями
CREATE OR REPLACE VIEW osm_ukraine.v_h3_opportunities AS
SELECT 
    h3_index,
    resolution,
    hex_geometry,
    opportunity_score,
    market_saturation_index,
    competitor_count,
    traffic_generator_count,
    transport_accessibility_score,
    net_influence_score
FROM osm_ukraine.h3_analytics_current
WHERE resolution = 9
  AND opportunity_score > 0.6
ORDER BY opportunity_score DESC;

-- =================================================================
-- 6. TRIGGER FUNCTIONS
-- =================================================================

-- Функція для автоматичного оновлення updated_at
CREATE OR REPLACE FUNCTION osm_ukraine.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.update_count = OLD.update_count + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger для poi_processed
CREATE TRIGGER update_poi_processed_updated_at 
    BEFORE UPDATE ON osm_ukraine.poi_processed
    FOR EACH ROW
    EXECUTE FUNCTION osm_ukraine.update_updated_at_column();

-- =================================================================
-- 7. ФУНКЦІЇ ДЛЯ H3 ОПЕРАЦІЙ
-- =================================================================

-- Функція для отримання сусідніх H3 cells
CREATE OR REPLACE FUNCTION osm_ukraine.get_h3_neighbors(
    h3_index VARCHAR(15),
    ring_size INTEGER DEFAULT 1
)
RETURNS VARCHAR(15)[]
AS $$
DECLARE
    neighbors VARCHAR(15)[];
BEGIN
    -- Використовуємо h3_k_ring для отримання сусідів
    SELECT array_agg(h3_k_ring_distances)
    INTO neighbors
    FROM h3_k_ring_distances(h3_index::h3index, ring_size)
    WHERE h3_k_ring_distances::text != h3_index;
    
    RETURN neighbors;
END;
$$ LANGUAGE plpgsql;

-- =================================================================
-- 8. PERMISSIONS
-- =================================================================

-- Надаємо права користувачу (замініть georetail_user на вашого користувача)
GRANT ALL PRIVILEGES ON SCHEMA osm_ukraine TO georetail_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA osm_ukraine TO georetail_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA osm_ukraine TO georetail_user;

-- =================================================================
-- Кінець скрипту
-- =================================================================