-- ==================================================================
-- OSM Full Schema для України - Production Ready
-- Всі 24 регіони, повні індекси, матеріалізовані представлення
-- ==================================================================

-- Створення схем
CREATE SCHEMA IF NOT EXISTS osm_ukraine;
CREATE SCHEMA IF NOT EXISTS osm_analytics;  
CREATE SCHEMA IF NOT EXISTS osm_cache;

-- ==================================================================
-- 1. ОСНОВНА ТАБЛИЦЯ OSM ДАНИХ (ПАРТИЦІОНОВАНА)
-- ==================================================================

CREATE TABLE osm_ukraine.osm_raw (
    id BIGSERIAL,
    region_name VARCHAR(50) NOT NULL,
    original_fid INTEGER,
    osm_id BIGINT,
    osm_type VARCHAR(20),
    version INTEGER,
    changeset INTEGER,
    uid INTEGER,
    username VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE,
    geom GEOMETRY(GEOMETRY, 4326),
    tags JSONB,
    
    -- H3 індекси для різних резолюцій
    h3_res_7 VARCHAR(15),
    h3_res_8 VARCHAR(15), 
    h3_res_9 VARCHAR(15),
    h3_res_10 VARCHAR(15),
    
    -- Витягнуті ключові теги для швидкого доступу
    poi_type VARCHAR(50),           -- amenity, shop, office, etc.
    poi_value VARCHAR(255),         -- restaurant, supermarket, etc.
    name VARCHAR(255),
    name_uk VARCHAR(255),
    name_en VARCHAR(255),
    brand VARCHAR(255),
    
    -- Адресна інформація
    addr_housenumber VARCHAR(20),
    addr_street VARCHAR(255),
    addr_city VARCHAR(255),
    addr_postcode VARCHAR(20),
    
    -- Комерційна інформація
    opening_hours TEXT,
    phone VARCHAR(50),
    website VARCHAR(500),
    cuisine VARCHAR(255),
    level VARCHAR(20),
    
    -- Метадані
    data_quality_score DECIMAL(3,2) DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (id, region_name)
) PARTITION BY LIST (region_name);

-- ==================================================================
-- 2. СТВОРЕННЯ ПАРТИЦІЙ ДЛЯ ВСІХ 24 РЕГІОНІВ
-- ==================================================================

-- Партиції відповідно до ваших .gpkg файлів
CREATE TABLE osm_ukraine.osm_raw_cherkasy PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Cherkasy');
CREATE TABLE osm_ukraine.osm_raw_chernihiv PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Chernihiv');
CREATE TABLE osm_ukraine.osm_raw_chernivci PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Chernivci');
CREATE TABLE osm_ukraine.osm_raw_dnipro PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Dnipro');
CREATE TABLE osm_ukraine.osm_raw_donetsk PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Donetsk');
CREATE TABLE osm_ukraine.osm_raw_if PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('IF');
CREATE TABLE osm_ukraine.osm_raw_kharkiv PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Kharkiv');
CREATE TABLE osm_ukraine.osm_raw_kherson PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Kherson');
CREATE TABLE osm_ukraine.osm_raw_khmelnytskiy PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Khmelnytskiy');
CREATE TABLE osm_ukraine.osm_raw_kirovograd PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Kirovograd');
CREATE TABLE osm_ukraine.osm_raw_kyiv PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Kyiv');
CREATE TABLE osm_ukraine.osm_raw_luhansk PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Luhansk');
CREATE TABLE osm_ukraine.osm_raw_lviv PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Lviv');
CREATE TABLE osm_ukraine.osm_raw_mykolaiv PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Mykolaiv');
CREATE TABLE osm_ukraine.osm_raw_odesa PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Odesa');
CREATE TABLE osm_ukraine.osm_raw_poltava PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Poltava');
CREATE TABLE osm_ukraine.osm_raw_rivne PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Rivne');
CREATE TABLE osm_ukraine.osm_raw_sumy PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Sumy');
CREATE TABLE osm_ukraine.osm_raw_ternopil PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Ternopil');
CREATE TABLE osm_ukraine.osm_raw_uzhgorod PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Uzhgorod');
CREATE TABLE osm_ukraine.osm_raw_vinnytsya PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Vinnytsya');
CREATE TABLE osm_ukraine.osm_raw_volyn PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Volyn');
CREATE TABLE osm_ukraine.osm_raw_zaporizh PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Zaporizh');
CREATE TABLE osm_ukraine.osm_raw_zhytomyr PARTITION OF osm_ukraine.osm_raw FOR VALUES IN ('Zhytomyr');

-- ==================================================================
-- 3. НОРМАЛІЗОВАНА ТАБЛИЦЯ POI (БЕЗ ПРОБЛЕМНИХ FK)
-- ==================================================================

CREATE TABLE osm_ukraine.poi_normalized (
    id BIGSERIAL PRIMARY KEY,
    osm_raw_id BIGINT,  -- Зв'язок без FK для швидкості
    region_name VARCHAR(50) NOT NULL,
    osm_id BIGINT,
    geom GEOMETRY(POINT, 4326),
    
    -- Категоризація POI
    poi_category VARCHAR(50),       -- retail, food, transport, etc.
    poi_subcategory VARCHAR(100),   -- shop:convenience, amenity:restaurant
    poi_type VARCHAR(50),           -- amenity, shop, office
    poi_value VARCHAR(255),         -- restaurant, convenience, bank
    
    -- Основна інформація
    name VARCHAR(255),
    brand VARCHAR(255),
    
    -- Адреса
    addr_housenumber VARCHAR(20),
    addr_street VARCHAR(255),
    addr_city VARCHAR(255),
    addr_postcode VARCHAR(20),
    
    -- Комерційні дані
    opening_hours TEXT,
    phone VARCHAR(50),
    website VARCHAR(500),
    cuisine VARCHAR(255),
    
    -- H3 індекси
    h3_res_8 VARCHAR(15),
    h3_res_9 VARCHAR(15),
    h3_res_10 VARCHAR(15),
    
    -- Метрики
    retail_relevance_score DECIMAL(3,2),
    
    created_at TIMESTAMP DEFAULT NOW()
);

-- ==================================================================
-- 4. АНАЛІТИЧНІ ТАБЛИЦІ
-- ==================================================================

-- H3 агрегації для швидкої аналітики
CREATE TABLE osm_analytics.h3_poi_summary (
    h3_index VARCHAR(15) PRIMARY KEY,
    resolution INTEGER NOT NULL,
    region_name VARCHAR(50),
    
    -- Підрахунки POI
    total_poi INTEGER DEFAULT 0,
    retail_poi INTEGER DEFAULT 0,
    food_poi INTEGER DEFAULT 0,
    service_poi INTEGER DEFAULT 0,
    transport_poi INTEGER DEFAULT 0,
    
    -- Щільності
    poi_density_per_km2 DECIMAL(10,2),
    retail_density_per_km2 DECIMAL(10,2),
    
    -- Різноманітність
    poi_diversity_score DECIMAL(3,2),
    brand_diversity INTEGER,
    
    -- Геометрія H3 комірки
    h3_boundary GEOMETRY(POLYGON, 4326),
    center_point GEOMETRY(POINT, 4326),
    
    last_updated TIMESTAMP DEFAULT NOW()
);

-- Таблиця для зберігання базової H3 сітки України
CREATE TABLE osm_analytics.ukraine_h3_grid (
    h3_index VARCHAR(15) PRIMARY KEY,
    resolution INTEGER NOT NULL,
    region_name VARCHAR(50),
    h3_boundary GEOMETRY(POLYGON, 4326),
    center_point GEOMETRY(POINT, 4326),
    area_km2 DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT NOW()
);

-- ==================================================================
-- 5. ETL КЕШІ ТА МЕТАДАНІ
-- ==================================================================

-- Логування ETL процесів
CREATE TABLE osm_cache.etl_runs (
    id SERIAL PRIMARY KEY,
    region_name VARCHAR(50),
    file_path VARCHAR(500),
    file_size_mb DECIMAL(10,2),
    records_processed INTEGER,
    records_imported INTEGER,
    h3_indexed_count INTEGER,
    processing_time_seconds INTEGER,
    status VARCHAR(20), -- 'running', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP
);

-- Кеш для часто використовуваних просторових запитів
CREATE TABLE osm_cache.spatial_queries (
    id SERIAL PRIMARY KEY,
    query_hash VARCHAR(64) UNIQUE,
    query_type VARCHAR(50),
    params JSONB,
    result_data JSONB,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ==================================================================
-- 6. ОСНОВНІ ІНДЕКСИ ДЛЯ ПРОДУКТИВНОСТІ
-- ==================================================================

-- Просторові індекси
CREATE INDEX idx_osm_raw_geom ON osm_ukraine.osm_raw USING GIST (geom);
CREATE INDEX idx_poi_normalized_geom ON osm_ukraine.poi_normalized USING GIST (geom);
CREATE INDEX idx_h3_summary_boundary ON osm_analytics.h3_poi_summary USING GIST (h3_boundary);
CREATE INDEX idx_ukraine_grid_boundary ON osm_analytics.ukraine_h3_grid USING GIST (h3_boundary);

-- H3 індекси
CREATE INDEX idx_osm_raw_h3_8 ON osm_ukraine.osm_raw (h3_res_8);
CREATE INDEX idx_osm_raw_h3_9 ON osm_ukraine.osm_raw (h3_res_9);
CREATE INDEX idx_osm_raw_h3_10 ON osm_ukraine.osm_raw (h3_res_10);
CREATE INDEX idx_poi_h3_8 ON osm_ukraine.poi_normalized (h3_res_8);
CREATE INDEX idx_poi_h3_9 ON osm_ukraine.poi_normalized (h3_res_9);
CREATE INDEX idx_poi_h3_10 ON osm_ukraine.poi_normalized (h3_res_10);

-- JSONB індекси для тегів
CREATE INDEX idx_osm_raw_tags_gin ON osm_ukraine.osm_raw USING GIN (tags);
CREATE INDEX idx_osm_raw_tags_amenity ON osm_ukraine.osm_raw USING GIN ((tags->>'amenity'));
CREATE INDEX idx_osm_raw_tags_shop ON osm_ukraine.osm_raw USING GIN ((tags->>'shop'));
CREATE INDEX idx_osm_raw_tags_building ON osm_ukraine.osm_raw USING GIN ((tags->>'building'));
CREATE INDEX idx_osm_raw_tags_landuse ON osm_ukraine.osm_raw USING GIN ((tags->>'landuse'));
CREATE INDEX idx_osm_raw_tags_highway ON osm_ukraine.osm_raw USING GIN ((tags->>'highway'));
CREATE INDEX idx_osm_raw_tags_name ON osm_ukraine.osm_raw USING GIN ((tags->>'name'));

-- POI категорії індекси
CREATE INDEX idx_poi_category ON osm_ukraine.poi_normalized (poi_category);
CREATE INDEX idx_poi_subcategory ON osm_ukraine.poi_normalized (poi_subcategory);
CREATE INDEX idx_poi_brand ON osm_ukraine.poi_normalized (brand);

-- Композитні індекси
CREATE INDEX idx_osm_raw_region_h3_8 ON osm_ukraine.osm_raw (region_name, h3_res_8);
CREATE INDEX idx_osm_raw_poi_h3_9 ON osm_ukraine.osm_raw (poi_type, h3_res_9);
CREATE INDEX idx_poi_category_h3_9 ON osm_ukraine.poi_normalized (poi_category, h3_res_9);

-- Текстовий пошук
CREATE INDEX idx_osm_raw_name_trgm ON osm_ukraine.osm_raw USING GIN (name gin_trgm_ops);
CREATE INDEX idx_poi_name_trgm ON osm_ukraine.poi_normalized USING GIN (name gin_trgm_ops);

-- ETL індекси
CREATE INDEX idx_etl_runs_region_status ON osm_cache.etl_runs (region_name, status);
CREATE INDEX idx_etl_runs_completed_at ON osm_cache.etl_runs (completed_at DESC);

-- ==================================================================
-- 7. МАТЕРІАЛІЗОВАНІ ПРЕДСТАВЛЕННЯ ДЛЯ АНАЛІТИКИ
-- ==================================================================

-- Щоденні агрегації по H3 комірках резолюції 8
CREATE MATERIALIZED VIEW osm_analytics.daily_h3_res8_summary AS
SELECT 
    h3_res_8,
    region_name,
    COUNT(*) as total_features,
    COUNT(*) FILTER (WHERE poi_type = 'amenity') as amenity_count,
    COUNT(*) FILTER (WHERE poi_type = 'shop') as shop_count,
    COUNT(*) FILTER (WHERE poi_type = 'office') as office_count,
    COUNT(*) FILTER (WHERE tags->>'building' IS NOT NULL) as building_count,
    COUNT(*) FILTER (WHERE tags->>'highway' IS NOT NULL) as highway_count,
    COUNT(DISTINCT brand) FILTER (WHERE brand IS NOT NULL) as unique_brands,
    ST_Centroid(ST_Collect(geom)) as center_point,
    AVG(data_quality_score) as avg_quality_score
FROM osm_ukraine.osm_raw
WHERE h3_res_8 IS NOT NULL
GROUP BY h3_res_8, region_name;

CREATE UNIQUE INDEX idx_daily_h3_res8_summary_h3 ON osm_analytics.daily_h3_res8_summary (h3_res_8);

-- Ретейл щільність по H3 комірках резолюції 9
CREATE MATERIALIZED VIEW osm_analytics.retail_density_h3_res9 AS
SELECT 
    h3_res_9,
    region_name,
    COUNT(*) FILTER (WHERE poi_type = 'shop' AND poi_value IN ('supermarket', 'convenience', 'mall', 'department_store')) as retail_count,
    COUNT(*) FILTER (WHERE poi_type = 'amenity' AND poi_value IN ('restaurant', 'cafe', 'fast_food', 'bar')) as food_count,
    COUNT(*) FILTER (WHERE poi_type IN ('shop', 'amenity', 'office')) as commercial_count,
    COUNT(*) FILTER (WHERE brand IS NOT NULL) as branded_count,
    COUNT(DISTINCT brand) FILTER (WHERE brand IS NOT NULL) as brand_diversity,
    ST_Centroid(ST_Collect(geom)) as center_point
FROM osm_ukraine.osm_raw
WHERE h3_res_9 IS NOT NULL 
  AND poi_type IS NOT NULL
GROUP BY h3_res_9, region_name;

CREATE UNIQUE INDEX idx_retail_density_h3_res9_h3 ON osm_analytics.retail_density_h3_res9 (h3_res_9);

-- ==================================================================
-- 8. ДОПОМІЖНІ ФУНКЦІЇ
-- ==================================================================

-- Функція для розрахунку H3 індексів
CREATE OR REPLACE FUNCTION osm_ukraine.calculate_h3_indexes(
    lat DOUBLE PRECISION, 
    lon DOUBLE PRECISION
) RETURNS TABLE(
    h3_res_7 VARCHAR(15),
    h3_res_8 VARCHAR(15), 
    h3_res_9 VARCHAR(15),
    h3_res_10 VARCHAR(15)
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY SELECT
        h3_lat_lng_to_cell(POINT(lon, lat), 7)::VARCHAR(15),
        h3_lat_lng_to_cell(POINT(lon, lat), 8)::VARCHAR(15),
        h3_lat_lng_to_cell(POINT(lon, lat), 9)::VARCHAR(15),
        h3_lat_lng_to_cell(POINT(lon, lat), 10)::VARCHAR(15);
END;
$$;

-- Функція для оновлення матеріалізованих представлень
CREATE OR REPLACE FUNCTION osm_analytics.refresh_all_materialized_views()
RETURNS TEXT LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY osm_analytics.daily_h3_res8_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY osm_analytics.retail_density_h3_res9;
    
    RETURN 'All materialized views refreshed successfully';
END;
$$;

-- ==================================================================
-- 9. НАЛАШТУВАННЯ ПРОДУКТИВНОСТІ
-- ==================================================================

-- Налаштування для партиціонованих таблиць
SET enable_partition_pruning = on;
SET constraint_exclusion = partition;

-- Налаштування для JSONB
SET gin_pending_list_limit = '4MB';

-- Налаштування статистики
ALTER TABLE osm_ukraine.osm_raw ALTER COLUMN h3_res_8 SET STATISTICS 1000;
ALTER TABLE osm_ukraine.osm_raw ALTER COLUMN h3_res_9 SET STATISTICS 1000;
ALTER TABLE osm_ukraine.osm_raw ALTER COLUMN poi_type SET STATISTICS 1000;
ALTER TABLE osm_ukraine.osm_raw ALTER COLUMN region_name SET STATISTICS 1000;

-- ==================================================================
-- 10. КОМЕНТАРІ ДЛЯ ДОКУМЕНТАЦІЇ
-- ==================================================================

COMMENT ON SCHEMA osm_ukraine IS 'Основні OSM дані України з H3 індексацією';
COMMENT ON SCHEMA osm_analytics IS 'Аналітичні таблиці та агрегації';
COMMENT ON SCHEMA osm_cache IS 'Кеш та метадані ETL процесів';

COMMENT ON TABLE osm_ukraine.osm_raw IS 'Партиціонована таблиця всіх OSM даних України (24 регіони)';
COMMENT ON TABLE osm_ukraine.poi_normalized IS 'Нормалізовані POI для швидкого доступу';
COMMENT ON TABLE osm_analytics.h3_poi_summary IS 'Агрегації POI по H3 комірках';
COMMENT ON TABLE osm_cache.etl_runs IS 'Логування ETL процесів імпорту .gpkg файлів';

-- ==================================================================
-- 11. ЗАВЕРШЕННЯ СТВОРЕННЯ СХЕМИ
-- ==================================================================

-- Оновлення статистики
ANALYZE;

-- Повідомлення про успішне завершення
DO $$
BEGIN
    RAISE NOTICE '🎉 OSM Full Schema створено успішно!';
    RAISE NOTICE '✅ Схеми: osm_ukraine, osm_analytics, osm_cache';
    RAISE NOTICE '✅ Партиції: 24 регіони України';
    RAISE NOTICE '✅ Індекси: Просторові, H3, JSONB, композитні';
    RAISE NOTICE '✅ Функції: H3 розрахунки, оновлення представлень';
    RAISE NOTICE '🚀 Готово до імпорту .gpkg файлів!';
END $$;
