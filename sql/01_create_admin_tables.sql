-- =====================================================
-- Створення таблиць для адміністративного поділу
-- Файл: sql/01_create_admin_tables.sql
-- =====================================================

-- 1. Створення таблиці довідника адміністративних одиниць
DROP TABLE IF EXISTS osm_ukraine.admin_boundaries CASCADE;

CREATE TABLE osm_ukraine.admin_boundaries (
    id SERIAL PRIMARY KEY,
    osm_id BIGINT UNIQUE NOT NULL,
    admin_level INTEGER NOT NULL CHECK (admin_level BETWEEN 2 AND 11),
    boundary_type VARCHAR(50) DEFAULT 'administrative',
    name VARCHAR(255),
    name_uk VARCHAR(255),
    name_en VARCHAR(255),
    parent_id INTEGER REFERENCES osm_ukraine.admin_boundaries(id) ON DELETE SET NULL,
    region_name VARCHAR(50),
    geometry GEOMETRY(MultiPolygon, 4326) NOT NULL,
    area_km2 NUMERIC(10, 2),
    population INTEGER,
    additional_tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Створення індексів для admin_boundaries
CREATE INDEX idx_admin_boundaries_geometry ON osm_ukraine.admin_boundaries USING GIST (geometry);
CREATE INDEX idx_admin_boundaries_admin_level ON osm_ukraine.admin_boundaries (admin_level);
CREATE INDEX idx_admin_boundaries_parent_id ON osm_ukraine.admin_boundaries (parent_id);
CREATE INDEX idx_admin_boundaries_region_name ON osm_ukraine.admin_boundaries (region_name);
CREATE INDEX idx_admin_boundaries_name ON osm_ukraine.admin_boundaries (name);
CREATE INDEX idx_admin_boundaries_name_uk ON osm_ukraine.admin_boundaries (name_uk);

-- 3. Створення таблиці зв'язків H3 з адмінодиницями
DROP TABLE IF EXISTS osm_ukraine.h3_admin_mapping CASCADE;

CREATE TABLE osm_ukraine.h3_admin_mapping (
    h3_index VARCHAR(15) PRIMARY KEY,
    h3_resolution INTEGER NOT NULL CHECK (h3_resolution BETWEEN 7 AND 10),
    -- Область (admin_level = 4)
    oblast_id INTEGER REFERENCES osm_ukraine.admin_boundaries(id),
    oblast_name VARCHAR(255),
    -- Район (admin_level = 6)
    raion_id INTEGER REFERENCES osm_ukraine.admin_boundaries(id),
    raion_name VARCHAR(255),
    -- Населений пункт (admin_level = 7, 8, 9)
    settlement_id INTEGER REFERENCES osm_ukraine.admin_boundaries(id),
    settlement_name VARCHAR(255),
    settlement_admin_level INTEGER,
    -- Службові поля
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Створення індексів для h3_admin_mapping
CREATE INDEX idx_h3_admin_mapping_resolution ON osm_ukraine.h3_admin_mapping (h3_resolution);
CREATE INDEX idx_h3_admin_mapping_oblast_id ON osm_ukraine.h3_admin_mapping (oblast_id);
CREATE INDEX idx_h3_admin_mapping_raion_id ON osm_ukraine.h3_admin_mapping (raion_id);
CREATE INDEX idx_h3_admin_mapping_settlement_id ON osm_ukraine.h3_admin_mapping (settlement_id);
CREATE INDEX idx_h3_admin_mapping_oblast_name ON osm_ukraine.h3_admin_mapping (oblast_name);

-- 5. Створення допоміжної таблиці для логування процесу
DROP TABLE IF EXISTS osm_ukraine.admin_processing_log CASCADE;

CREATE TABLE osm_ukraine.admin_processing_log (
    id SERIAL PRIMARY KEY,
    process_name VARCHAR(100),
    status VARCHAR(20),
    message TEXT,
    details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Створення функції для оновлення updated_at
CREATE OR REPLACE FUNCTION osm_ukraine.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 7. Створення тригерів для автоматичного оновлення updated_at
CREATE TRIGGER update_admin_boundaries_updated_at 
    BEFORE UPDATE ON osm_ukraine.admin_boundaries 
    FOR EACH ROW 
    EXECUTE FUNCTION osm_ukraine.update_updated_at_column();

CREATE TRIGGER update_h3_admin_mapping_updated_at 
    BEFORE UPDATE ON osm_ukraine.h3_admin_mapping 
    FOR EACH ROW 
    EXECUTE FUNCTION osm_ukraine.update_updated_at_column();

-- 8. Коментарі до таблиць
COMMENT ON TABLE osm_ukraine.admin_boundaries IS 'Довідник адміністративних одиниць України з OSM';
COMMENT ON TABLE osm_ukraine.h3_admin_mapping IS 'Зв''язки між H3 гексагонами та адміністративними одиницями';
COMMENT ON TABLE osm_ukraine.admin_processing_log IS 'Лог процесу обробки адміністративних даних';

COMMENT ON COLUMN osm_ukraine.admin_boundaries.admin_level IS '4 - область, 6 - район, 7 - місто/смт, 8 - село, 9 - частина населеного пункту';
COMMENT ON COLUMN osm_ukraine.h3_admin_mapping.h3_resolution IS 'Роздільна здатність H3: 7, 8, 9 або 10';

-- Виведення результату
SELECT 'Таблиці успішно створені' as result;