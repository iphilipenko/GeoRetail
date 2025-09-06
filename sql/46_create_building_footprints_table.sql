-- ================================================================
-- Файл: 46_create_building_footprints_table.sql
-- Мета: Створення таблиці для зберігання даних про забудову з OSM
-- Дата: 2025-01-05
-- 
-- Таблиця містить:
-- - Всі будівлі України з OSM (~12.6 млн записів)
-- - Класифікацію забудови (житлова, комерційна, промислова)
-- - Геометрію та площі будівель
-- - H3 індекси для просторової агрегації
-- - Розрахункові оцінки населення та кількості домогосподарств
-- 
-- Орієнтовний час виконання: < 1 хвилина (тільки створення структури)
-- ================================================================

-- Видаляємо таблицю якщо існує (для тестування)
DROP TABLE IF EXISTS osm_ukraine.building_footprints;

-- Створення таблиці для зберігання даних про забудову
CREATE TABLE osm_ukraine.building_footprints (
    -- [далі йде весь код створення таблиці]

-- Видаляємо таблицю якщо існує (для тестування)
DROP TABLE IF EXISTS osm_ukraine.building_footprints;

-- Створення таблиці для зберігання даних про забудову
CREATE TABLE osm_ukraine.building_footprints (
    -- Ідентифікація
    building_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    osm_id BIGINT NOT NULL,
    osm_raw_id BIGINT NOT NULL,
    
    -- Основна класифікація
    building_category VARCHAR(30) NOT NULL, -- 'residential', 'commercial', 'industrial', 'public', 'agricultural', 'other'
    building_type VARCHAR(50), -- значення з building tag (apartments, house, office, etc.)
    building_use VARCHAR(100), -- додаткова класифікація з shop/amenity/office тегів
    
    -- Геометрія
    footprint GEOMETRY(POLYGON, 4326) NOT NULL,
    centroid GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (ST_Centroid(footprint)) STORED,
    area_sqm DECIMAL(12,2) GENERATED ALWAYS AS (ST_Area(footprint::geography)) STORED,
    perimeter_m DECIMAL(10,2),
    
    -- H3 індекси (копіюємо готові з osm_raw)
    h3_res_7 VARCHAR(15),
    h3_res_8 VARCHAR(15),
    h3_res_9 VARCHAR(15),
    h3_res_10 VARCHAR(15),
    
    -- Характеристики будівлі
    building_levels INTEGER, -- кількість поверхів
    building_height_m DECIMAL(6,2), -- висота в метрах
    building_material VARCHAR(50), -- матеріал стін
    roof_shape VARCHAR(30), -- форма даху
    construction_year INTEGER, -- рік побудови якщо відомо
    
    -- Адреса (частково з osm_raw, частково з тегів)
    addr_street VARCHAR(255),
    addr_housenumber VARCHAR(20),
    addr_city VARCHAR(255),
    addr_postcode VARCHAR(20),
    
    -- Додаткова інформація
    name VARCHAR(255), -- назва будівлі якщо є
    
    -- Розрахункові площі та оцінки
    total_floor_area_sqm DECIMAL(12,2), -- загальна площа всіх поверхів (area_sqm * levels)
    
    -- Оцінки для аналітики
    residential_units_estimate INTEGER DEFAULT 0, -- кількість квартир/домогосподарств
    commercial_units_estimate INTEGER DEFAULT 0,  -- кількість комерційних приміщень  
    population_estimate INTEGER DEFAULT 0,        -- оцінка населення
    
    -- Якість даних
    data_completeness DECIMAL(3,2) DEFAULT 0, -- 0-1, повнота даних
    confidence_score DECIMAL(3,2) DEFAULT 0,  -- 0-1, впевненість в класифікації
    
    -- Метадані
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Унікальність по osm_id
    CONSTRAINT uk_building_osm_id UNIQUE(osm_id)
);

-- Основні індекси для швидкого пошуку
CREATE INDEX idx_building_footprint ON osm_ukraine.building_footprints USING GIST(footprint);
CREATE INDEX idx_building_centroid ON osm_ukraine.building_footprints USING GIST(centroid);
CREATE INDEX idx_building_category ON osm_ukraine.building_footprints(building_category);
CREATE INDEX idx_building_type ON osm_ukraine.building_footprints(building_type);
CREATE INDEX idx_building_use ON osm_ukraine.building_footprints(building_use);
CREATE INDEX idx_building_h3_10 ON osm_ukraine.building_footprints(h3_res_10);
CREATE INDEX idx_building_h3_9 ON osm_ukraine.building_footprints(h3_res_9);
CREATE INDEX idx_building_h3_8 ON osm_ukraine.building_footprints(h3_res_8);
CREATE INDEX idx_building_area ON osm_ukraine.building_footprints(area_sqm) WHERE area_sqm > 50;
CREATE INDEX idx_building_residential ON osm_ukraine.building_footprints(residential_units_estimate) 
    WHERE residential_units_estimate > 0;

-- Коментарі до таблиці та ключових полів
COMMENT ON TABLE osm_ukraine.building_footprints IS 'Забудова України з OSM з розрахунковими метриками для геоаналітики';
COMMENT ON COLUMN osm_ukraine.building_footprints.residential_units_estimate IS 'Оцінка кількості квартир: площа*поверхи/65м² для багатоквартирних, 1 для приватних будинків';
COMMENT ON COLUMN osm_ukraine.building_footprints.commercial_units_estimate IS 'Оцінка кількості комерційних приміщень: площа*поверхи/100м²';
COMMENT ON COLUMN osm_ukraine.building_footprints.population_estimate IS 'Оцінка населення: residential_units * 2.5';
COMMENT ON COLUMN osm_ukraine.building_footprints.total_floor_area_sqm IS 'Загальна площа всіх поверхів: area_sqm * building_levels';    