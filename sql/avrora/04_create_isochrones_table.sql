-- =====================================================
-- ЕТАП 4: Створення таблиць ізохрон та перетинів
-- =====================================================

-- Видалення таблиць якщо існують (для розробки)
-- DROP TABLE IF EXISTS avrora.isochrone_overlaps CASCADE;
-- DROP TABLE IF EXISTS avrora.isochrones CASCADE;

-- Таблиця ізохрон (зони досяжності)
CREATE TABLE avrora.isochrones (
    -- Первинний ключ
    isochrone_id SERIAL PRIMARY KEY,
    
    -- Ідентифікація об'єкта
    entity_type VARCHAR(20) NOT NULL CHECK (entity_type IN ('store', 'competitor')),
    entity_id INT NOT NULL,
    
    -- Параметри ізохрони
    mode VARCHAR(20) NOT NULL CHECK (mode IN ('walk', 'drive', 'bike', 'transit')),
    distance_meters INT NOT NULL,
    time_minutes INT,
    
    -- Геометрія
    polygon geometry(Polygon, 4326) NOT NULL,
    area_sqm DECIMAL(12,2),
    
    -- Оптимізація для швидкої фільтрації по viewport
    center_lat DECIMAL(10,8),
    center_lon DECIMAL(11,8),
    bbox_min_lat DECIMAL(10,8),
    bbox_min_lon DECIMAL(11,8),
    bbox_max_lat DECIMAL(10,8),
    bbox_max_lon DECIMAL(11,8),
    
    -- H3 покриття (для швидкого пошуку перетинів)
    h3_8_coverage TEXT[],
    
    -- Службові поля
    calculation_date DATE DEFAULT CURRENT_DATE,
    is_current BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Унікальність для активних ізохрон
    CONSTRAINT unique_current_isochrone UNIQUE(entity_type, entity_id, mode, distance_meters, is_current)
);

-- Коментарі
COMMENT ON TABLE avrora.isochrones IS 'Ізохрони (зони досяжності) для магазинів та конкурентів';
COMMENT ON COLUMN avrora.isochrones.mode IS 'Тип транспорту: walk, drive, bike, transit';
COMMENT ON COLUMN avrora.isochrones.distance_meters IS 'Відстань в метрах (400 для пішоходів, 1500 для авто тощо)';
COMMENT ON COLUMN avrora.isochrones.h3_8_coverage IS 'Масив H3 індексів резолюції 8, які покриває ізохрона';

-- Індекси для isochrones
CREATE INDEX idx_avrora_isochrones_entity ON avrora.isochrones(entity_type, entity_id);
CREATE INDEX idx_avrora_isochrones_mode_distance ON avrora.isochrones(mode, distance_meters);
CREATE INDEX idx_avrora_isochrones_current ON avrora.isochrones(is_current) WHERE is_current = true;

-- Індекс для швидкої фільтрації по viewport (bounding box)
CREATE INDEX idx_avrora_isochrones_bbox ON avrora.isochrones(
    bbox_min_lat, bbox_max_lat, bbox_min_lon, bbox_max_lon
);

-- GIN індекс для масиву H3 покриття
CREATE INDEX idx_avrora_isochrones_h3_coverage ON avrora.isochrones USING GIN(h3_8_coverage);

-- Геопросторовий індекс
CREATE INDEX idx_avrora_isochrones_polygon ON avrora.isochrones USING GIST(polygon);

-- =====================================================
-- Таблиця перетинів ізохрон
-- =====================================================

CREATE TABLE avrora.isochrone_overlaps (
    -- Первинний ключ
    overlap_id SERIAL PRIMARY KEY,
    
    -- Посилання на ізохрони
    isochrone_id_1 INT REFERENCES avrora.isochrones(isochrone_id) ON DELETE CASCADE,
    isochrone_id_2 INT REFERENCES avrora.isochrones(isochrone_id) ON DELETE CASCADE,
    
    -- Тип перетину
    overlap_type VARCHAR(20) CHECK (overlap_type IN ('cannibalization', 'competition')),
    
    -- Геометрія перетину
    overlap_polygon geometry(Geometry, 4326),
    overlap_area_sqm DECIMAL(12,2),
    
    -- Відсотки перекриття
    overlap_percent_1 DECIMAL(5,2), -- % від площі першої ізохрони
    overlap_percent_2 DECIMAL(5,2), -- % від площі другої ізохрони
    
    -- Додаткові метрики (заповнюються окремо)
    buildings_count INT,
    population_estimate INT,
    
    -- Службові поля
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Унікальність пари
    CONSTRAINT unique_overlap_pair UNIQUE(isochrone_id_1, isochrone_id_2),
    
    -- Перевірка що id_1 < id_2 для уникнення дублікатів
    CONSTRAINT ordered_ids CHECK (isochrone_id_1 < isochrone_id_2)
);

-- Коментарі
COMMENT ON TABLE avrora.isochrone_overlaps IS 'Перетини між ізохронами для аналізу канібалізації та конкуренції';
COMMENT ON COLUMN avrora.isochrone_overlaps.overlap_type IS 'cannibalization - між своїми магазинами, competition - з конкурентами';

-- Індекси для overlaps
CREATE INDEX idx_avrora_overlaps_type ON avrora.isochrone_overlaps(overlap_type);
CREATE INDEX idx_avrora_overlaps_isochrone_1 ON avrora.isochrone_overlaps(isochrone_id_1);
CREATE INDEX idx_avrora_overlaps_isochrone_2 ON avrora.isochrone_overlaps(isochrone_id_2);
CREATE INDEX idx_avrora_overlaps_area ON avrora.isochrone_overlaps(overlap_area_sqm DESC);

-- Геопросторовий індекс для полігонів перетинів
CREATE INDEX idx_avrora_overlaps_polygon ON avrora.isochrone_overlaps USING GIST(overlap_polygon);

-- Перевірка створення
SELECT 
    t.table_name,
    COUNT(c.column_name) as columns_count,
    COUNT(i.indexname) as indexes_count
FROM information_schema.tables t
LEFT JOIN information_schema.columns c 
    ON t.table_schema = c.table_schema AND t.table_name = c.table_name
LEFT JOIN pg_indexes i 
    ON t.table_schema = i.schemaname AND t.table_name = i.tablename
WHERE t.table_schema = 'avrora' 
  AND t.table_name IN ('isochrones', 'isochrone_overlaps')
GROUP BY t.table_name;