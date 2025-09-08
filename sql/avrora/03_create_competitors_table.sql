-- =====================================================
-- ЕТАП 3: Створення таблиці конкурентів
-- =====================================================

-- Видалення таблиці якщо існує (для розробки)
-- DROP TABLE IF EXISTS avrora.competitors CASCADE;

-- Таблиця конкурентів
CREATE TABLE avrora.competitors (
    -- Первинний ключ
    competitor_id SERIAL PRIMARY KEY,
    
    -- Ідентифікація
    brand VARCHAR(100),
    name VARCHAR(255) NOT NULL,
    
    -- Геодані
    lat DECIMAL(10,8) NOT NULL,
    lon DECIMAL(11,8) NOT NULL,
    geometry geometry(Point, 4326),
    
    -- Адреса
    address TEXT,
    
    -- Адміністративні прив'язки (заповнюються тригером)
    oblast_id INT,
    oblast_name VARCHAR(255),
    raion_id INT,
    raion_name VARCHAR(255),
    gromada_id INT,
    gromada_name VARCHAR(255),
    settlement_id INT,
    settlement_name VARCHAR(255),
    
    -- H3 індекси (заповнюються тригером)
    h3_7 VARCHAR(15),
    h3_8 VARCHAR(15),
    h3_9 VARCHAR(15),
    h3_10 VARCHAR(15),
    
    -- Службові поля
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Коментарі
COMMENT ON TABLE avrora.competitors IS 'Довідник магазинів-конкурентів';
COMMENT ON COLUMN avrora.competitors.brand IS 'Бренд конкурента (АТБ, Сільпо, Наша Ряба тощо)';

-- Створення індексів для таблиці competitors
CREATE INDEX idx_avrora_competitors_brand ON avrora.competitors(brand);
CREATE INDEX idx_avrora_competitors_brand_h3_8 ON avrora.competitors(brand, h3_8);
CREATE INDEX idx_avrora_competitors_h3_7 ON avrora.competitors(h3_7);
CREATE INDEX idx_avrora_competitors_h3_8 ON avrora.competitors(h3_8);
CREATE INDEX idx_avrora_competitors_oblast ON avrora.competitors(oblast_id);
CREATE INDEX idx_avrora_competitors_active ON avrora.competitors(is_active) WHERE is_active = true;

-- Геопросторовий індекс
CREATE INDEX idx_avrora_competitors_geometry ON avrora.competitors USING GIST(geometry);

-- Перевірка створення
SELECT COUNT(*) as indexes_count
FROM pg_indexes
WHERE schemaname = 'avrora' 
  AND tablename = 'competitors';