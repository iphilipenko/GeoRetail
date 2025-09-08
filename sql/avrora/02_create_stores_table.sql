-- =====================================================
-- ЕТАП 2: Створення таблиці магазинів
-- =====================================================

-- Видалення таблиці якщо існує (для розробки, закоментуйте в продакшн)
-- DROP TABLE IF EXISTS avrora.stores CASCADE;

-- Таблиця магазинів мережі Avrora
CREATE TABLE avrora.stores (
    -- Первинний ключ
    store_id SERIAL PRIMARY KEY,
    
    -- Бізнес ідентифікатори
    shop_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    
    -- Адресні дані
    oblast VARCHAR(255),
    city VARCHAR(255),
    street VARCHAR(255),
    address TEXT GENERATED ALWAYS AS (
        COALESCE(street, '') || 
        CASE WHEN city IS NOT NULL THEN ', ' || city ELSE '' END ||
        CASE WHEN oblast IS NOT NULL THEN ', ' || oblast ELSE '' END
    ) STORED,
    
    -- Дати
    opening_date DATE,
    closing_date DATE,
    
    -- Характеристики магазину
    format VARCHAR(50),
    square_trade DECIMAL(10,2),
    shop_square DECIMAL(10,2),
    
    -- Геодані
    lat DECIMAL(10,8) NOT NULL,
    lon DECIMAL(11,8) NOT NULL,
    geometry geometry(Point, 4326),
    
    -- Метрики
    population_x10k DECIMAL(10,4),
    avg_month_n_checks DECIMAL(12,2),
    
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
    
    -- H3 метрики
    population_h3_8 INT,
    neighbor_population_h3_8 INT,
    
    -- Службові поля
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Коментарі до таблиці та полів
COMMENT ON TABLE avrora.stores IS 'Довідник магазинів мережі Avrora';
COMMENT ON COLUMN avrora.stores.shop_id IS 'Унікальний бізнес-ідентифікатор магазину';
COMMENT ON COLUMN avrora.stores.format IS 'Формат магазину (СТАНДАРТ, МІНІ тощо)';
COMMENT ON COLUMN avrora.stores.population_x10k IS 'Населення в радіусі обслуговування (десятки тисяч)';
COMMENT ON COLUMN avrora.stores.h3_8 IS 'H3 індекс резолюції 8 (~0.74 км²)';

-- Створення індексів для таблиці stores
CREATE INDEX idx_avrora_stores_shop_id ON avrora.stores(shop_id);
CREATE INDEX idx_avrora_stores_h3_7 ON avrora.stores(h3_7);
CREATE INDEX idx_avrora_stores_h3_8 ON avrora.stores(h3_8);
CREATE INDEX idx_avrora_stores_oblast_raion ON avrora.stores(oblast_id, raion_id);
CREATE INDEX idx_avrora_stores_active ON avrora.stores(is_active) WHERE is_active = true;
CREATE INDEX idx_avrora_stores_opening_date ON avrora.stores(opening_date);
CREATE INDEX idx_avrora_stores_format ON avrora.stores(format);

-- Геопросторовий індекс (виправлений синтаксис)
CREATE INDEX idx_avrora_stores_geometry ON avrora.stores USING GIST(geometry);

-- Перевірка створення таблиці
SELECT 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'avrora' 
  AND table_name = 'stores'
ORDER BY ordinal_position;