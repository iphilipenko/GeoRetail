-- ================================================================
-- Файл: 41_alter_h3_admin_mapping_table.sql
-- Мета: Оновлення структури таблиці h3_admin_mapping для нової ієрархії
-- Дата: 2025-01-04
-- ================================================================

-- 1. Перевіряємо поточну структуру таблиці
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns
WHERE table_schema = 'osm_ukraine'
AND table_name = 'h3_admin_mapping'
ORDER BY ordinal_position;

-- 2. Додаємо колонки для громад (якщо їх ще немає)
DO $$
BEGIN
    -- Перевіряємо чи існує колонка gromada_id
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_schema = 'osm_ukraine' 
        AND table_name = 'h3_admin_mapping' 
        AND column_name = 'gromada_id'
    ) THEN
        ALTER TABLE osm_ukraine.h3_admin_mapping 
        ADD COLUMN gromada_id INTEGER;
        
        ALTER TABLE osm_ukraine.h3_admin_mapping 
        ADD COLUMN gromada_name VARCHAR(255);
        
        RAISE NOTICE 'Додано колонки gromada_id та gromada_name';
    ELSE
        RAISE NOTICE 'Колонки для громад вже існують';
    END IF;
END $$;

-- 3. Створюємо індекси для нових полів
CREATE INDEX IF NOT EXISTS idx_h3_admin_gromada_id 
ON osm_ukraine.h3_admin_mapping(gromada_id);

CREATE INDEX IF NOT EXISTS idx_h3_admin_gromada_name 
ON osm_ukraine.h3_admin_mapping(gromada_name);

-- 4. Додаємо зовнішні ключі для цілісності даних (опціонально)
ALTER TABLE osm_ukraine.h3_admin_mapping 
DROP CONSTRAINT IF EXISTS fk_h3_gromada;

ALTER TABLE osm_ukraine.h3_admin_mapping 
ADD CONSTRAINT fk_h3_gromada 
FOREIGN KEY (gromada_id) 
REFERENCES osm_ukraine.admin_boundaries(id) 
ON DELETE SET NULL;

-- 5. Перевіряємо оновлену структуру
SELECT 
    column_name,
    data_type,
    CASE 
        WHEN column_name = 'h3_index' THEN 'H3 індекс'
        WHEN column_name = 'h3_resolution' THEN 'Резолюція H3'
        WHEN column_name = 'oblast_id' THEN 'ID області'
        WHEN column_name = 'oblast_name' THEN 'Назва області'
        WHEN column_name = 'raion_id' THEN 'ID району'
        WHEN column_name = 'raion_name' THEN 'Назва району'
        WHEN column_name = 'gromada_id' THEN 'ID громади (НОВЕ)'
        WHEN column_name = 'gromada_name' THEN 'Назва громади (НОВЕ)'
        WHEN column_name = 'settlement_id' THEN 'ID населеного пункту'
        WHEN column_name = 'settlement_name' THEN 'Назва населеного пункту'
        WHEN column_name = 'settlement_admin_level' THEN 'Адмін рівень НП'
        ELSE column_name
    END as description
FROM information_schema.columns
WHERE table_schema = 'osm_ukraine'
AND table_name = 'h3_admin_mapping'
ORDER BY ordinal_position;

-- 6. Статистика по існуючих даних (якщо є)
SELECT 
    'Існуючі дані' as info,
    COUNT(*) as total_records,
    COUNT(DISTINCT h3_resolution) as resolutions,
    MIN(h3_resolution) as min_resolution,
    MAX(h3_resolution) as max_resolution
FROM osm_ukraine.h3_admin_mapping;