-- =====================================================
-- Оновлення region_name для областей
-- Файл: sql/05_update_oblast_region_name.sql
-- =====================================================

-- 1. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('update_oblast_region_name', 'started', 'Оновлення region_name для областей');

-- 2. Оновлення region_name - встановлюємо назву самої області
UPDATE osm_ukraine.admin_boundaries
SET 
    region_name = COALESCE(name_uk, name),
    updated_at = CURRENT_TIMESTAMP
WHERE 
    admin_level = 4;

-- 3. Логування результату
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'update_oblast_region_name',
    'completed',
    'Оновлено region_name для ' || COUNT(*) || ' областей',
    jsonb_build_object('updated_count', COUNT(*))
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 4;

-- 4. Перевірка результату - тепер region_name має співпадати з назвою області
SELECT 
    name_uk as "Назва області",
    region_name as "Регіон (оновлено)",
    CASE 
        WHEN name_uk = region_name THEN 'OK'
        WHEN name = region_name THEN 'OK' 
        ELSE 'Перевірити'
    END as "Статус",
    ROUND(area_km2::numeric) as "Площа (км²)",
    osm_id
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 4
ORDER BY name_uk;

-- 5. Підсумкова статистика
SELECT 
    'Підсумок оновлення' as info,
    COUNT(*) as total_oblast,
    COUNT(CASE WHEN region_name = name_uk OR region_name = name THEN 1 END) as correct_region_name,
    COUNT(CASE WHEN region_name IS NULL THEN 1 END) as null_region_name
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 4;