-- ================================================================
-- Файл: 27_migrate_communities_level9_to_6.sql
-- Мета: Міграція територіальних громад з рівня 9 на рівень 6
-- Дата: 2025-01-04
-- ================================================================

-- 1. Перевірка громад перед міграцією
SELECT 
    'Громади для міграції' as description,
    COUNT(*) as total_communities,
    COUNT(CASE WHEN parent_id IN (
        SELECT id FROM osm_ukraine.admin_boundaries WHERE admin_level = 5
    ) THEN 1 END) as parent_is_district,
    COUNT(DISTINCT region_name) as regions
FROM osm_ukraine.admin_boundaries 
WHERE admin_level = 9 
AND name_uk LIKE '%громада%';

-- 2. Міграція громад з рівня 9 на рівень 6
UPDATE osm_ukraine.admin_boundaries
SET admin_level = 6
WHERE admin_level = 9 
AND name_uk LIKE '%громада%';

-- Результат: Updated XXX rows

-- 3. Перевірка після міграції
SELECT 
    'Рівень 6 (громади)' as level_desc,
    COUNT(*) as count,
    COUNT(CASE WHEN name_uk LIKE '%міська громада%' THEN 1 END) as city_communities,
    COUNT(CASE WHEN name_uk LIKE '%селищна громада%' THEN 1 END) as town_communities,
    COUNT(CASE WHEN name_uk LIKE '%сільська громада%' THEN 1 END) as village_communities
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 4. Перевірка зв'язків громад з районами
SELECT 
    'Зв''язки громад' as description,
    COUNT(*) as total,
    COUNT(CASE WHEN p.admin_level = 5 THEN 1 END) as correct_parent_level5,
    COUNT(CASE WHEN p.admin_level != 5 OR p.admin_level IS NULL THEN 1 END) as wrong_parent
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 6;

-- 5. Перевірка що залишилось на рівні 9
SELECT 
    'Залишок на рівні 9' as description,
    COUNT(*) as total,
    COUNT(CASE WHEN name_uk IS NULL THEN 1 END) as null_names,
    COUNT(CASE WHEN name_uk LIKE '%рада%' THEN 1 END) as old_councils,
    COUNT(CASE WHEN name_uk LIKE '%район%' THEN 1 END) as city_districts
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9;

-- Очікуваний результат:
-- Рівень 6: 1,465 громад
-- Всі громади мають parent на рівні 5 (райони)
-- На рівні 9 залишилось ~6,441 об'єктів