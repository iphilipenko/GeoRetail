-- ================================================================
-- Файл: 26_migrate_districts_level6_to_5.sql
-- Мета: Міграція районів з адміністративного рівня 6 на рівень 5
-- Дата: 2025-01-04
-- ================================================================

-- 1. Перевірка перед міграцією
SELECT 
    'Райони для міграції' as description,
    COUNT(*) as count,
    STRING_AGG(DISTINCT region_name, ', ' ORDER BY region_name) as regions
FROM osm_ukraine.admin_boundaries 
WHERE admin_level = 6;

-- 2. Міграція районів з рівня 6 на рівень 5
UPDATE osm_ukraine.admin_boundaries
SET admin_level = 5
WHERE admin_level = 6;

-- Результат: Updated XXX rows

-- 3. Перевірка після міграції
SELECT 
    admin_level,
    COUNT(*) as count,
    STRING_AGG(DISTINCT SUBSTRING(name_uk, 1, 20), ', ' ORDER BY SUBSTRING(name_uk, 1, 20)) as examples
FROM osm_ukraine.admin_boundaries
WHERE admin_level IN (5, 6)
GROUP BY admin_level
ORDER BY admin_level;

-- 4. Перевірка зв'язків (районні центри повинні вказувати на рівень 5)
SELECT 
    'Районні центри' as description,
    COUNT(*) as total,
    COUNT(CASE WHEN p.admin_level = 5 THEN 1 END) as correct_parent,
    COUNT(CASE WHEN p.admin_level != 5 OR p.admin_level IS NULL THEN 1 END) as wrong_parent
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 8;

-- Очікуваний результат: 
-- Рівень 5: 134 районів
-- Рівень 6: 0 записів
-- Всі 104 районні центри мають parent на рівні 5