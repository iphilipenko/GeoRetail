-- ================================================================
-- Файл: 31_cleanup_level9_garbage.sql
-- Мета: Видалення непотрібних записів з рівня 9
--       (NULL назви, старі ради, райони міст)
-- Дата: 2025-01-04
-- ================================================================

-- 1. Перегляд записів для видалення
SELECT 
    'Для видалення' as category,
    COUNT(*) as total,
    COUNT(CASE WHEN name_uk IS NULL THEN 1 END) as null_names,
    COUNT(CASE WHEN name_uk LIKE '%рада%' THEN 1 END) as old_councils,
    COUNT(CASE WHEN name_uk LIKE '%район%' THEN 1 END) as city_districts
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9
AND (
    name_uk IS NULL 
    OR name_uk LIKE '%рада%' 
    OR name_uk LIKE '%район%'
);

-- 2. Збереження ID для видалення (для можливого відновлення)
CREATE TABLE osm_ukraine.deleted_level9_records AS
SELECT *
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9
AND (
    name_uk IS NULL 
    OR name_uk LIKE '%рада%' 
    OR name_uk LIKE '%район%'
);

-- 3. Видалення непотрібних записів
DELETE FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9
AND (
    name_uk IS NULL 
    OR name_uk LIKE '%рада%' 
    OR name_uk LIKE '%район%'
);

-- Очікуваний результат: DELETE 1059

-- 4. Перевірка після очищення
SELECT 
    'Залишилось на рівні 9' as description,
    COUNT(*) as total,
    COUNT(CASE WHEN population > 10000 THEN 1 END) as big_settlements,
    COUNT(CASE WHEN population BETWEEN 1000 AND 10000 THEN 1 END) as medium_settlements,
    COUNT(CASE WHEN population < 1000 THEN 1 END) as small_settlements,
    COUNT(CASE WHEN population IS NULL THEN 1 END) as no_population
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9;

-- Очікуваний результат: ~5,382 чистих населених пунктів