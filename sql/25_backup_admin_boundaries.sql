-- ================================================================
-- Файл: 25_backup_admin_boundaries.sql
-- Мета: Створення резервної копії таблиці admin_boundaries 
--       перед міграцією адміністративних рівнів
-- Дата: 2025-01-04
-- ================================================================

-- 1. Створюємо повну резервну копію таблиці
CREATE TABLE osm_ukraine.admin_boundaries_backup_20250104 AS 
SELECT * FROM osm_ukraine.admin_boundaries;

-- 2. Додаємо коментар до таблиці
COMMENT ON TABLE osm_ukraine.admin_boundaries_backup_20250104 IS 
'Резервна копія таблиці admin_boundaries перед міграцією рівнів (райони 6->5, громади 9->6)';

-- 3. Перевірка успішності копіювання
SELECT 
    'Original' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT admin_level) as unique_levels,
    MIN(admin_level) as min_level,
    MAX(admin_level) as max_level
FROM osm_ukraine.admin_boundaries
UNION ALL
SELECT 
    'Backup' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT admin_level) as unique_levels,
    MIN(admin_level) as min_level,
    MAX(admin_level) as max_level
FROM osm_ukraine.admin_boundaries_backup_20250104;

-- Очікуваний результат: однакова кількість записів в обох таблицях