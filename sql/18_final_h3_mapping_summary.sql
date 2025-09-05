-- =====================================================
-- Фінальний підсумок прив'язки H3 до адмінодиниць
-- Файл: sql/18_final_h3_mapping_summary.sql
-- =====================================================

-- 1. Загальна статистика по всіх резолюціях
SELECT 
    h3_resolution as "H3 резолюція",
    COUNT(*) as "Прив'язано гексагонів",
    COUNT(DISTINCT oblast_id) as "Областей",
    COUNT(raion_id) as "З районом",
    COUNT(settlement_id) as "З населеним пунктом",
    ROUND(100.0 * COUNT(raion_id) / COUNT(*), 2) as "% з районом",
    ROUND(100.0 * COUNT(settlement_id) / COUNT(*), 2) as "% з НП"
FROM osm_ukraine.h3_admin_mapping
GROUP BY h3_resolution
ORDER BY h3_resolution;

-- 2. Статистика по областях
SELECT 
    oblast_name as "Область",
    COUNT(CASE WHEN h3_resolution = 7 THEN 1 END) as "H3-7",
    COUNT(CASE WHEN h3_resolution = 8 THEN 1 END) as "H3-8",
    COUNT(CASE WHEN h3_resolution = 9 THEN 1 END) as "H3-9",
    COUNT(CASE WHEN h3_resolution = 10 THEN 1 END) as "H3-10",
    COUNT(*) as "Всього"
FROM osm_ukraine.h3_admin_mapping
GROUP BY oblast_name
ORDER BY COUNT(*) DESC;

-- 3. Топ-10 населених пунктів за кількістю гексагонів
SELECT 
    settlement_name as "Населений пункт",
    oblast_name as "Область",
    COUNT(CASE WHEN h3_resolution = 7 THEN 1 END) as "H3-7",
    COUNT(CASE WHEN h3_resolution = 8 THEN 1 END) as "H3-8",
    COUNT(CASE WHEN h3_resolution = 9 THEN 1 END) as "H3-9",
    COUNT(CASE WHEN h3_resolution = 10 THEN 1 END) as "H3-10",
    COUNT(*) as "Всього гексагонів"
FROM osm_ukraine.h3_admin_mapping
WHERE settlement_name IS NOT NULL
GROUP BY settlement_name, oblast_name
ORDER BY COUNT(*) DESC
LIMIT 10;

-- 4. Перевірка покриття території України
WITH coverage AS (
    SELECT 
        h3_resolution,
        (SELECT COUNT(*) FROM osm_ukraine.h3_grid WHERE resolution = h3_resolution) as total_in_grid,
        COUNT(*) as mapped,
        (SELECT COUNT(*) FROM osm_ukraine.h3_grid WHERE resolution = h3_resolution) - COUNT(*) as not_mapped
    FROM osm_ukraine.h3_admin_mapping
    GROUP BY h3_resolution
)
SELECT 
    h3_resolution as "Резолюція",
    total_in_grid as "Всього в h3_grid",
    mapped as "Прив'язано",
    not_mapped as "Не прив'язано",
    ROUND(100.0 * mapped / total_in_grid, 2) as "% покриття"
FROM coverage
ORDER BY h3_resolution;

-- 5. Приклад використання даних - знайти всі H3-10 гексагони в конкретному місті
SELECT 
    COUNT(*) as hexagon_count,
    settlement_name,
    raion_name,
    oblast_name
FROM osm_ukraine.h3_admin_mapping
WHERE h3_resolution = 10
    AND settlement_name = 'Львів'
GROUP BY settlement_name, raion_name, oblast_name;

-- 6. Загальний підсумок
SELECT 
    'Фінальний підсумок' as info,
    COUNT(DISTINCT h3_index) as "Унікальних гексагонів",
    COUNT(DISTINCT oblast_id) as "Областей",
    COUNT(DISTINCT raion_id) as "Районів",
    COUNT(DISTINCT settlement_id) as "Населених пунктів",
    SUM(CASE WHEN h3_resolution = 7 THEN 1 ELSE 0 END) as "H3-7",
    SUM(CASE WHEN h3_resolution = 8 THEN 1 ELSE 0 END) as "H3-8",
    SUM(CASE WHEN h3_resolution = 9 THEN 1 ELSE 0 END) as "H3-9",
    SUM(CASE WHEN h3_resolution = 10 THEN 1 ELSE 0 END) as "H3-10"
FROM osm_ukraine.h3_admin_mapping;