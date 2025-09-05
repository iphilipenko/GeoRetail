-- =====================================================
-- Аналіз H3 гексагонів перед прив'язкою (виправлено)
-- Файл: sql/13_analyze_h3_hexagons.sql
-- =====================================================

-- 1. Підрахунок кількості гексагонів по резолюціях
SELECT 
    resolution as "H3 резолюція",
    COUNT(*) as "Кількість гексагонів",
    CASE 
        WHEN resolution = 7 THEN 'Площа ~5.16 км²'
        WHEN resolution = 8 THEN 'Площа ~0.74 км²'
        WHEN resolution = 9 THEN 'Площа ~0.11 км²'
        WHEN resolution = 10 THEN 'Площа ~0.015 км²'
    END as "Приблизна площа гексагона"
FROM osm_ukraine.h3_grid
GROUP BY resolution
ORDER BY resolution;

-- 2. Перевірка наявності центроїдів
SELECT 
    resolution,
    COUNT(*) as total_hexagons,
    COUNT(center_point) as with_centroid,
    COUNT(*) - COUNT(center_point) as without_centroid
FROM osm_ukraine.h3_grid
GROUP BY resolution
ORDER BY resolution;

-- 3. Приклад даних з h3_grid
SELECT 
    h3_index,
    resolution,
    ST_AsText(center_point) as center_wkt,
    area_km2,
    ukraine_region
FROM osm_ukraine.h3_grid
WHERE resolution = 7
LIMIT 5;

-- 4. Тест прив'язки для кількох H3-7 гексагонів
WITH sample_h3 AS (
    SELECT 
        h3_index,
        center_point,
        ukraine_region
    FROM osm_ukraine.h3_grid
    WHERE resolution = 7 
        AND center_point IS NOT NULL
    LIMIT 5
)
SELECT 
    s.h3_index,
    s.ukraine_region as existing_region,
    o.name_uk as oblast,
    r.name_uk as raion,
    n.name_uk as settlement
FROM sample_h3 s
LEFT JOIN osm_ukraine.admin_boundaries o 
    ON o.admin_level = 4 
    AND ST_Contains(o.geometry, s.center_point)
LEFT JOIN osm_ukraine.admin_boundaries r 
    ON r.admin_level = 6 
    AND ST_Contains(r.geometry, s.center_point)
LEFT JOIN osm_ukraine.admin_boundaries n 
    ON n.admin_level IN (7, 8, 9) 
    AND ST_Contains(n.geometry, s.center_point);

-- 5. Перевірка індексів на геометрії
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'osm_ukraine' 
    AND tablename IN ('h3_grid', 'admin_boundaries')
    AND indexdef LIKE '%gist%';

-- 6. Оцінка часу обробки для H3-7
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM osm_ukraine.h3_grid h
JOIN osm_ukraine.admin_boundaries o 
    ON o.admin_level = 4 
    AND ST_Contains(o.geometry, h.center_point)
WHERE h.resolution = 7
LIMIT 1000;