-- ================================================================
-- Файл: 33_find_correct_communities_for_settlements.sql
-- Мета: Знайти правильні громади для 76 населених пунктів
--       через просторове входження
-- Дата: 2025-01-04
-- ================================================================

-- 1. Створити таблицю для проблемних населених пунктів
CREATE TEMP TABLE problem_settlements AS
SELECT 
    ab.id,
    ab.osm_id,
    ab.name_uk,
    ab.parent_id,
    ab.geometry,
    p.admin_level as current_parent_level
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 9
AND (p.admin_level != 6 OR p.admin_level IS NULL OR ab.parent_id IS NULL);

-- 2. Знайти громади через геометрію для кожного проблемного поселення
WITH matches AS (
    SELECT DISTINCT ON (ps.id)
        ps.id as settlement_id,
        ps.name_uk as settlement_name,
        ps.parent_id as old_parent_id,
        c.id as community_id,
        c.name_uk as community_name,
        ST_Area(ST_Intersection(ps.geometry, c.geometry)) / NULLIF(ST_Area(ps.geometry), 0) * 100 as overlap_pct
    FROM problem_settlements ps
    JOIN osm_ukraine.admin_boundaries c 
        ON ST_Intersects(ps.geometry, c.geometry)
        AND c.admin_level = 6
    ORDER BY ps.id, overlap_pct DESC
)
SELECT 
    settlement_name,
    old_parent_id,
    community_id,
    community_name,
    ROUND(overlap_pct::numeric, 2) as overlap_percent
FROM matches
WHERE overlap_pct > 50  -- Тільки якщо перекриття більше 50%
ORDER BY settlement_name
LIMIT 30;

-- 3. Перевірка кримських поселень окремо
SELECT 
    COUNT(*) as crimea_settlements
FROM problem_settlements ps
WHERE ps.name_uk LIKE '%поселення%';