-- ================================================================
-- Файл: 34_update_settlements_parent_ids.sql
-- Мета: Оновлення parent_id для населених пунктів на правильні громади
-- Дата: 2025-01-04
-- ================================================================

-- 1. Перевірка кримських поселень (вони можуть не мати громад)
SELECT 
    name_uk,
    parent_id
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9
AND name_uk LIKE '%поселення%'
LIMIT 10;

-- 2. Оновлення parent_id для знайдених відповідностей
WITH correct_parents AS (
    SELECT DISTINCT ON (s.id)
        s.id as settlement_id,
        c.id as community_id
    FROM osm_ukraine.admin_boundaries s
    JOIN osm_ukraine.admin_boundaries c 
        ON ST_Intersects(s.geometry, c.geometry)
        AND c.admin_level = 6
    WHERE s.admin_level = 9
    AND (s.parent_id NOT IN (
        SELECT id FROM osm_ukraine.admin_boundaries WHERE admin_level = 6
    ) OR s.parent_id IS NULL)
    AND ST_Area(ST_Intersection(s.geometry, c.geometry)) / NULLIF(ST_Area(s.geometry), 0) > 0.5
    ORDER BY s.id, ST_Area(ST_Intersection(s.geometry, c.geometry)) / NULLIF(ST_Area(s.geometry), 0) DESC
)
UPDATE osm_ukraine.admin_boundaries ab
SET parent_id = cp.community_id
FROM correct_parents cp
WHERE ab.id = cp.settlement_id;

-- Очікуваний результат: UPDATE ~34-40

-- 3. Перевірка населених пунктів що залишились без громад (Крим та інші)
SELECT 
    name_uk,
    parent_id,
    p.name_uk as parent_name,
    p.admin_level as parent_level
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 9
AND (p.admin_level != 6 OR p.admin_level IS NULL)
ORDER BY name_uk
LIMIT 20;

-- 4. Фінальна статистика
SELECT 
    'Фінальна перевірка рівня 9' as description,
    COUNT(*) as total,
    COUNT(CASE WHEN p.admin_level = 6 THEN 1 END) as with_community,
    COUNT(CASE WHEN p.admin_level != 6 OR p.admin_level IS NULL THEN 1 END) as without_community
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 9;