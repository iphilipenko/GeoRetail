-- =====================================================
-- Аналіз та виправлення даних районів
-- Файл: sql/06_analyze_fix_raion_data.sql
-- =====================================================

-- 1. Аналіз проблемних районів (назви що вказують на інші країни)
WITH foreign_raions AS (
    SELECT 
        name,
        name_uk,
        parent_id,
        p.name as oblast_name
    FROM osm_ukraine.admin_boundaries r
    LEFT JOIN osm_ukraine.admin_boundaries p ON r.parent_id = p.id
    WHERE r.admin_level = 6
    AND (
        name LIKE '%повіт%' 
        OR name LIKE '%powiat%'
        OR name LIKE '%медьє%'
        OR name LIKE '%megye%'
        OR name LIKE '%міський округ%'
        OR name LIKE '%городской округ%'
        OR name_uk LIKE '%повіт%'
        OR name_uk LIKE '%медьє%'
        OR name_uk LIKE '%міський округ%'
    )
)
SELECT 
    'Райони інших країн (за назвою)' as analysis,
    COUNT(*) as count,
    STRING_AGG(DISTINCT name, ', ' ORDER BY name) as sample_names
FROM foreign_raions;

-- 2. Перевірка різниці між ST_Within та ST_Intersects
WITH within_check AS (
    SELECT 
        r.osm_id,
        (r.tags->>'tags')::jsonb->>'name' as raion_name,
        -- Перевірка через ST_Within
        (SELECT o.id FROM osm_ukraine.admin_boundaries o 
         WHERE o.admin_level = 4 
         AND ST_Within(
            CASE 
                WHEN ST_GeometryType(r.geom) = 'ST_Polygon' THEN ST_Multi(r.geom)
                ELSE r.geom
            END, 
            o.geometry
         ) LIMIT 1) as oblast_id_within,
        -- Перевірка через ST_Intersects
        (SELECT o.id FROM osm_ukraine.admin_boundaries o 
         WHERE o.admin_level = 4 
         AND ST_Intersects(
            CASE 
                WHEN ST_GeometryType(r.geom) = 'ST_Polygon' THEN ST_Multi(r.geom)
                ELSE r.geom
            END, 
            o.geometry
         ) LIMIT 1) as oblast_id_intersects
    FROM osm_ukraine.osm_raw r
    WHERE 
        r.tags->>'tags' IS NOT NULL
        AND (r.tags->>'tags')::jsonb->>'boundary' = 'administrative'
        AND (r.tags->>'tags')::jsonb->>'admin_level' = '6'
        AND ST_GeometryType(r.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
)
SELECT 
    'Порівняння ST_Within vs ST_Intersects' as analysis,
    COUNT(*) as total_raions,
    COUNT(oblast_id_within) as with_st_within,
    COUNT(oblast_id_intersects) as with_st_intersects,
    COUNT(CASE WHEN oblast_id_intersects IS NOT NULL AND oblast_id_within IS NULL THEN 1 END) as only_intersects,
    COUNT(CASE WHEN oblast_id_intersects IS NULL THEN 1 END) as no_oblast
FROM within_check;

-- 3. Список районів які є тільки в ST_Intersects але не в ST_Within (прикордонні)
WITH border_raions AS (
    SELECT 
        r.osm_id,
        (r.tags->>'tags')::jsonb->>'name' as name,
        (r.tags->>'tags')::jsonb->>'name:uk' as name_uk,
        r.region_name,
        -- Область через ST_Intersects
        (SELECT o.name FROM osm_ukraine.admin_boundaries o 
         WHERE o.admin_level = 4 
         AND ST_Intersects(
            CASE 
                WHEN ST_GeometryType(r.geom) = 'ST_Polygon' THEN ST_Multi(r.geom)
                ELSE r.geom
            END, 
            o.geometry
         ) LIMIT 1) as oblast_intersects,
        -- Перевірка чи є ST_Within
        EXISTS (
            SELECT 1 FROM osm_ukraine.admin_boundaries o 
            WHERE o.admin_level = 4 
            AND ST_Within(
                CASE 
                    WHEN ST_GeometryType(r.geom) = 'ST_Polygon' THEN ST_Multi(r.geom)
                    ELSE r.geom
                END, 
                o.geometry
            )
        ) as has_within
    FROM osm_ukraine.osm_raw r
    WHERE 
        r.tags->>'tags' IS NOT NULL
        AND (r.tags->>'tags')::jsonb->>'boundary' = 'administrative'
        AND (r.tags->>'tags')::jsonb->>'admin_level' = '6'
        AND ST_GeometryType(r.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
)
SELECT 
    'Прикордонні райони (тільки ST_Intersects)' as type,
    name,
    name_uk,
    oblast_intersects,
    region_name
FROM border_raions
WHERE NOT has_within AND oblast_intersects IS NOT NULL
ORDER BY name
LIMIT 20;

-- 4. Очищаємо таблицю від районів інших країн
DELETE FROM osm_ukraine.admin_boundaries 
WHERE admin_level = 6
AND (
    name LIKE '%повіт%' 
    OR name LIKE '%powiat%'
    OR name LIKE '%медьє%'
    OR name LIKE '%megye%'
    OR name LIKE '%міський округ%'
    OR name LIKE '%городской округ%'
    OR name_uk LIKE '%повіт%'
    OR name_uk LIKE '%медьє%'
    OR name_uk LIKE '%міський округ%'
);

-- 5. Видаляємо райони які не мають області через ST_Within
DELETE FROM osm_ukraine.admin_boundaries r
WHERE r.admin_level = 6 
AND NOT EXISTS (
    SELECT 1 
    FROM osm_ukraine.admin_boundaries o
    WHERE o.admin_level = 4 
    AND ST_Within(r.geometry, o.geometry)
);

-- 6. Оновлюємо parent_id через ST_Within для більшої точності
UPDATE osm_ukraine.admin_boundaries r
SET parent_id = (
    SELECT o.id 
    FROM osm_ukraine.admin_boundaries o
    WHERE o.admin_level = 4 
    AND ST_Within(r.geometry, o.geometry)
    LIMIT 1
),
updated_at = CURRENT_TIMESTAMP
WHERE r.admin_level = 6;

-- 7. Фінальна статистика після очищення
SELECT 
    'Після очищення' as status,
    COUNT(*) as total_raions,
    COUNT(DISTINCT parent_id) as oblasti_with_raions,
    COUNT(CASE WHEN parent_id IS NULL THEN 1 END) as without_oblast
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 8. Кількість районів по областях після очищення
SELECT 
    p.name as "Область",
    COUNT(r.id) as "Кількість районів"
FROM osm_ukraine.admin_boundaries r
JOIN osm_ukraine.admin_boundaries p ON r.parent_id = p.id
WHERE r.admin_level = 6
GROUP BY p.name
ORDER BY p.name;