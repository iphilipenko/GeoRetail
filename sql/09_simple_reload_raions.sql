-- =====================================================
-- Просте перезавантаження районів через ST_Within
-- Файл: sql/09_simple_reload_raions.sql
-- =====================================================

-- 1. Очищаємо таблицю районів
DELETE FROM osm_ukraine.admin_boundaries WHERE admin_level = 6;

-- 2. Завантажуємо всі райони що повністю потрапляють в області (ST_Within)
INSERT INTO osm_ukraine.admin_boundaries (
    osm_id,
    admin_level,
    boundary_type,
    name,
    name_uk,
    name_en,
    parent_id,
    region_name,
    geometry,
    area_km2,
    population,
    additional_tags
)
SELECT DISTINCT ON (r.osm_id)
    r.osm_id,
    6 as admin_level,
    'administrative' as boundary_type,
    COALESCE((r.tags->>'tags')::jsonb->>'name:uk', (r.tags->>'tags')::jsonb->>'name') as name,
    (r.tags->>'tags')::jsonb->>'name:uk' as name_uk,
    (r.tags->>'tags')::jsonb->>'name:en' as name_en,
    o.id as parent_id,
    r.region_name,
    CASE 
        WHEN ST_GeometryType(r.geom) = 'ST_Polygon' 
        THEN ST_Multi(r.geom)
        ELSE r.geom
    END as geometry,
    ROUND((ST_Area(r.geom::geography) / 1000000.0)::numeric, 2) as area_km2,
    CASE 
        WHEN (r.tags->>'tags')::jsonb->>'population' ~ '^\d+$' 
        THEN ((r.tags->>'tags')::jsonb->>'population')::INTEGER 
        ELSE NULL 
    END as population,
    (r.tags->>'tags')::jsonb - '{name,name:uk,name:en,boundary,admin_level,population}'::text[] as additional_tags
FROM osm_ukraine.osm_raw r
INNER JOIN osm_ukraine.admin_boundaries o 
    ON o.admin_level = 4 
    AND ST_Within(
        CASE 
            WHEN ST_GeometryType(r.geom) = 'ST_Polygon' THEN ST_Multi(r.geom)
            ELSE r.geom
        END, 
        o.geometry
    )
WHERE 
    r.tags->>'tags' IS NOT NULL
    AND (r.tags->>'tags')::jsonb->>'boundary' = 'administrative'
    AND (r.tags->>'tags')::jsonb->>'admin_level' = '6'
    AND ST_GeometryType(r.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
ORDER BY r.osm_id;

-- 3. Статистика результатів
SELECT 
    'Результат завантаження' as status,
    COUNT(*) as total_raions,
    COUNT(DISTINCT parent_id) as oblasti_with_raions
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 4. Розподіл по областях
SELECT 
    p.name_uk as "Область",
    COUNT(r.id) as "Кількість районів",
    STRING_AGG(r.name_uk, ', ' ORDER BY r.name_uk) as "Райони"
FROM osm_ukraine.admin_boundaries p
LEFT JOIN osm_ukraine.admin_boundaries r ON r.parent_id = p.id AND r.admin_level = 6
WHERE p.admin_level = 4
GROUP BY p.name_uk
ORDER BY COUNT(r.id) DESC, p.name_uk;

-- 5. Області без районів або з малою кількістю
SELECT 
    name_uk as "Область без районів/мало районів",
    'Немає районів в даних OSM' as "Коментар"
FROM osm_ukraine.admin_boundaries p
WHERE p.admin_level = 4
AND NOT EXISTS (
    SELECT 1 FROM osm_ukraine.admin_boundaries r 
    WHERE r.parent_id = p.id AND r.admin_level = 6
)
ORDER BY name_uk;