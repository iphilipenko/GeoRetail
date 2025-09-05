-- =====================================================
-- Виправлення обласного центру Полтава
-- Файл: sql/22_fix_poltava_oblast_center.sql
-- =====================================================

-- 1. Видаляємо неправильну Полтаву (село)
DELETE FROM osm_ukraine.admin_boundaries 
WHERE admin_level = 7 
    AND osm_id = 30718977
    AND name_uk = 'Полтава';

-- 2. Переміщаємо правильну Полтаву з рівня 9 на рівень 7
UPDATE osm_ukraine.admin_boundaries
SET 
    admin_level = 7,
    parent_id = (
        SELECT id FROM osm_ukraine.admin_boundaries 
        WHERE admin_level = 4 AND name_uk = 'Полтавська область'
    ),
    region_name = 'Полтавська область',
    population = CASE 
        WHEN population IS NULL OR population < 10000 
        THEN 297600  -- Відоме населення Полтави
        ELSE population
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE osm_id = 1641691
    AND name_uk = 'Полтава';

-- 3. Якщо Полтави з osm_id=1641691 немає, вставляємо з osm_raw
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
SELECT 
    r.osm_id,
    7,
    'administrative',
    'Полтава',
    'Полтава',
    (r.tags->>'tags')::jsonb->>'name:en',
    o.id,
    o.name_uk,
    ST_Multi(r.geom),
    ROUND((ST_Area(r.geom::geography) / 1000000.0)::numeric, 2),
    297600,
    (r.tags->>'tags')::jsonb - '{name,name:uk,name:en,place,population,admin_level}'::text[]
FROM osm_ukraine.osm_raw r
CROSS JOIN osm_ukraine.admin_boundaries o
WHERE r.osm_id = 1641691
    AND o.admin_level = 4 
    AND o.name_uk = 'Полтавська область'
    AND NOT EXISTS (
        SELECT 1 FROM osm_ukraine.admin_boundaries 
        WHERE osm_id = 1641691
    );

-- 4. Опціонально спрощуємо геометрію Вінниці якщо занадто деталізована
UPDATE osm_ukraine.admin_boundaries
SET 
    geometry = ST_Multi(ST_SimplifyPreserveTopology(geometry, 0.0001)),
    updated_at = CURRENT_TIMESTAMP
WHERE admin_level = 7 
    AND name_uk = 'Вінниця'
    AND ST_NPoints(geometry) > 10000;

-- 5. Перевірка результатів
SELECT 
    'Виправлені обласні центри' as info,
    ab.name_uk as "Місто",
    ab.osm_id,
    p.name_uk as "Область",
    ab.population as "Населення",
    ab.area_km2 as "Площа км²",
    ST_X(ST_Centroid(ab.geometry)) as lon,
    ST_Y(ST_Centroid(ab.geometry)) as lat,
    ST_GeometryType(ab.geometry) as geom_type
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 7 
    AND ab.name_uk IN ('Вінниця', 'Полтава')
ORDER BY ab.name_uk;

-- 6. Загальна перевірка всіх обласних центрів
SELECT 
    ab.name_uk as "Обласний центр",
    p.name_uk as "Область",
    ab.area_km2 as "Площа км²"
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 7
ORDER BY ab.name_uk;