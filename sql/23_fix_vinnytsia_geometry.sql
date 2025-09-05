-- =====================================================
-- Діагностика та виправлення геометрії Вінниці
-- Файл: sql/23_fix_vinnytsia_geometry.sql
-- =====================================================

-- 1. Діагностика поточної геометрії Вінниці
SELECT 
    'Поточна геометрія Вінниці' as info,
    osm_id,
    ST_GeometryType(geometry) as geom_type,
    ST_IsValid(geometry) as is_valid,
    ST_IsSimple(geometry) as is_simple,
    ST_NPoints(geometry) as points_count,
    ST_NumGeometries(geometry) as num_geometries,
    area_km2
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 7 AND name_uk = 'Вінниця';

-- 2. Перевірка причин невалідності (якщо є)
SELECT 
    'Причина невалідності' as info,
    ST_IsValidReason(geometry) as reason
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 7 AND name_uk = 'Вінниця'
    AND NOT ST_IsValid(geometry);

-- 3. Пошук альтернативних геометрій для Вінниці
SELECT 
    'Альтернативні геометрії Вінниці' as info,
    osm_id,
    (tags->>'tags')::jsonb->>'place' as place,
    ST_GeometryType(geom) as geom_type,
    ST_IsValid(geom) as is_valid,
    ST_NPoints(geom) as points_count,
    ROUND((ST_Area(geom::geography) / 1000000.0)::numeric, 2) as area_km2
FROM osm_ukraine.osm_raw
WHERE (
    (tags->>'tags')::jsonb->>'name:uk' = 'Вінниця'
    OR (tags->>'tags')::jsonb->>'name' = 'Вінниця'
)
AND ST_GeometryType(geom) LIKE '%Polygon%'
ORDER BY 
    CASE WHEN ST_IsValid(geom) THEN 0 ELSE 1 END,
    ST_Area(geom::geography) DESC;

-- 4. Виправлення геометрії Вінниці - варіант 1: використати валідну геометрію з osm_raw
UPDATE osm_ukraine.admin_boundaries ab
SET 
    geometry = (
        SELECT ST_Multi(ST_MakeValid(geom))
        FROM osm_ukraine.osm_raw
        WHERE osm_id = 361818  -- osm_id Вінниці
        LIMIT 1
    ),
    updated_at = CURRENT_TIMESTAMP
WHERE admin_level = 7 
    AND name_uk = 'Вінниця'
    AND osm_id = 361818;



-- 7. Перевірка результату
SELECT 
    'Виправлена геометрія Вінниці' as info,
    ab.osm_id,
    ST_GeometryType(ab.geometry) as geom_type,
    ST_IsValid(ab.geometry) as is_valid,
    ST_NPoints(ab.geometry) as points_count,
    ab.area_km2,
    p.name_uk as oblast
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 7 AND ab.name_uk = 'Вінниця';

-- 8. Перевірка всіх обласних центрів на валідність геометрії
SELECT 
    'Проблемні обласні центри' as info,
    name_uk,
    osm_id,
    ST_IsValid(geometry) as is_valid,
    ST_GeometryType(geometry) as geom_type,
    area_km2
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 7
    AND NOT ST_IsValid(geometry);