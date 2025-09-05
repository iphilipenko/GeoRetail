-- =====================================================
-- Заповнення таблиці admin_boundaries даними районів
-- Файл: sql/04_populate_raion_data.sql
-- =====================================================

-- 1. Логування початку процесу
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('populate_raion', 'started', 'Початок заповнення районів (admin_level=6)');

-- 2. Створюємо тимчасову таблицю з районами
DROP TABLE IF EXISTS temp_raion_data;

CREATE TEMP TABLE temp_raion_data AS
WITH raion_with_oblast AS (
    SELECT DISTINCT ON (r.osm_id)
        r.osm_id,
        r.region_name,
        (r.tags->>'tags')::jsonb as tags_json,
        r.geom,
        -- Визначаємо область для кожного району через ST_Within або ST_Intersects
        o.id as oblast_id,
        o.name as oblast_name
    FROM osm_ukraine.osm_raw r
    LEFT JOIN osm_ukraine.admin_boundaries o 
        ON o.admin_level = 4 
        AND ST_Intersects(
            CASE 
                WHEN ST_GeometryType(r.geom) = 'ST_Polygon' THEN ST_Multi(r.geom)
                WHEN ST_GeometryType(r.geom) = 'ST_MultiPolygon' THEN r.geom
                ELSE NULL
            END, 
            o.geometry
        )
    WHERE 
        r.tags->>'tags' IS NOT NULL
        AND (r.tags->>'tags')::jsonb->>'boundary' = 'administrative'
        AND (r.tags->>'tags')::jsonb->>'admin_level' = '6'
        AND ST_GeometryType(r.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
        -- Фільтруємо тільки райони що належать українським областям
        AND o.id IS NOT NULL
    ORDER BY r.osm_id, ST_Area(r.geom) DESC
)
SELECT 
    osm_id,
    tags_json->>'name' as name,
    tags_json->>'name:uk' as name_uk,
    tags_json->>'name:en' as name_en,
    tags_json->>'population' as population_text,
    region_name,
    oblast_id,
    oblast_name,
    tags_json as additional_tags,
    CASE 
        WHEN ST_GeometryType(geom) = 'ST_Polygon' 
        THEN ST_Multi(geom)
        ELSE geom
    END as geometry,
    ROUND((ST_Area(geom::geography) / 1000000.0)::numeric, 2) as area_km2
FROM raion_with_oblast;

-- 3. Перевірка кількості знайдених районів
DO $$
DECLARE
    raion_count INTEGER;
    oblast_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO raion_count FROM temp_raion_data;
    SELECT COUNT(DISTINCT oblast_id) INTO oblast_count FROM temp_raion_data WHERE oblast_id IS NOT NULL;
    
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES ('populate_raion', 'info', 
            'Знайдено районів: ' || raion_count || ' в ' || oblast_count || ' областях',
            jsonb_build_object('raion_count', raion_count, 'oblast_count', oblast_count));
END $$;

-- 4. Вставка даних в основну таблицю
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
    osm_id,
    6 as admin_level,
    'administrative' as boundary_type,
    COALESCE(name_uk, name) as name,
    name_uk,
    name_en,
    oblast_id as parent_id,
    region_name,
    geometry,
    area_km2,
    CASE 
        WHEN population_text ~ '^\d+$' 
        THEN population_text::INTEGER 
        ELSE NULL 
    END as population,
    additional_tags - '{name,name:uk,name:en,boundary,admin_level,population}'::text[] as additional_tags
FROM temp_raion_data
ON CONFLICT (osm_id) DO UPDATE SET
    name = EXCLUDED.name,
    name_uk = EXCLUDED.name_uk,
    name_en = EXCLUDED.name_en,
    parent_id = EXCLUDED.parent_id,
    region_name = EXCLUDED.region_name,
    geometry = EXCLUDED.geometry,
    area_km2 = EXCLUDED.area_km2,
    population = EXCLUDED.population,
    additional_tags = EXCLUDED.additional_tags,
    updated_at = CURRENT_TIMESTAMP;

-- 5. Статистика по районах
WITH raion_stats AS (
    SELECT 
        p.name as oblast_name,
        COUNT(r.id) as raion_count,
        STRING_AGG(r.name_uk, ', ' ORDER BY r.name_uk) as raion_names
    FROM osm_ukraine.admin_boundaries r
    LEFT JOIN osm_ukraine.admin_boundaries p ON r.parent_id = p.id
    WHERE r.admin_level = 6
    GROUP BY p.name
)
SELECT 
    'Райони по областях' as info,
    oblast_name,
    raion_count,
    LEFT(raion_names, 100) || '...' as sample_raions
FROM raion_stats
ORDER BY oblast_name
LIMIT 10;

-- 6. Загальна статистика
SELECT 
    'Загальна статистика районів' as status,
    COUNT(*) as total_raions,
    COUNT(DISTINCT parent_id) as oblasti_with_raions,
    MIN(area_km2) as min_area_km2,
    MAX(area_km2) as max_area_km2,
    ROUND(AVG(area_km2)::numeric, 2) as avg_area_km2
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 7. Перевірка районів без прив'язки до області
SELECT 
    'Райони без області' as problem,
    COUNT(*) as count
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6 AND parent_id IS NULL;

-- Очищення тимчасової таблиці
DROP TABLE IF EXISTS temp_raion_data;