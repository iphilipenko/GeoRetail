-- =====================================================
-- Заповнення таблиці admin_boundaries даними областей
-- Файл: sql/02_populate_oblast_data.sql
-- =====================================================

-- 1. Очистка логів попередніх запусків
DELETE FROM osm_ukraine.admin_processing_log WHERE process_name = 'populate_oblast';

-- 2. Логування початку процесу
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('populate_oblast', 'started', 'Початок заповнення областей (admin_level=4)');

-- 3. Створення тимчасової таблиці для дедуплікації областей
DROP TABLE IF EXISTS temp_oblast_data;

CREATE TEMP TABLE temp_oblast_data AS
WITH deduplicated_oblast AS (
    SELECT DISTINCT ON (osm_id)
        osm_id,
        region_name,
        (tags->>'tags')::jsonb as tags_json,
        geom,
        -- Пріоритет: спочатку полігони, потім мультиполігони
        CASE 
            WHEN ST_GeometryType(geom) = 'ST_Polygon' THEN 1
            WHEN ST_GeometryType(geom) = 'ST_MultiPolygon' THEN 2
            ELSE 3
        END as geom_priority
    FROM osm_ukraine.osm_raw
    WHERE 
        tags->>'tags' IS NOT NULL
        AND (tags->>'tags')::jsonb->>'boundary' = 'administrative'
        AND (tags->>'tags')::jsonb->>'admin_level' = '4'
        AND ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
        -- Фільтруємо тільки українські області
        AND (
            (tags->>'tags')::jsonb->>'name' LIKE '%область%'
            OR (tags->>'tags')::jsonb->>'name:uk' LIKE '%область%'
            OR region_name IN (
                'Cherkasy', 'Chernihiv', 'Chernivci', 'Dnipro', 'Donetsk', 
                'IF', 'Kharkiv', 'Kherson', 'Khmelnytskiy', 'Kirovograd', 
                'Kyiv', 'Luhansk', 'Lviv', 'Mykolaiv', 'Odesa', 'Poltava', 
                'Rivne', 'Sumy', 'Ternopil', 'Uzhgorod', 'Vinnytsya', 
                'Volyn', 'Zaporizh', 'Zhytomyr'
            )
        )
    ORDER BY osm_id, geom_priority
)
SELECT 
    osm_id,
    tags_json->>'name' as name,
    tags_json->>'name:uk' as name_uk,
    tags_json->>'name:en' as name_en,
    tags_json->>'population' as population_text,
    region_name,
    tags_json as additional_tags,
    -- Конвертуємо всі геометрії в MultiPolygon для уніфікації
    CASE 
        WHEN ST_GeometryType(geom) = 'ST_Polygon' 
        THEN ST_Multi(geom)
        ELSE geom
    END as geometry,
    -- Розраховуємо площу в км²
    ROUND((ST_Area(geom::geography) / 1000000.0)::numeric, 2) as area_km2
FROM deduplicated_oblast;

-- 4. Перевірка кількості знайдених областей
DO $$
DECLARE
    oblast_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO oblast_count FROM temp_oblast_data;
    
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES ('populate_oblast', 'info', 
            'Знайдено областей: ' || oblast_count,
            jsonb_build_object('count', oblast_count));
    
    IF oblast_count < 24 THEN
        INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
        VALUES ('populate_oblast', 'warning', 
                'Увага! Знайдено менше 24 областей. Можливо, не всі дані присутні.');
    END IF;
END $$;

-- 5. Вставка даних в основну таблицю
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
    4 as admin_level,
    'administrative' as boundary_type,
    COALESCE(name_uk, name) as name,  -- Пріоритет українській назві
    name_uk,
    name_en,
    NULL as parent_id,  -- Області не мають батьківського елемента
    region_name,
    geometry,
    area_km2,
    CASE 
        WHEN population_text ~ '^\d+$' 
        THEN population_text::INTEGER 
        ELSE NULL 
    END as population,
    additional_tags - '{name,name:uk,name:en,boundary,admin_level,population}'::text[] as additional_tags
FROM temp_oblast_data
ON CONFLICT (osm_id) DO UPDATE SET
    name = EXCLUDED.name,
    name_uk = EXCLUDED.name_uk,
    name_en = EXCLUDED.name_en,
    region_name = EXCLUDED.region_name,
    geometry = EXCLUDED.geometry,
    area_km2 = EXCLUDED.area_km2,
    population = EXCLUDED.population,
    additional_tags = EXCLUDED.additional_tags,
    updated_at = CURRENT_TIMESTAMP;

-- 6. Додаткова вставка для Києва та Севастополя як міст зі спеціальним статусом
-- (якщо вони є в даних як admin_level=4)
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
    additional_tags
)
SELECT DISTINCT ON (osm_id)
    osm_id,
    4 as admin_level,
    'administrative' as boundary_type,
    (tags->>'tags')::jsonb->>'name' as name,
    (tags->>'tags')::jsonb->>'name:uk' as name_uk,
    (tags->>'tags')::jsonb->>'name:en' as name_en,
    NULL as parent_id,
    region_name,
    CASE 
        WHEN ST_GeometryType(geom) = 'ST_Polygon' 
        THEN ST_Multi(geom)
        ELSE geom
    END as geometry,
    ROUND((ST_Area(geom::geography) / 1000000.0)::numeric, 2) as area_km2,
    (tags->>'tags')::jsonb as additional_tags
FROM osm_ukraine.osm_raw
WHERE 
    tags->>'tags' IS NOT NULL
    AND (tags->>'tags')::jsonb->>'boundary' = 'administrative'
    AND (tags->>'tags')::jsonb->>'admin_level' = '4'
    AND ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
    AND (
        (tags->>'tags')::jsonb->>'name' IN ('Київ', 'Севастополь', 'Kyiv', 'Sevastopol')
        OR (tags->>'tags')::jsonb->>'name:uk' IN ('Київ', 'Севастополь')
    )
ON CONFLICT (osm_id) DO NOTHING;

-- 7. Фінальна статистика
WITH stats AS (
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT region_name) as unique_regions,
        ROUND(AVG(area_km2)::numeric, 2) as avg_area_km2,
        ROUND(SUM(area_km2)::numeric, 2) as total_area_km2
    FROM osm_ukraine.admin_boundaries
    WHERE admin_level = 4
)
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'populate_oblast',
    'completed',
    'Заповнення областей завершено успішно',
    jsonb_build_object(
        'total_count', total_count,
        'unique_regions', unique_regions,
        'avg_area_km2', avg_area_km2,
        'total_area_km2', total_area_km2
    )
FROM stats;

-- 8. Виведення результатів
SELECT 
    'Області завантажені' as status,
    COUNT(*) as total_count,
    COUNT(DISTINCT region_name) as unique_regions,
    STRING_AGG(DISTINCT name_uk, ', ' ORDER BY name_uk) as oblasti_names
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 4;

-- 9. Перевірка наявності всіх областей
SELECT 
    'Перевірка областей' as check_type,
    name_uk,
    region_name,
    ROUND(area_km2::numeric) as area_km2
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 4
ORDER BY name_uk;