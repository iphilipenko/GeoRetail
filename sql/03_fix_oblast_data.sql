-- =====================================================
-- Виправлення даних областей - тільки українські
-- Файл: sql/03_fix_oblast_data.sql
-- =====================================================

-- 1. Очищаємо таблицю від некоректних даних
DELETE FROM osm_ukraine.admin_boundaries WHERE admin_level = 4;

-- 2. Логування
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('fix_oblast', 'started', 'Виправлення завантаження областей - тільки українські');

-- 3. Список правильних українських областей для перевірки
WITH ukrainian_oblast_names AS (
    SELECT unnest(ARRAY[
        'Вінницька область', 'Волинська область', 'Дніпропетровська область',
        'Донецька область', 'Житомирська область', 'Закарпатська область',
        'Запорізька область', 'Івано-Франківська область', 'Київська область',
        'Кіровоградська область', 'Луганська область', 'Львівська область',
        'Миколаївська область', 'Одеська область', 'Полтавська область',
        'Рівненська область', 'Сумська область', 'Тернопільська область',
        'Харківська область', 'Херсонська область', 'Хмельницька область',
        'Черкаська область', 'Чернівецька область', 'Чернігівська область',
        'Автономна Республіка Крим', 'Республіка Крим', 'Київ', 'Севастополь'
    ]) AS name_uk
),
-- Створюємо тимчасову таблицю з дедуплікованими областями
deduplicated_oblast AS (
    SELECT DISTINCT ON (
        COALESCE((tags->>'tags')::jsonb->>'name:uk', (tags->>'tags')::jsonb->>'name')
    )
        osm_id,
        region_name,
        (tags->>'tags')::jsonb as tags_json,
        geom,
        COALESCE((tags->>'tags')::jsonb->>'name:uk', (tags->>'tags')::jsonb->>'name') as oblast_name
    FROM osm_ukraine.osm_raw
    WHERE 
        tags->>'tags' IS NOT NULL
        AND (tags->>'tags')::jsonb->>'boundary' = 'administrative'
        AND (tags->>'tags')::jsonb->>'admin_level' = '4'
        AND ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
        -- Фільтр тільки українські області
        AND (
            COALESCE((tags->>'tags')::jsonb->>'name:uk', (tags->>'tags')::jsonb->>'name') 
            IN (SELECT name_uk FROM ukrainian_oblast_names)
        )
    ORDER BY 
        COALESCE((tags->>'tags')::jsonb->>'name:uk', (tags->>'tags')::jsonb->>'name'),
        ST_Area(geom) DESC  -- Беремо найбільший полігон якщо є дублі
)
-- Вставляємо відфільтровані дані
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
    COALESCE(tags_json->>'name:uk', tags_json->>'name') as name,
    tags_json->>'name:uk' as name_uk,
    tags_json->>'name:en' as name_en,
    NULL as parent_id,
    region_name,
    CASE 
        WHEN ST_GeometryType(geom) = 'ST_Polygon' 
        THEN ST_Multi(geom)
        ELSE geom
    END as geometry,
    ROUND((ST_Area(geom::geography) / 1000000.0)::numeric, 2) as area_km2,
    CASE 
        WHEN tags_json->>'population' ~ '^\d+$' 
        THEN (tags_json->>'population')::INTEGER 
        ELSE NULL 
    END as population,
    tags_json - '{name,name:uk,name:en,boundary,admin_level,population}'::text[] as additional_tags
FROM deduplicated_oblast;

-- 4. Статистика після виправлення
WITH stats AS (
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT name_uk) as unique_names,
        STRING_AGG(name_uk, ', ' ORDER BY name_uk) as all_names
    FROM osm_ukraine.admin_boundaries
    WHERE admin_level = 4
)
SELECT 
    'Результат виправлення' as status,
    total_count,
    unique_names,
    CASE 
        WHEN total_count BETWEEN 24 AND 27 THEN 'OK'
        ELSE 'ПОТРЕБУЄ УВАГИ'
    END as check_status,
    all_names
FROM stats;

-- 5. Перевірка відсутніх областей
WITH expected_oblast AS (
    SELECT unnest(ARRAY[
        'Вінницька область', 'Волинська область', 'Дніпропетровська область',
        'Донецька область', 'Житомирська область', 'Закарпатська область',
        'Запорізька область', 'Івано-Франківська область', 'Київська область',
        'Кіровоградська область', 'Луганська область', 'Львівська область',
        'Миколаївська область', 'Одеська область', 'Полтавська область',
        'Рівненська область', 'Сумська область', 'Тернопільська область',
        'Харківська область', 'Херсонська область', 'Хмельницька область',
        'Черкаська область', 'Чернівецька область', 'Чернігівська область'
    ]) AS name
),
existing_oblast AS (
    SELECT DISTINCT name_uk as name 
    FROM osm_ukraine.admin_boundaries 
    WHERE admin_level = 4
)
SELECT 
    'Відсутні області:' as info,
    STRING_AGG(e.name, ', ') as missing_oblast
FROM expected_oblast e
LEFT JOIN existing_oblast ex ON e.name = ex.name
WHERE ex.name IS NULL
HAVING COUNT(*) > 0;

-- 6. Фінальний список завантажених областей
SELECT 
    name_uk as "Назва області",
    region_name as "Регіон",
    ROUND(area_km2::numeric) as "Площа (км²)",
    osm_id
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 4
ORDER BY name_uk;