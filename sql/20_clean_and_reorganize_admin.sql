-- =====================================================
-- Повна реорганізація адміністративних рівнів
-- Файл: sql/20_clean_and_reorganize_admin.sql
-- =====================================================

-- КРОК 1: ОЧИЩЕННЯ
-- =====================================================

-- Очищаємо таблицю прив'язки H3
TRUNCATE TABLE osm_ukraine.h3_admin_mapping;

-- Логування
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('reorganize_admin_levels', 'started', 'Початок реорганізації адміністративних рівнів');

-- КРОК 2: РЕОРГАНІЗАЦІЯ РІВНІВ
-- =====================================================

-- Переміщаємо все з рівня 7 на рівень 9
UPDATE osm_ukraine.admin_boundaries 
SET admin_level = 9, updated_at = CURRENT_TIMESTAMP
WHERE admin_level = 7;

-- Переміщаємо все з рівня 8 на рівень 9
UPDATE osm_ukraine.admin_boundaries 
SET admin_level = 9, updated_at = CURRENT_TIMESTAMP
WHERE admin_level = 8;

-- Статистика після переміщення
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'reorganize_admin_levels',
    'info',
    'Переміщено на рівень 9',
    jsonb_build_object(
        'total_on_level_9', COUNT(*),
        'unique_names', COUNT(DISTINCT name_uk)
    )
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9;

-- КРОК 3: ЗАВАНТАЖЕННЯ ОБЛАСНИХ ЦЕНТРІВ (РІВЕНЬ 7)
-- =====================================================

WITH oblast_mapping AS (
    -- Мапінг обласних центрів до областей
    SELECT 'Вінниця' as city_name, 'Вінницька область' as oblast_name
    UNION ALL SELECT 'Луцьк', 'Волинська область'
    UNION ALL SELECT 'Дніпро', 'Дніпропетровська область'
    UNION ALL SELECT 'Донецьк', 'Донецька область'
    UNION ALL SELECT 'Житомир', 'Житомирська область'
    UNION ALL SELECT 'Ужгород', 'Закарпатська область'
    UNION ALL SELECT 'Запоріжжя', 'Запорізька область'
    UNION ALL SELECT 'Івано-Франківськ', 'Івано-Франківська область'
    UNION ALL SELECT 'Київ', 'Київська область'
    UNION ALL SELECT 'Кропивницький', 'Кіровоградська область'
    UNION ALL SELECT 'Луганськ', 'Луганська область'
    UNION ALL SELECT 'Львів', 'Львівська область'
    UNION ALL SELECT 'Миколаїв', 'Миколаївська область'
    UNION ALL SELECT 'Одеса', 'Одеська область'
    UNION ALL SELECT 'Полтава', 'Полтавська область'
    UNION ALL SELECT 'Рівне', 'Рівненська область'
    UNION ALL SELECT 'Суми', 'Сумська область'
    UNION ALL SELECT 'Тернопіль', 'Тернопільська область'
    UNION ALL SELECT 'Харків', 'Харківська область'
    UNION ALL SELECT 'Херсон', 'Херсонська область'
    UNION ALL SELECT 'Хмельницький', 'Хмельницька область'
    UNION ALL SELECT 'Черкаси', 'Черкаська область'
    UNION ALL SELECT 'Чернівці', 'Чернівецька область'
    UNION ALL SELECT 'Чернігів', 'Чернігівська область'
    UNION ALL SELECT 'Сімферополь', 'Автономна Республіка Крим'
),
base_data AS (
    -- Знаходимо геометрії для обласних центрів
    SELECT
        COALESCE((r.tags->>'tags')::jsonb->>'name:uk', (r.tags->>'tags')::jsonb->>'name') as city_name,
        (r.tags->>'tags')::jsonb->>'name:uk' as name_uk,
        (r.tags->>'tags')::jsonb->>'name' as name,
        (r.tags->>'tags')::jsonb->>'name:en' as name_en,
        (r.tags->>'tags')::jsonb->>'population' as population,
        ST_GeometryType(r.geom) as geom_type,
        r.osm_id,
        r.tags,
        r.geom,
        CASE 
            WHEN ST_GeometryType(r.geom) LIKE '%Polygon%' AND (r.tags->>'tags')::jsonb->>'population' IS NOT NULL THEN 1
            WHEN ST_GeometryType(r.geom) LIKE '%Polygon%' THEN 2
            WHEN ST_GeometryType(r.geom) = 'ST_Point' AND (r.tags->>'tags')::jsonb->>'population' IS NOT NULL THEN 3
            ELSE 4
        END as priority
    FROM osm_ukraine.osm_raw r
    WHERE COALESCE((r.tags->>'tags')::jsonb->>'name:uk', (r.tags->>'tags')::jsonb->>'name') IN (
        SELECT city_name FROM oblast_mapping
    )
    AND (
        (r.tags->>'tags')::jsonb->>'place' IN ('city', 'town')
        OR (r.tags->>'tags')::jsonb->>'admin_level' IN ('4', '6', '7', '8')
    )
),
best_geometries AS (
    -- Вибираємо найкращу геометрію для кожного міста
    SELECT DISTINCT ON (city_name)
        osm_id,
        city_name,
        name_uk,
        name,
        name_en,
        population,
        geom_type,
        geom,
        tags
    FROM base_data
    ORDER BY city_name, priority, COALESCE(population::INTEGER, 0) DESC
)
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
    bg.osm_id,
    7,
    'administrative',
    bg.city_name,
    bg.name_uk,
    bg.name_en,
    o.id,
    o.name_uk,
    CASE 
        WHEN ST_GeometryType(bg.geom) = 'ST_Polygon' THEN ST_Multi(bg.geom)
        WHEN ST_GeometryType(bg.geom) = 'ST_MultiPolygon' THEN bg.geom
        ELSE ST_Buffer(bg.geom::geography, 1000)::geometry
    END,
    CASE 
        WHEN ST_GeometryType(bg.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
        THEN ROUND((ST_Area(bg.geom::geography) / 1000000.0)::numeric, 2)
        ELSE NULL
    END,
    CASE 
        WHEN bg.population ~ '^\d+$' 
        THEN bg.population::INTEGER 
        ELSE NULL 
    END,
    (bg.tags->>'tags')::jsonb - '{name,name:uk,name:en,place,population,admin_level}'::text[]
FROM best_geometries bg
JOIN oblast_mapping om ON bg.city_name = om.city_name
JOIN osm_ukraine.admin_boundaries o ON o.name_uk = om.oblast_name AND o.admin_level = 4
ON CONFLICT (osm_id) DO UPDATE SET
    admin_level = 7,
    parent_id = EXCLUDED.parent_id,
    region_name = EXCLUDED.region_name,
    updated_at = CURRENT_TIMESTAMP;

-- Додаємо Київ як місто-область (якщо він є на рівні 4)
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
    osm_id * 10 + 7,
    7,
    'administrative',
    name,
    name_uk,
    name_en,
    id,
    name_uk,
    geometry,
    area_km2,
    population,
    additional_tags
FROM osm_ukraine.admin_boundaries 
WHERE name_uk = 'Київ' AND admin_level = 4
ON CONFLICT (osm_id) DO NOTHING;

-- КРОК 4: ФІНАЛЬНА СТАТИСТИКА
-- =====================================================

-- Структура адміністративних рівнів
SELECT 
    'Фінальна структура' as info,
    admin_level as "Рівень",
    COUNT(*) as "Кількість",
    CASE 
        WHEN admin_level = 4 THEN 'Області'
        WHEN admin_level = 6 THEN 'Райони'
        WHEN admin_level = 7 THEN 'Обласні центри'
        WHEN admin_level = 8 THEN 'Районні центри (буде додано)'
        WHEN admin_level = 9 THEN 'Інші населені пункти'
    END as "Опис"
FROM osm_ukraine.admin_boundaries
GROUP BY admin_level
ORDER BY admin_level;

-- Перевірка обласних центрів
SELECT 
    ab.name_uk as "Обласний центр",
    p.name_uk as "Область",
    ab.population as "Населення",
    ST_GeometryType(ab.geometry) as "Тип геометрії"
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 7
ORDER BY ab.name_uk;

-- Логування завершення
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('reorganize_admin_levels', 'completed', 'Реорганізація завершена. Таблиця h3_admin_mapping очищена.');