-- =====================================================
-- Завантаження населених пунктів admin_level=8
-- Файл: sql/11_populate_settlements_level8.sql
-- =====================================================

-- 1. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('populate_settlements_8', 'started', 'Початок завантаження населених пунктів (admin_level=8)');

-- 2. Завантаження населених пунктів рівня 8
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
SELECT DISTINCT ON (s.osm_id)
    s.osm_id,
    8 as admin_level,
    'administrative' as boundary_type,
    COALESCE((s.tags->>'tags')::jsonb->>'name:uk', (s.tags->>'tags')::jsonb->>'name') as name,
    (s.tags->>'tags')::jsonb->>'name:uk' as name_uk,
    (s.tags->>'tags')::jsonb->>'name:en' as name_en,
    -- Визначаємо батьківський елемент (пріоритети: рівень 7 → 6 → 4)
    COALESCE(
        -- Спочатку шукаємо в територіальних громадах (рівень 7)
        (SELECT t.id 
         FROM osm_ukraine.admin_boundaries t 
         WHERE t.admin_level = 7 
         AND ST_Within(
            CASE 
                WHEN ST_GeometryType(s.geom) = 'ST_Polygon' THEN ST_Multi(s.geom)
                ELSE s.geom
            END, 
            t.geometry
         )
         LIMIT 1),
        -- Потім в районах (рівень 6)
        (SELECT r.id 
         FROM osm_ukraine.admin_boundaries r 
         WHERE r.admin_level = 6 
         AND ST_Within(
            CASE 
                WHEN ST_GeometryType(s.geom) = 'ST_Polygon' THEN ST_Multi(s.geom)
                ELSE s.geom
            END, 
            r.geometry
         )
         LIMIT 1),
        -- Якщо нічого не знайшли - в областях (рівень 4)
        (SELECT o.id 
         FROM osm_ukraine.admin_boundaries o 
         WHERE o.admin_level = 4 
         AND ST_Within(
            CASE 
                WHEN ST_GeometryType(s.geom) = 'ST_Polygon' THEN ST_Multi(s.geom)
                ELSE s.geom
            END, 
            o.geometry
         )
         LIMIT 1)
    ) as parent_id,
    s.region_name,
    CASE 
        WHEN ST_GeometryType(s.geom) = 'ST_Polygon' 
        THEN ST_Multi(s.geom)
        ELSE s.geom
    END as geometry,
    ROUND((ST_Area(s.geom::geography) / 1000000.0)::numeric, 2) as area_km2,
    CASE 
        WHEN (s.tags->>'tags')::jsonb->>'population' ~ '^\d+$' 
        THEN ((s.tags->>'tags')::jsonb->>'population')::INTEGER 
        ELSE NULL 
    END as population,
    (s.tags->>'tags')::jsonb - '{name,name:uk,name:en,boundary,admin_level,population}'::text[] as additional_tags
FROM osm_ukraine.osm_raw s
WHERE 
    s.tags->>'tags' IS NOT NULL
    AND (s.tags->>'tags')::jsonb->>'boundary' = 'administrative'
    AND (s.tags->>'tags')::jsonb->>'admin_level' = '8'
    AND ST_GeometryType(s.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
    -- Тільки ті що потрапляють в українські адмінодиниці
    AND EXISTS (
        SELECT 1 
        FROM osm_ukraine.admin_boundaries parent
        WHERE parent.admin_level IN (4, 6, 7)
        AND ST_Within(
            CASE 
                WHEN ST_GeometryType(s.geom) = 'ST_Polygon' THEN ST_Multi(s.geom)
                ELSE s.geom
            END, 
            parent.geometry
        )
    )
ORDER BY s.osm_id;

-- 3. Статистика завантаження
WITH stats AS (
    SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT s.parent_id) as unique_parents,
        SUM(CASE WHEN p.admin_level = 7 THEN 1 ELSE 0 END) as in_communities,
        SUM(CASE WHEN p.admin_level = 6 THEN 1 ELSE 0 END) as in_raions,
        SUM(CASE WHEN p.admin_level = 4 THEN 1 ELSE 0 END) as in_oblasts
    FROM osm_ukraine.admin_boundaries s
    LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
    WHERE s.admin_level = 8
)
SELECT 
    'Статистика admin_level=8' as info,
    total_count as "Всього",
    in_communities as "В громадах",
    in_raions as "В районах",
    in_oblasts as "В областях",
    unique_parents as "Унікальних батьків"
FROM stats;

-- 4. Перевірка великих міст на рівні 8
SELECT 
    s.name_uk as "Місто",
    p.name_uk as "Батьківська одиниця",
    p.admin_level as "Рівень батька",
    ROUND(s.area_km2::numeric, 2) as "Площа км²",
    s.population as "Населення"
FROM osm_ukraine.admin_boundaries s
LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
WHERE s.admin_level = 8
    AND (
        s.name_uk IN (
            'Київ', 'Харків', 'Одеса', 'Дніпро', 'Донецьк', 'Запоріжжя',
            'Львів', 'Кривий Ріг', 'Миколаїв', 'Маріуполь', 'Луганськ',
            'Вінниця', 'Севастополь', 'Сімферополь', 'Херсон', 'Полтава',
            'Чернігів', 'Черкаси', 'Суми', 'Житомир', 'Хмельницький'
        )
        OR s.name IN (
            'Київ', 'Харків', 'Одеса', 'Дніпро', 'Донецьк', 'Запоріжжя',
            'Львів', 'Кривий Ріг', 'Миколаїв', 'Маріуполь', 'Луганськ',
            'Вінниця', 'Севастополь', 'Сімферополь', 'Херсон', 'Полтава',
            'Чернігів', 'Черкаси', 'Суми', 'Житомир', 'Хмельницький'
        )
    )
ORDER BY s.population DESC NULLS LAST;

-- 5. Топ-20 населених пунктів рівня 8 за площею
SELECT 
    s.name_uk as "Населений пункт",
    p.name_uk as "Батьківська одиниця",
    p.admin_level as "Рівень батька",
    ROUND(s.area_km2::numeric, 2) as "Площа км²"
FROM osm_ukraine.admin_boundaries s
LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
WHERE s.admin_level = 8
ORDER BY s.area_km2 DESC NULLS LAST
LIMIT 20;

-- 6. Логування результату
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'populate_settlements_8',
    'completed', 
    'Завантажено населених пунктів рівня 8: ' || COUNT(*),
    jsonb_build_object('count', COUNT(*))
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 8;