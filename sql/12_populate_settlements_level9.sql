-- =====================================================
-- Завантаження населених пунктів admin_level=9
-- Файл: sql/12_populate_settlements_level9.sql
-- =====================================================

-- 1. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('populate_settlements_9', 'started', 'Початок завантаження населених пунктів (admin_level=9)');

-- 2. Завантаження населених пунктів рівня 9
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
    9 as admin_level,
    'administrative' as boundary_type,
    COALESCE((s.tags->>'tags')::jsonb->>'name:uk', (s.tags->>'tags')::jsonb->>'name') as name,
    (s.tags->>'tags')::jsonb->>'name:uk' as name_uk,
    (s.tags->>'tags')::jsonb->>'name:en' as name_en,
    -- Визначаємо батьківський елемент (пріоритети: 8 → 7 → 6 → 4)
    COALESCE(
        -- Спочатку шукаємо в населених пунктах рівня 8
        (SELECT t.id 
         FROM osm_ukraine.admin_boundaries t 
         WHERE t.admin_level = 8 
         AND ST_Within(
            CASE 
                WHEN ST_GeometryType(s.geom) = 'ST_Polygon' THEN ST_Multi(s.geom)
                ELSE s.geom
            END, 
            t.geometry
         )
         LIMIT 1),
        -- Потім в територіальних громадах (рівень 7)
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
        -- В крайньому випадку - в областях (рівень 4)
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
    AND (s.tags->>'tags')::jsonb->>'admin_level' = '9'
    AND ST_GeometryType(s.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
    -- Тільки ті що потрапляють в українські адмінодиниці
    AND EXISTS (
        SELECT 1 
        FROM osm_ukraine.admin_boundaries parent
        WHERE parent.admin_level IN (4, 6, 7, 8)
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
        SUM(CASE WHEN p.admin_level = 8 THEN 1 ELSE 0 END) as in_level8,
        SUM(CASE WHEN p.admin_level = 7 THEN 1 ELSE 0 END) as in_communities,
        SUM(CASE WHEN p.admin_level = 6 THEN 1 ELSE 0 END) as in_raions,
        SUM(CASE WHEN p.admin_level = 4 THEN 1 ELSE 0 END) as in_oblasts
    FROM osm_ukraine.admin_boundaries s
    LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
    WHERE s.admin_level = 9
)
SELECT 
    'Статистика admin_level=9' as info,
    total_count as "Всього",
    in_level8 as "В рівні 8",
    in_communities as "В громадах",
    in_raions as "В районах",
    in_oblasts as "В областях"
FROM stats;

-- 4. Перевірка великих міст на рівні 9
SELECT 
    s.name_uk as "Місто",
    s.name as "Назва",
    p.name_uk as "Батьківська одиниця",
    p.admin_level as "Рівень батька",
    ROUND(s.area_km2::numeric, 2) as "Площа км²",
    s.population as "Населення"
FROM osm_ukraine.admin_boundaries s
LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
WHERE s.admin_level = 9
    AND (
        s.name_uk ILIKE '%київ%' OR s.name ILIKE '%київ%' OR s.name ILIKE '%kyiv%'
        OR s.name_uk ILIKE '%харків%' OR s.name ILIKE '%харків%' OR s.name ILIKE '%kharkiv%'
        OR s.name_uk ILIKE '%одес%' OR s.name ILIKE '%одес%' OR s.name ILIKE '%odesa%'
        OR s.name_uk ILIKE '%дніпр%' OR s.name ILIKE '%дніпр%' OR s.name ILIKE '%dnipr%'
        OR s.name_uk ILIKE '%львів%' OR s.name ILIKE '%львів%' OR s.name ILIKE '%lviv%'
        OR s.name_uk ILIKE '%запоріж%' OR s.name ILIKE '%запоріж%'
        OR s.name_uk ILIKE '%кривий ріг%' OR s.name ILIKE '%кривий ріг%'
        OR s.name_uk ILIKE '%миколаїв%' OR s.name ILIKE '%миколаїв%'
        OR s.name_uk ILIKE '%вінниц%' OR s.name ILIKE '%вінниц%'
        OR s.name_uk ILIKE '%полтав%' OR s.name ILIKE '%полтав%'
    )
ORDER BY s.area_km2 DESC NULLS LAST
LIMIT 30;

-- 5. Загальна статистика всіх рівнів
SELECT 
    admin_level as "Адмін. рівень",
    COUNT(*) as "Кількість",
    MIN(area_km2) as "Мін. площа",
    MAX(area_km2) as "Макс. площа",
    ROUND(AVG(area_km2)::numeric, 2) as "Сер. площа"
FROM osm_ukraine.admin_boundaries
GROUP BY admin_level
ORDER BY admin_level;

-- 6. Логування результату
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'populate_settlements_9',
    'completed', 
    'Завантажено населених пунктів рівня 9: ' || COUNT(*),
    jsonb_build_object('count', COUNT(*))
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 9;