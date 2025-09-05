-- =====================================================
-- Завантаження населених пунктів admin_level=7
-- Файл: sql/10_populate_settlements_level7.sql
-- =====================================================

-- 1. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('populate_settlements_7', 'started', 'Початок завантаження населених пунктів (admin_level=7)');

-- 2. Завантаження населених пунктів рівня 7 (міста, СМТ)
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
    7 as admin_level,
    'administrative' as boundary_type,
    COALESCE((s.tags->>'tags')::jsonb->>'name:uk', (s.tags->>'tags')::jsonb->>'name') as name,
    (s.tags->>'tags')::jsonb->>'name:uk' as name_uk,
    (s.tags->>'tags')::jsonb->>'name:en' as name_en,
    -- Визначаємо батьківський район
    COALESCE(
        -- Спочатку шукаємо район
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
        -- Якщо району немає, прив'язуємо до області
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
    AND (s.tags->>'tags')::jsonb->>'admin_level' = '7'
    AND ST_GeometryType(s.geom) IN ('ST_Polygon', 'ST_MultiPolygon')
    -- Тільки ті що потрапляють в українські області або райони
    AND EXISTS (
        SELECT 1 
        FROM osm_ukraine.admin_boundaries parent
        WHERE parent.admin_level IN (4, 6)
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
        SUM(CASE WHEN p.admin_level = 6 THEN 1 ELSE 0 END) as in_raions,
        SUM(CASE WHEN p.admin_level = 4 THEN 1 ELSE 0 END) as in_oblasts
    FROM osm_ukraine.admin_boundaries s
    LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
    WHERE s.admin_level = 7
)
SELECT 
    'Статистика admin_level=7' as info,
    total_count as "Всього",
    in_raions as "В районах",
    in_oblasts as "В областях напряму",
    unique_parents as "Унікальних батьків"
FROM stats;

-- 4. Топ-10 районів за кількістю населених пунктів рівня 7
SELECT 
    p.name_uk as "Район/Область",
    p.admin_level as "Рівень батька",
    COUNT(s.id) as "К-ть населених пунктів",
    STRING_AGG(s.name_uk, ', ' ORDER BY s.name_uk) as "Населені пункти"
FROM osm_ukraine.admin_boundaries s
JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
WHERE s.admin_level = 7
GROUP BY p.name_uk, p.admin_level
ORDER BY COUNT(s.id) DESC
LIMIT 10;

-- 5. Перевірка великих міст
SELECT 
    s.name_uk as "Місто",
    p.name_uk as "Район/Область",
    p.admin_level as "Рівень батька",
    ROUND(s.area_km2::numeric, 2) as "Площа км²",
    s.population as "Населення"
FROM osm_ukraine.admin_boundaries s
LEFT JOIN osm_ukraine.admin_boundaries p ON s.parent_id = p.id
WHERE s.admin_level = 7
    AND s.name_uk IN (
        'Київ', 'Харків', 'Одеса', 'Дніпро', 'Донецьк', 'Запоріжжя',
        'Львів', 'Кривий Ріг', 'Миколаїв', 'Маріуполь', 'Луганськ',
        'Вінниця', 'Севастополь', 'Сімферополь', 'Херсон', 'Полтава',
        'Чернігів', 'Черкаси', 'Суми', 'Житомир', 'Хмельницький'
    )
ORDER BY s.population DESC NULLS LAST;

-- 6. Логування результату
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'populate_settlements_7',
    'completed',
    'Завантажено населених пунктів рівня 7: ' || COUNT(*),
    jsonb_build_object('count', COUNT(*))
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 7;