-- =====================================================
-- Виправлене завантаження районних центрів на рівень 8
-- Файл: sql/24_load_raion_centers_level8_fixed.sql
-- =====================================================

-- 1. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('load_raion_centers_fixed', 'started', 'Початок виправленого завантаження районних центрів на рівень 8');

-- 2. Створюємо розширений мапінг районних центрів з альтернативними назвами
WITH raion_center_mapping AS (
    SELECT city_name, alt_name, oblast_name FROM (
        VALUES
            -- Вінницька область
            ('Гайсин', NULL, 'Вінницька область'), 
            ('Жмеринка', NULL, 'Вінницька область'), 
            ('Могилів-Подільський', NULL, 'Вінницька область'), 
            ('Тульчин', NULL, 'Вінницька область'), 
            ('Хмільник', NULL, 'Вінницька область'),
            
            -- Волинська область
            ('Володимир', NULL, 'Волинська область'), 
            ('Камінь-Каширський', NULL, 'Волинська область'), 
            ('Ковель', NULL, 'Волинська область'),
            
            -- Дніпропетровська область - з альтернативною назвою
            ('Кам''янське', 'Дніпродзержинськ', 'Дніпропетровська область'), 
            ('Кривий Ріг', NULL, 'Дніпропетровська область'), 
            ('Нікополь', NULL, 'Дніпропетровська область'), 
            ('Павлоград', NULL, 'Дніпропетровська область'), 
            ('Самар', NULL, 'Дніпропетровська область'), 
            ('Синельникове', NULL, 'Дніпропетровська область'),
            
            -- Донецька область - з альтернативними назвами
            ('Бахмут', 'Артемівськ', 'Донецька область'), 
            ('Волноваха', NULL, 'Донецька область'), 
            ('Горлівка', NULL, 'Донецька область'), 
            ('Кальміуське', 'Кальміуське', 'Донецька область'), 
            ('Краматорськ', NULL, 'Донецька область'), 
            ('Маріуполь', NULL, 'Донецька область'), 
            ('Покровськ', NULL, 'Донецька область'),
            
            -- Житомирська область - з альтернативною назвою
            ('Бердичів', NULL, 'Житомирська область'), 
            ('Звягель', 'Новоград-Волинський', 'Житомирська область'), 
            ('Коростень', NULL, 'Житомирська область'),
            
            -- Закарпатська область
            ('Берегове', NULL, 'Закарпатська область'), 
            ('Мукачево', NULL, 'Закарпатська область'), 
            ('Рахів', NULL, 'Закарпатська область'), 
            ('Тячів', NULL, 'Закарпатська область'), 
            ('Хуст', NULL, 'Закарпатська область'),
            
            -- Запорізька область
            ('Бердянськ', NULL, 'Запорізька область'), 
            ('Василівка', NULL, 'Запорізька область'), 
            ('Мелітополь', NULL, 'Запорізька область'), 
            ('Пологи', NULL, 'Запорізька область'),
            
            -- Івано-Франківська область
            ('Верховина', NULL, 'Івано-Франківська область'), 
            ('Калуш', NULL, 'Івано-Франківська область'), 
            ('Коломия', NULL, 'Івано-Франківська область'), 
            ('Косів', NULL, 'Івано-Франківська область'), 
            ('Надвірна', NULL, 'Івано-Франківська область'),
            
            -- Київська область
            ('Біла Церква', NULL, 'Київська область'), 
            ('Бориспіль', NULL, 'Київська область'), 
            ('Бровари', NULL, 'Київська область'), 
            ('Буча', NULL, 'Київська область'), 
            ('Вишгород', NULL, 'Київська область'), 
            ('Обухів', NULL, 'Київська область'), 
            ('Фастів', NULL, 'Київська область'),
            
            -- Кіровоградська область
            ('Голованівськ', NULL, 'Кіровоградська область'), 
            ('Новоукраїнка', NULL, 'Кіровоградська область'), 
            ('Олександрія', NULL, 'Кіровоградська область'),
            
            -- Луганська область - з альтернативною назвою
            ('Алчевськ', NULL, 'Луганська область'), 
            ('Довжанськ', 'Свердловськ', 'Луганська область'), 
            ('Ровеньки', NULL, 'Луганська область'), 
            ('Сватове', NULL, 'Луганська область'), 
            ('Сіверськодонецьк', NULL, 'Луганська область'), 
            ('Старобільськ', NULL, 'Луганська область'), 
            ('Щастя', NULL, 'Луганська область'),
            
            -- Львівська область
            ('Дрогобич', NULL, 'Львівська область'), 
            ('Золочів', NULL, 'Львівська область'), 
            ('Самбір', NULL, 'Львівська область'), 
            ('Стрий', NULL, 'Львівська область'), 
            ('Шептицький', NULL, 'Львівська область'), 
            ('Яворів', NULL, 'Львівська область'),
            
            -- Миколаївська область
            ('Баштанка', NULL, 'Миколаївська область'), 
            ('Вознесенськ', NULL, 'Миколаївська область'), 
            ('Первомайськ', NULL, 'Миколаївська область'),
            
            -- Одеська область
            ('Березівка', NULL, 'Одеська область'), 
            ('Білгород-Дністровський', NULL, 'Одеська область'), 
            ('Болград', NULL, 'Одеська область'), 
            ('Ізмаїл', NULL, 'Одеська область'), 
            ('Подільськ', NULL, 'Одеська область'), 
            ('Роздільна', NULL, 'Одеська область'),
            
            -- Полтавська область
            ('Кременчук', NULL, 'Полтавська область'), 
            ('Лубни', NULL, 'Полтавська область'), 
            ('Миргород', NULL, 'Полтавська область'),
            
            -- Рівненська область
            ('Вараш', NULL, 'Рівненська область'), 
            ('Дубно', NULL, 'Рівненська область'), 
            ('Сарни', NULL, 'Рівненська область'),
            
            -- Сумська область
            ('Конотоп', NULL, 'Сумська область'), 
            ('Охтирка', NULL, 'Сумська область'), 
            ('Ромни', NULL, 'Сумська область'), 
            ('Шостка', NULL, 'Сумська область'),
            
            -- Тернопільська область
            ('Кременець', NULL, 'Тернопільська область'), 
            ('Чортків', NULL, 'Тернопільська область'),
            
            -- Харківська область
            ('Берестин', NULL, 'Харківська область'), 
            ('Богодухів', NULL, 'Харківська область'), 
            ('Ізюм', NULL, 'Харківська область'), 
            ('Куп''янськ', NULL, 'Харківська область'), 
            ('Лозова', NULL, 'Харківська область'), 
            ('Чугуїв', NULL, 'Харківська область'),
            
            -- Херсонська область
            ('Берислав', NULL, 'Херсонська область'), 
            ('Генічеськ', NULL, 'Херсонська область'), 
            ('Нова Каховка', NULL, 'Херсонська область'), 
            ('Скадовськ', NULL, 'Херсонська область'),
            
            -- Хмельницька область
            ('Кам''янець-Подільський', NULL, 'Хмельницька область'), 
            ('Шепетівка', NULL, 'Хмельницька область'),
            
            -- Черкаська область
            ('Звенигородка', NULL, 'Черкаська область'), 
            ('Золотоноша', NULL, 'Черкаська область'), 
            ('Умань', NULL, 'Черкаська область'),
            
            -- Чернівецька область
            ('Вижниця', NULL, 'Чернівецька область'), 
            ('Кельменці', NULL, 'Чернівецька область'),
            
            -- Чернігівська область
            ('Корюківка', NULL, 'Чернігівська область'), 
            ('Ніжин', NULL, 'Чернігівська область'), 
            ('Новгород-Сіверський', NULL, 'Чернігівська область'), 
            ('Прилуки', NULL, 'Чернігівська область'),
            
            -- Республіка Крим - з альтернативними назвами
            ('Бахчисарай', 'Бахчисарай', 'Республіка Крим'), 
            ('Білогірськ', 'Белогорск', 'Республіка Крим'), 
            ('Джанкой', 'Джанкой', 'Республіка Крим'), 
            ('Євпаторія', 'Евпатория', 'Республіка Крим'), 
            ('Керч', 'Керчь', 'Республіка Крим'), 
            ('Курман', 'Армянск', 'Республіка Крим'), 
            ('Яни Капу', 'Красноперекопск', 'Республіка Крим'), 
            ('Феодосія', 'Феодосия', 'Республіка Крим'), 
            ('Ялта', 'Ялта', 'Республіка Крим')
    ) AS t(city_name, alt_name, oblast_name)
),

-- 3. Функція для нормалізації назв (заміна типографських апострофів)
normalized_search AS (
    SELECT DISTINCT
        rcm.*,
        o.id as oblast_id,
        o.geometry as oblast_geom
    FROM raion_center_mapping rcm
    JOIN osm_ukraine.admin_boundaries o 
        ON o.name_uk = rcm.oblast_name 
        AND o.admin_level = 4
),

-- 4. Знаходимо найкращі геометрії для районних центрів
raion_center_geometries AS (
    SELECT DISTINCT ON (ns.city_name, ns.oblast_name)
        r.osm_id,
        ns.city_name,
        COALESCE((r.tags->>'tags')::jsonb->>'name:uk', (r.tags->>'tags')::jsonb->>'name') as name_uk,
        (r.tags->>'tags')::jsonb->>'name:en' as name_en,
        (r.tags->>'tags')::jsonb->>'population' as population,
        (r.tags->>'tags')::jsonb->>'place' as place,
        r.tags,
        r.geom,
        ST_GeometryType(r.geom) as geom_type,
        ns.oblast_id,
        ns.oblast_name,
        -- Пріоритет геометрії
        CASE 
            WHEN ST_GeometryType(r.geom) LIKE '%Polygon%' AND (r.tags->>'tags')::jsonb->>'place' = 'city' THEN 1
            WHEN ST_GeometryType(r.geom) LIKE '%Polygon%' AND (r.tags->>'tags')::jsonb->>'population' IS NOT NULL THEN 2
            WHEN ST_GeometryType(r.geom) LIKE '%Polygon%' THEN 3
            WHEN ST_GeometryType(r.geom) = 'ST_Point' AND (r.tags->>'tags')::jsonb->>'place' = 'city' THEN 4
            WHEN ST_GeometryType(r.geom) = 'ST_Point' AND (r.tags->>'tags')::jsonb->>'population' IS NOT NULL THEN 5
            ELSE 6
        END as priority
    FROM normalized_search ns
    JOIN osm_ukraine.osm_raw r ON (
        -- Нормалізуємо назви для порівняння (заміна типографських апострофів)
        LOWER(REPLACE(REPLACE(COALESCE((r.tags->>'tags')::jsonb->>'name:uk', (r.tags->>'tags')::jsonb->>'name'), chr(8217), chr(39)), ''', '''')) 
            = LOWER(REPLACE(REPLACE(ns.city_name, chr(8217), chr(39)), ''', ''''))
        -- Пошук за альтернативною назвою якщо вона є
        OR (ns.alt_name IS NOT NULL AND 
            LOWER(COALESCE((r.tags->>'tags')::jsonb->>'name:uk', (r.tags->>'tags')::jsonb->>'name')) = LOWER(ns.alt_name))
        -- Пошук за старою назвою
        OR (ns.alt_name IS NOT NULL AND 
            LOWER((r.tags->>'tags')::jsonb->>'old_name') = LOWER(ns.alt_name))
        OR (ns.alt_name IS NOT NULL AND 
            LOWER((r.tags->>'tags')::jsonb->>'old_name:uk') = LOWER(ns.alt_name))
    )
    -- Перевірка що населений пункт в межах правильної області  
    AND ST_Contains(ns.oblast_geom, ST_Centroid(r.geom))
    WHERE (
        (r.tags->>'tags')::jsonb->>'place' IN ('city', 'town', 'village')
        OR (r.tags->>'tags')::jsonb->>'admin_level' IN ('6', '7', '8', '9')
    )
    ORDER BY ns.city_name, ns.oblast_name, priority, COALESCE((r.tags->>'tags')::jsonb->>'population', '0')::INTEGER DESC
),

-- 5. Визначаємо район для кожного районного центру
raion_centers_with_raion AS (
    SELECT 
        rcg.*,
        -- Знаходимо район в який потрапляє центр
        raion.id as raion_id,
        raion.name_uk as raion_name
    FROM raion_center_geometries rcg
    LEFT JOIN LATERAL (
        SELECT id, name_uk
        FROM osm_ukraine.admin_boundaries
        WHERE admin_level = 6
            AND parent_id = rcg.oblast_id
            AND ST_Contains(geometry, ST_Centroid(rcg.geom))
        LIMIT 1
    ) raion ON true
)

-- 6. Вставка/оновлення районних центрів
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
    8,
    'administrative',
    city_name,
    name_uk,
    name_en,
    COALESCE(raion_id, oblast_id),  -- Якщо район не знайдено, прив'язуємо до області
    oblast_name,
    CASE 
        WHEN ST_GeometryType(geom) = 'ST_Polygon' THEN ST_Multi(geom)
        WHEN ST_GeometryType(geom) = 'ST_MultiPolygon' THEN geom
        ELSE ST_Buffer(geom::geography, 1000)::geometry  -- Буфер 1км для точок
    END,
    CASE 
        WHEN ST_GeometryType(geom) IN ('ST_Polygon', 'ST_MultiPolygon')
        THEN ROUND((ST_Area(geom::geography) / 1000000.0)::numeric, 2)
        ELSE NULL
    END,
    CASE 
        WHEN population ~ '^\d+$' THEN population::INTEGER
        ELSE NULL
    END,
    (tags->>'tags')::jsonb - '{name,name:uk,name:en,place,population,admin_level}'::text[]
FROM raion_centers_with_raion
ON CONFLICT (osm_id) DO UPDATE SET
    admin_level = CASE 
        WHEN admin_boundaries.admin_level = 4 THEN admin_boundaries.admin_level  -- Не змінюємо обласні центри
        ELSE 8
    END,
    parent_id = CASE 
        WHEN admin_boundaries.admin_level = 4 THEN admin_boundaries.parent_id  -- Не змінюємо parent_id обласних центрів
        ELSE COALESCE(EXCLUDED.parent_id, admin_boundaries.parent_id)
    END,
    region_name = EXCLUDED.region_name,
    population = COALESCE(EXCLUDED.population, admin_boundaries.population),
    updated_at = CURRENT_TIMESTAMP;

-- 7. Логування завершення
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('load_raion_centers_fixed', 'completed', 'Завершено виправлене завантаження районних центрів');

-- 8. Статистика завантаження
WITH stats AS (
    SELECT 
        COUNT(*) as total,
        COUNT(CASE WHEN p.admin_level = 6 THEN 1 END) as with_raion,
        COUNT(CASE WHEN p.admin_level = 4 THEN 1 END) as with_oblast_only,
        COUNT(CASE WHEN rc.population IS NOT NULL THEN 1 END) as with_population
    FROM osm_ukraine.admin_boundaries rc
    LEFT JOIN osm_ukraine.admin_boundaries p ON rc.parent_id = p.id
    WHERE rc.admin_level = 8
)
SELECT 
    'Статистика районних центрів' as info,
    total as "Всього на рівні 8",
    with_raion as "Прив'язано до району",
    with_oblast_only as "Прив'язано тільки до області",
    with_population as "З даними про населення"
FROM stats;