-- ============================================================
-- ЗАПОВНЕННЯ ПОЛІВ ДЛЯ КОНКУРЕНТІВ
-- ============================================================

-- 1. СТВОРЕННЯ GEOMETRY
-- ------------------------------------------------------------
UPDATE avrora.competitors 
SET geometry = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
WHERE geometry IS NULL 
AND lat IS NOT NULL 
AND lon IS NOT NULL;

-- Перевірка
SELECT COUNT(*) as competitors_with_geometry 
FROM avrora.competitors 
WHERE geometry IS NOT NULL;

-- 2. ДОДАВАННЯ H3 КОЛОНОК (якщо їх немає)
-- ------------------------------------------------------------
ALTER TABLE avrora.competitors 
ADD COLUMN IF NOT EXISTS h3_7 text,
ADD COLUMN IF NOT EXISTS h3_8 text,
ADD COLUMN IF NOT EXISTS h3_9 text,
ADD COLUMN IF NOT EXISTS h3_10 text;

-- 3. ЗАПОВНЕННЯ H3 ІНДЕКСІВ
-- ------------------------------------------------------------
UPDATE avrora.competitors 
SET 
    h3_7 = h3_lat_lng_to_cell(geometry, 7)::text,
    h3_8 = h3_lat_lng_to_cell(geometry, 8)::text,
    h3_9 = h3_lat_lng_to_cell(geometry, 9)::text,
    h3_10 = h3_lat_lng_to_cell(geometry, 10)::text
WHERE geometry IS NOT NULL
AND h3_7 IS NULL;

-- Перевірка
SELECT COUNT(*) as with_h3 FROM avrora.competitors WHERE h3_10 IS NOT NULL;

-- 4. АДМІНПОДІЛ ЧЕРЕЗ H3_10 MAPPING
-- ------------------------------------------------------------
UPDATE avrora.competitors c
SET 
    oblast_id = h.oblast_id,
    oblast_name = h.oblast_name,
    raion_id = h.raion_id,
    raion_name = h.raion_name,
    gromada_id = h.gromada_id,
    gromada_name = h.gromada_name,
    settlement_id = h.settlement_id,
    settlement_name = h.settlement_name
FROM osm_ukraine.h3_admin_mapping h
WHERE c.h3_10 = h.h3_index
AND h.h3_resolution = 10
AND c.oblast_id IS NULL;

-- 5. ДОДАТКОВО ЧЕРЕЗ SPATIAL JOIN (для тих що не знайшлись через H3)
-- ------------------------------------------------------------
UPDATE avrora.competitors c
SET 
    oblast_id = ab.id,
    oblast_name = ab.name
FROM osm_ukraine.admin_boundaries ab
WHERE ab.admin_level = 4  -- область
AND ST_Contains(ab.geometry, c.geometry)
AND c.oblast_id IS NULL;



-- 7. ФІНАЛЬНА СТАТИСТИКА
-- ------------------------------------------------------------
SELECT 
    'Competitors Statistics' as info,
    COUNT(*) as total,
    COUNT(geometry) as with_geometry,
    COUNT(h3_7) as with_h3_7,
    COUNT(h3_10) as with_h3_10,
    COUNT(oblast_id) as with_oblast,
    COUNT(raion_id) as with_raion,
    COUNT(gromada_id) as with_gromada,
    COUNT(settlement_id) as with_settlement,
    COUNT(settlement_name) as with_settlement_name
FROM avrora.competitors;

-- 8. ПРИКЛАД ЗАПОВНЕНИХ ДАНИХ
-- ------------------------------------------------------------
SELECT 
    competitor_id,
    name,
    brand,
    city,
    oblast_name,
    raion_name,
    gromada_name,
    h3_10
FROM avrora.competitors
WHERE oblast_id IS NOT NULL
LIMIT 10;

-- 9. ПЕРЕВІРКА ЩО НЕ ЗАПОВНИЛОСЬ
-- ------------------------------------------------------------
SELECT 
    brand,
    COUNT(*) as count_without_oblast
FROM avrora.competitors
WHERE oblast_id IS NULL
GROUP BY brand
ORDER BY count_without_oblast DESC;