-- ================================================================
-- Файл: 46_fix_kyiv_h3_mapping.sql
-- Мета: Додавання відсутнього мапінгу H3 гексагонів для міста Києва
-- Дата: 2025-01-09
-- 
-- Проблема: Київ геометрично є "діркою" в Київській області,
-- тому стандартні скрипти не обробляють його гексагони
-- 
-- Обробляє всі рівні H3: 7, 8, 9, 10
-- Всього ~44,160 гексагонів
-- ================================================================

-- 1. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('kyiv_h3_mapping_fix', 'started', 'Початок виправлення мапінгу H3 для Києва');

-- 2. Створюємо тимчасову таблицю з гексагонами Києва для всіх рівнів
DROP TABLE IF EXISTS temp_kyiv_hexagons;
CREATE TEMP TABLE temp_kyiv_hexagons AS
SELECT 
    h.h3_index,
    h.resolution,
    h.center_point,
    95 as kyiv_id,  -- ID Києва
    'Київ' as kyiv_name
FROM osm_ukraine.h3_grid h
CROSS JOIN osm_ukraine.admin_boundaries k
WHERE k.id = 95  -- Київ
    AND ST_Contains(k.geometry, h.center_point)
    AND h.resolution IN (7, 8, 9, 10);

-- Індекси для швидкості
CREATE INDEX idx_temp_kyiv_res ON temp_kyiv_hexagons(resolution);
CREATE INDEX idx_temp_kyiv_h3 ON temp_kyiv_hexagons(h3_index);

-- 3. Статистика по тимчасовій таблиці
DO $$
DECLARE
    stats_record RECORD;
    total_count INTEGER;
BEGIN
    -- Загальна кількість
    SELECT COUNT(*) INTO total_count FROM temp_kyiv_hexagons;
    
    -- Логування статистики
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    SELECT 
        'kyiv_h3_mapping_fix',
        'info',
        'Знайдено гексагонів Києва: ' || total_count,
        jsonb_object_agg(
            'h3_' || resolution::text, 
            count
        ) as details
    FROM (
        SELECT resolution, COUNT(*) as count
        FROM temp_kyiv_hexagons
        GROUP BY resolution
        ORDER BY resolution
    ) t;
END $$;

-- 4. Видаляємо існуючі записи для Києва (якщо є помилкові)
DELETE FROM osm_ukraine.h3_admin_mapping 
WHERE settlement_id = 95 
    AND h3_resolution IN (7, 8, 9, 10);

-- 5. Вставляємо правильний мапінг для всіх гексагонів Києва
DO $$
DECLARE
    resolution_rec RECORD;
    batch_count INTEGER;
    total_processed INTEGER := 0;
    start_time TIMESTAMP;
    resolution_time INTERVAL;
BEGIN
    start_time := clock_timestamp();
    
    -- Обробляємо по черзі кожен рівень H3
    FOR resolution_rec IN 
        SELECT DISTINCT resolution 
        FROM temp_kyiv_hexagons 
        ORDER BY resolution
    LOOP
        -- Вставляємо батч для поточного рівня
        INSERT INTO osm_ukraine.h3_admin_mapping (
            h3_index,
            h3_resolution,
            oblast_id,
            oblast_name,
            raion_id,
            raion_name,
            gromada_id,
            gromada_name,
            settlement_id,
            settlement_name,
            settlement_admin_level
        )
        SELECT 
            h3_index,
            resolution,
            96,                    -- Київська область (parent)
            'Київська область',    -- Назва області
            NULL,                  -- Київ не належить до району
            NULL,                  -- Без назви району
            NULL,                  -- Київ не належить до громади
            NULL,                  -- Без назви громади
            95,                    -- ID Києва
            'Київ',               -- Назва міста
            7                      -- Рівень 7 - обласний центр
        FROM temp_kyiv_hexagons
        WHERE resolution = resolution_rec.resolution
        ON CONFLICT (h3_index) DO UPDATE SET
            -- Оновлюємо якщо вже існує (наприклад, з помилковими даними)
            oblast_id = EXCLUDED.oblast_id,
            oblast_name = EXCLUDED.oblast_name,
            raion_id = EXCLUDED.raion_id,
            raion_name = EXCLUDED.raion_name,
            gromada_id = EXCLUDED.gromada_id,
            gromada_name = EXCLUDED.gromada_name,
            settlement_id = EXCLUDED.settlement_id,
            settlement_name = EXCLUDED.settlement_name,
            settlement_admin_level = EXCLUDED.settlement_admin_level;
        
        GET DIAGNOSTICS batch_count = ROW_COUNT;
        total_processed := total_processed + batch_count;
        resolution_time := clock_timestamp() - start_time;
        
        -- Логування прогресу
        INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
        VALUES (
            'kyiv_h3_mapping_fix',
            'progress',
            'Оброблено H3-' || resolution_rec.resolution,
            jsonb_build_object(
                'resolution', resolution_rec.resolution,
                'processed', batch_count,
                'total_processed', total_processed,
                'elapsed_seconds', EXTRACT(EPOCH FROM resolution_time)::int
            )
        );
        
        -- COMMIT після кожного рівня
        COMMIT;
    END LOOP;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES (
        'kyiv_h3_mapping_fix',
        'completed',
        'Завершено виправлення мапінгу для Києва',
        jsonb_build_object(
            'total_hexagons', total_processed,
            'total_time', (clock_timestamp() - start_time)::text,
            'hexagons_per_second', (total_processed / NULLIF(EXTRACT(EPOCH FROM (clock_timestamp() - start_time)), 0))::int
        )
    );
END $$;

-- 6. Очищення тимчасової таблиці
DROP TABLE IF EXISTS temp_kyiv_hexagons;

-- 7. Перевірка результатів
WITH kyiv_stats AS (
    SELECT 
        h3_resolution,
        COUNT(*) as count,
        MIN(h3_index) as sample_h3_index
    FROM osm_ukraine.h3_admin_mapping
    WHERE settlement_id = 95  -- Київ
    GROUP BY h3_resolution
),
expected AS (
    SELECT 
        unnest(ARRAY[7, 8, 9, 10]) as resolution,
        unnest(ARRAY[168, 1168, 7281, 35543]) as expected_count
)
SELECT 
    e.resolution as "H3 рівень",
    COALESCE(k.count, 0) as "Фактично в БД",
    e.expected_count as "Очікувалось",
    CASE 
        WHEN COALESCE(k.count, 0) = e.expected_count THEN '✅ OK'
        WHEN COALESCE(k.count, 0) > 0 THEN '⚠️ Розбіжність'
        ELSE '❌ Відсутні'
    END as "Статус",
    k.sample_h3_index as "Приклад H3 індексу"
FROM expected e
LEFT JOIN kyiv_stats k ON k.h3_resolution = e.resolution
ORDER BY e.resolution;

-- 8. Фінальна статистика по Києву
SELECT 
    'Фінальна статистика Києва' as metric,
    COUNT(DISTINCT h3_resolution) as "Рівнів H3",
    COUNT(*) as "Всього гексагонів",
    COUNT(DISTINCT oblast_id) as "Унікальних областей",
    STRING_AGG(DISTINCT oblast_name, ', ') as "Область",
    STRING_AGG(DISTINCT settlement_name, ', ') as "Населений пункт",
    MIN(settlement_admin_level) as "Адмін.рівень"
FROM osm_ukraine.h3_admin_mapping
WHERE settlement_id = 95;

-- 9. Порівняння з іншими обласними центрами
SELECT 
    settlement_name as "Обласний центр",
    COUNT(*) as "Гексагонів H3-7",
    RANK() OVER (ORDER BY COUNT(*) DESC) as "Ранг за розміром"
FROM osm_ukraine.h3_admin_mapping
WHERE h3_resolution = 7
    AND settlement_admin_level = 7
GROUP BY settlement_name
ORDER BY COUNT(*) DESC
LIMIT 10;