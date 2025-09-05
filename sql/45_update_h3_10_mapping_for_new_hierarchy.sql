-- ================================================================
-- Файл: 45_update_h3_10_mapping_for_new_hierarchy.sql
-- Мета: Оновлення прив'язки H3-10 гексагонів до нової ієрархії
-- Дата: 2025-01-04
-- 
-- H3-10: ~5,325,109 гексагонів (максимальна деталізація)
-- Орієнтовний час виконання: 30-60 хвилин
-- ================================================================

-- 1. Очищаємо таблицю для H3-10
DELETE FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 10;

-- 2. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('h3_10_mapping_v2', 'started', 'Початок прив''язки H3-10 до нової ієрархії (5,325,109 гексагонів)');

-- 3. Створюємо тимчасову таблицю для батчевої обробки
DROP TABLE IF EXISTS temp_h3_10_batch;
CREATE TEMP TABLE temp_h3_10_batch AS
SELECT 
    h.h3_index,
    h.resolution,
    h.center_point,
    o.id as oblast_id,
    o.name_uk as oblast_name
FROM osm_ukraine.h3_grid h
JOIN osm_ukraine.admin_boundaries o 
    ON o.admin_level = 4 
    AND ST_Contains(o.geometry, h.center_point)
WHERE h.resolution = 10;

-- Індекси для швидкості
CREATE INDEX idx_temp_h3_10_oblast ON temp_h3_10_batch(oblast_id);
CREATE INDEX idx_temp_h3_10_center ON temp_h3_10_batch USING GIST(center_point);
ANALYZE temp_h3_10_batch;

-- 4. Статистика по тимчасовій таблиці
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'h3_10_mapping_v2',
    'info',
    'Гексагонів H3-10 в межах України: ' || COUNT(*),
    jsonb_build_object(
        'total_in_ukraine', COUNT(*), 
        'unique_oblasts', COUNT(DISTINCT oblast_id),
        'avg_per_oblast', (COUNT(*) / NULLIF(COUNT(DISTINCT oblast_id), 0))::int,
        'max_per_oblast', MAX(cnt),
        'min_per_oblast', MIN(cnt)
    )
FROM (
    SELECT oblast_id, COUNT(*) as cnt
    FROM temp_h3_10_batch
    GROUP BY oblast_id
) t;

-- 5. Видаляємо стару процедуру якщо існує
DROP PROCEDURE IF EXISTS process_h3_10_mapping_v2();

-- 6. Створюємо процедуру для батчевої обробки
CREATE PROCEDURE process_h3_10_mapping_v2()
LANGUAGE plpgsql
AS $$
DECLARE
    oblast_rec RECORD;
    batch_size INTEGER := 0;
    total_processed INTEGER := 0;
    oblast_count INTEGER := 0;
    total_oblasts INTEGER := 26; -- Відомо заздалегідь
    start_time TIMESTAMP;
    oblast_start TIMESTAMP;
    oblast_time INTERVAL;
    total_time INTERVAL;
BEGIN
    start_time := clock_timestamp();
    
    -- Проходимо по кожній області
    FOR oblast_rec IN 
        SELECT DISTINCT oblast_id, oblast_name
        FROM temp_h3_10_batch
        ORDER BY oblast_name
    LOOP
        oblast_count := oblast_count + 1;
        oblast_start := clock_timestamp();
        
        -- Вставляємо гексагони з новою ієрархією
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
            t.h3_index,
            t.resolution,
            t.oblast_id,
            t.oblast_name,
            r.id as raion_id,
            r.name_uk as raion_name,
            g.id as gromada_id,
            g.name_uk as gromada_name,
            -- Пріоритет: села(9) → рай.центри(8) → обл.центри(7)
            COALESCE(s9.id, s8.id, s7.id) as settlement_id,
            COALESCE(s9.name_uk, s8.name_uk, s7.name_uk) as settlement_name,
            COALESCE(s9.admin_level, s8.admin_level, s7.admin_level) as settlement_admin_level
        FROM temp_h3_10_batch t
        
        -- Район (рівень 5, був 6)
        LEFT JOIN osm_ukraine.admin_boundaries r 
            ON r.admin_level = 5
            AND r.parent_id = t.oblast_id
            AND ST_Contains(r.geometry, t.center_point)
            
        -- Громада (рівень 6)
        LEFT JOIN osm_ukraine.admin_boundaries g
            ON g.admin_level = 6
            AND g.parent_id = r.id
            AND ST_Contains(g.geometry, t.center_point)
            
        -- Обласний центр (рівень 7)
        LEFT JOIN osm_ukraine.admin_boundaries s7
            ON s7.admin_level = 7
            AND s7.parent_id = t.oblast_id
            AND ST_Contains(s7.geometry, t.center_point)
            
        -- Районний центр (рівень 8)
        LEFT JOIN osm_ukraine.admin_boundaries s8
            ON s8.admin_level = 8
            AND s8.parent_id = r.id
            AND ST_Contains(s8.geometry, t.center_point)
            
        -- Село/селище (рівень 9)
        LEFT JOIN osm_ukraine.admin_boundaries s9
            ON s9.admin_level = 9
            AND (s9.parent_id = g.id OR 
                 (g.id IS NULL AND s9.parent_id = r.id)) -- для Криму
            AND ST_Contains(s9.geometry, t.center_point)
            
        WHERE t.oblast_id = oblast_rec.oblast_id
        ON CONFLICT (h3_index) DO NOTHING;
        
        GET DIAGNOSTICS batch_size = ROW_COUNT;
        total_processed := total_processed + batch_size;
        
        -- Час обробки області
        oblast_time := clock_timestamp() - oblast_start;
        total_time := clock_timestamp() - start_time;
        
        -- Логуємо після КОЖНОЇ області для H3-10
        INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
        VALUES (
            'h3_10_mapping_v2', 
            'progress', 
            'Оброблено: ' || oblast_rec.oblast_name || ' (' || oblast_count || ' з ' || total_oblasts || ')',
            jsonb_build_object(
                'oblast', oblast_rec.oblast_name,
                'hexagons_in_oblast', batch_size,
                'total_processed', total_processed,
                'oblast_time_sec', EXTRACT(EPOCH FROM oblast_time)::int,
                'total_elapsed', total_time::text,
                'avg_sec_per_oblast', (EXTRACT(EPOCH FROM total_time) / oblast_count)::int,
                'estimated_remaining_min', 
                    ((EXTRACT(EPOCH FROM total_time) / oblast_count) * (total_oblasts - oblast_count) / 60)::int,
                'progress_percent', ROUND(100.0 * oblast_count / total_oblasts, 1)
            )
        );
        
        -- COMMIT після кожної області (критично для H3-10!)
        COMMIT;
    END LOOP;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES (
        'h3_10_mapping_v2', 
        'completed', 
        'Завершено обробку H3-10',
        jsonb_build_object(
            'oblasts_processed', oblast_count,
            'total_hexagons', total_processed,
            'total_time', (clock_timestamp() - start_time)::text,
            'avg_hexagons_per_second', (total_processed / NULLIF(EXTRACT(EPOCH FROM (clock_timestamp() - start_time)), 0))::int,
            'avg_seconds_per_oblast', (EXTRACT(EPOCH FROM (clock_timestamp() - start_time)) / oblast_count)::int
        )
    );
    
    COMMIT;
END;
$$;

-- 7. Викликаємо процедуру
CALL process_h3_10_mapping_v2();

-- 8. Очищення
DROP PROCEDURE IF EXISTS process_h3_10_mapping_v2();
DROP TABLE IF EXISTS temp_h3_10_batch;

-- 9. Фінальна статистика по всіх резолюціях
SELECT 
    h3_resolution as "H3 рівень",
    COUNT(*) as "Гексагонів",
    COUNT(DISTINCT oblast_id) as "Областей",
    ROUND(100.0 * COUNT(raion_id) / COUNT(*), 1) as "% в районах",
    ROUND(100.0 * COUNT(gromada_id) / COUNT(*), 1) as "% в громадах",
    ROUND(100.0 * COUNT(settlement_id) / COUNT(*), 1) as "% в нас.пунктах"
FROM osm_ukraine.h3_admin_mapping
GROUP BY h3_resolution
ORDER BY h3_resolution;