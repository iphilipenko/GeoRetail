-- ================================================================
-- Файл: 44_update_h3_9_mapping_for_new_hierarchy.sql
-- Мета: Оновлення прив'язки H3-9 гексагонів до нової ієрархії
-- Дата: 2025-01-04
-- 
-- H3-9: ~2,478,145 гексагонів (найдетальніша резолюція)
-- ================================================================

-- 1. Очищаємо таблицю для H3-9
DELETE FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 9;

-- 2. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('h3_9_mapping_v2', 'started', 'Початок прив''язки H3-9 до нової ієрархії (2,478,145 гексагонів)');

-- 3. Створюємо тимчасову таблицю для батчевої обробки
DROP TABLE IF EXISTS temp_h3_9_batch;
CREATE TEMP TABLE temp_h3_9_batch AS
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
WHERE h.resolution = 9;

-- Індекси для швидкості
CREATE INDEX idx_temp_h3_9_oblast ON temp_h3_9_batch(oblast_id);
CREATE INDEX idx_temp_h3_9_center ON temp_h3_9_batch USING GIST(center_point);
ANALYZE temp_h3_9_batch; -- Оновлення статистики для оптимізатора

-- 4. Статистика по тимчасовій таблиці
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'h3_9_mapping_v2',
    'info',
    'Гексагонів H3-9 в межах України: ' || COUNT(*),
    jsonb_build_object(
        'total_in_ukraine', COUNT(*), 
        'unique_oblasts', COUNT(DISTINCT oblast_id),
        'avg_per_oblast', (COUNT(*) / COUNT(DISTINCT oblast_id))
    )
FROM temp_h3_9_batch;

-- 5. Видаляємо стару процедуру якщо існує
DROP PROCEDURE IF EXISTS process_h3_9_mapping_v2();

-- 6. Створюємо процедуру для батчевої обробки
CREATE PROCEDURE process_h3_9_mapping_v2()
LANGUAGE plpgsql
AS $$
DECLARE
    oblast_rec RECORD;
    batch_size INTEGER := 0;
    total_processed INTEGER := 0;
    oblast_count INTEGER := 0;
    start_time TIMESTAMP;
    oblast_time INTERVAL;
BEGIN
    start_time := clock_timestamp();
    
    -- Проходимо по кожній області
    FOR oblast_rec IN 
        SELECT DISTINCT oblast_id, oblast_name
        FROM temp_h3_9_batch
        ORDER BY oblast_name
    LOOP
        oblast_count := oblast_count + 1;
        
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
        FROM temp_h3_9_batch t
        
        -- Район (рівень 5, був 6)
        LEFT JOIN osm_ukraine.admin_boundaries r 
            ON r.admin_level = 5
            AND r.parent_id = t.oblast_id
            AND ST_Contains(r.geometry, t.center_point)
            
        -- Громада (рівень 6, нове)
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
        
        -- Логуємо і COMMIT кожні 2 області (частіше для H3-9)
        IF oblast_count % 2 = 0 THEN
            oblast_time := clock_timestamp() - start_time;
            
            INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
            VALUES (
                'h3_9_mapping_v2', 
                'progress', 
                'Оброблено областей: ' || oblast_count || ', гексагонів: ' || total_processed,
                jsonb_build_object(
                    'last_oblast', oblast_rec.oblast_name,
                    'total_processed', total_processed,
                    'elapsed_time', oblast_time::text,
                    'avg_seconds_per_oblast', (EXTRACT(EPOCH FROM oblast_time) / oblast_count)::int,
                    'estimated_total_minutes', ((EXTRACT(EPOCH FROM oblast_time) / oblast_count) * 26 / 60)::int
                )
            );
            
            COMMIT; -- Критично для H3-9 через великий об'єм
        END IF;
    END LOOP;
    
    -- Фінальний COMMIT
    COMMIT;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES (
        'h3_9_mapping_v2', 
        'completed', 
        'Завершено обробку H3-9',
        jsonb_build_object(
            'oblasts_processed', oblast_count,
            'total_hexagons', total_processed,
            'total_time', (clock_timestamp() - start_time)::text,
            'avg_hexagons_per_second', (total_processed / EXTRACT(EPOCH FROM (clock_timestamp() - start_time)))::int
        )
    );
    
    COMMIT;
END;
$$;

-- 7. Викликаємо процедуру
CALL process_h3_9_mapping_v2();

-- 8. Очищення
DROP PROCEDURE IF EXISTS process_h3_9_mapping_v2();
DROP TABLE IF EXISTS temp_h3_9_batch;

-- 9. Статистика результатів
WITH stats AS (
    SELECT 
        COUNT(*) as total_mapped,
        COUNT(DISTINCT oblast_id) as unique_oblasts,
        COUNT(raion_id) as with_raion,
        COUNT(gromada_id) as with_gromada,
        COUNT(settlement_id) as with_settlement,
        COUNT(CASE WHEN settlement_admin_level = 7 THEN 1 END) as in_obl_centers,
        COUNT(CASE WHEN settlement_admin_level = 8 THEN 1 END) as in_raion_centers,
        COUNT(CASE WHEN settlement_admin_level = 9 THEN 1 END) as in_villages
    FROM osm_ukraine.h3_admin_mapping
    WHERE h3_resolution = 9
)
SELECT 
    'H3-9 Final Stats' as metric,
    total_mapped as "Всього",
    ROUND(100.0 * with_raion / total_mapped, 2) as "% в районах",
    ROUND(100.0 * with_gromada / total_mapped, 2) as "% в громадах",
    ROUND(100.0 * with_settlement / total_mapped, 2) as "% в нас.пунктах",
    in_obl_centers as "В обл.центрах",
    in_raion_centers as "В рай.центрах",
    in_villages as "В селах"
FROM stats;

-- 10. Перевірка покриття
SELECT 
    'Coverage H3-9' as check_type,
    (SELECT COUNT(*) FROM osm_ukraine.h3_grid WHERE resolution = 9) as total_h3_9,
    (SELECT COUNT(*) FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 9) as mapped_h3_9,
    ROUND(100.0 * 
        (SELECT COUNT(*) FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 9) / 
        (SELECT COUNT(*) FROM osm_ukraine.h3_grid WHERE resolution = 9), 2
    ) as coverage_percent;