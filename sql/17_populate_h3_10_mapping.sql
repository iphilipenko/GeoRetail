-- =====================================================
-- Прив'язка H3-10 гексагонів до адміністративних одиниць
-- Файл: sql/17_populate_h3_10_mapping.sql
-- =====================================================

-- 1. Очищаємо таблицю для H3-10
DELETE FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 10;

-- 2. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('h3_10_mapping', 'started', 'Початок прив''язки H3-10 гексагонів (5,325,109 штук)');

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

-- 4. Статистика по тимчасовій таблиці
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'h3_10_mapping',
    'info',
    'Гексагонів H3-10 в межах України: ' || COUNT(*),
    jsonb_build_object('total_in_ukraine', COUNT(*), 'unique_oblasts', COUNT(DISTINCT oblast_id))
FROM temp_h3_10_batch;

-- 5. Видаляємо стару процедуру якщо існує
DROP PROCEDURE IF EXISTS process_h3_10_mapping();

-- 6. Створюємо процедуру для батчевої обробки з COMMIT
CREATE PROCEDURE process_h3_10_mapping()
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
        FROM temp_h3_10_batch
        ORDER BY oblast_name
    LOOP
        oblast_count := oblast_count + 1;
        
        -- Вставляємо гексагони цієї області з визначенням району та населеного пункту
        INSERT INTO osm_ukraine.h3_admin_mapping (
            h3_index,
            h3_resolution,
            oblast_id,
            oblast_name,
            raion_id,
            raion_name,
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
            COALESCE(s9.id, s8.id, s7.id) as settlement_id,
            COALESCE(s9.name_uk, s8.name_uk, s7.name_uk) as settlement_name,
            COALESCE(s9.admin_level, s8.admin_level, s7.admin_level) as settlement_admin_level
        FROM temp_h3_10_batch t
        -- Район
        LEFT JOIN osm_ukraine.admin_boundaries r 
            ON r.admin_level = 6 
            AND r.parent_id = t.oblast_id
            AND ST_Contains(r.geometry, t.center_point)
        -- Територіальна громада (рівень 7)
        LEFT JOIN osm_ukraine.admin_boundaries s7
            ON s7.admin_level = 7
            AND (s7.parent_id = r.id OR (r.id IS NULL AND s7.parent_id = t.oblast_id))
            AND ST_Contains(s7.geometry, t.center_point)
        -- Населений пункт (рівень 8)
        LEFT JOIN osm_ukraine.admin_boundaries s8
            ON s8.admin_level = 8
            AND ST_Contains(s8.geometry, t.center_point)
        -- Село/частина міста (рівень 9)
        LEFT JOIN osm_ukraine.admin_boundaries s9
            ON s9.admin_level = 9
            AND ST_Contains(s9.geometry, t.center_point)
        WHERE t.oblast_id = oblast_rec.oblast_id
        ON CONFLICT (h3_index) DO NOTHING;
        
        GET DIAGNOSTICS batch_size = ROW_COUNT;
        total_processed := total_processed + batch_size;
        
        -- Логуємо прогрес кожну область для H3-10 (більше даних)
        oblast_time := clock_timestamp() - start_time;
        
        INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
        VALUES (
            'h3_10_mapping', 
            'progress', 
            'Оброблено: ' || oblast_rec.oblast_name || ' (' || oblast_count || ' з 27)',
            jsonb_build_object(
                'oblast', oblast_rec.oblast_name,
                'hexagons_in_oblast', batch_size,
                'total_processed', total_processed,
                'elapsed_time', oblast_time::text,
                'avg_per_oblast', (EXTRACT(EPOCH FROM oblast_time) / oblast_count)::int || ' сек'
            )
        );
        
        -- COMMIT після кожної області для H3-10
        COMMIT;
    END LOOP;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES (
        'h3_10_mapping', 
        'completed', 
        'Завершено обробку H3-10',
        jsonb_build_object(
            'oblasts_processed', oblast_count,
            'total_hexagons', total_processed,
            'total_time', (clock_timestamp() - start_time)::text
        )
    );
    
    COMMIT;
END;
$$;