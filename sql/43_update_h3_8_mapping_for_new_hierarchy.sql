-- ================================================================
-- Файл: 43_update_h3_8_mapping_for_new_hierarchy.sql
-- Мета: Оновлення прив'язки H3-8 гексагонів до нової ієрархії
-- Дата: 2025-01-04
-- 
-- H3-8: ~759,291 гексагонів (детальніша резолюція)
-- ================================================================

-- 1. Очищаємо таблицю для H3-8
DELETE FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 8;

-- 2. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('h3_8_mapping_v2', 'started', 'Початок прив''язки H3-8 до нової ієрархії (759,291 гексагонів)');

-- 3. Створюємо тимчасову таблицю з прив'язкою до областей
CREATE TEMP TABLE IF NOT EXISTS temp_h3_8_batch AS
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
WHERE h.resolution = 8;

-- Індекси для швидкості
CREATE INDEX idx_temp_h3_8_oblast ON temp_h3_8_batch(oblast_id);
CREATE INDEX idx_temp_h3_8_center ON temp_h3_8_batch USING GIST(center_point);

-- 4. Статистика по тимчасовій таблиці
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'h3_8_mapping_v2',
    'info',
    'Гексагонів H3-8 в межах України: ' || COUNT(*),
    jsonb_build_object(
        'total_in_ukraine', COUNT(*), 
        'unique_oblasts', COUNT(DISTINCT oblast_id)
    )
FROM temp_h3_8_batch;

-- 5. Видаляємо стару процедуру якщо існує
DROP PROCEDURE IF EXISTS process_h3_8_mapping_v2();

-- 6. Створюємо процедуру для батчевої обробки
CREATE PROCEDURE process_h3_8_mapping_v2()
LANGUAGE plpgsql
AS $$
DECLARE
    oblast_rec RECORD;
    batch_size INTEGER := 0;
    total_processed INTEGER := 0;
    oblast_count INTEGER := 0;
BEGIN
    -- Проходимо по кожній області
    FOR oblast_rec IN 
        SELECT DISTINCT oblast_id, oblast_name
        FROM temp_h3_8_batch
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
        FROM temp_h3_8_batch t
        
        -- Район (рівень 5, був 6)
        LEFT JOIN osm_ukraine.admin_boundaries r 
            ON r.admin_level = 5
            AND r.parent_id = t.oblast_id
            AND ST_Contains(r.geometry, t.center_point)
            
        -- Громада (рівень 6, була на 7)
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
        
        -- Логуємо і COMMIT кожні 3 області
        IF oblast_count % 3 = 0 THEN
            INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
            VALUES (
                'h3_8_mapping_v2', 
                'progress', 
                'Оброблено областей: ' || oblast_count || ', гексагонів: ' || total_processed,
                jsonb_build_object(
                    'last_oblast', oblast_rec.oblast_name,
                    'total_processed', total_processed
                )
            );
            
            COMMIT; -- Звільнення пам'яті та locks
        END IF;
    END LOOP;
    
    -- Фінальний COMMIT
    COMMIT;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES (
        'h3_8_mapping_v2', 
        'completed', 
        'Завершено обробку H3-8',
        jsonb_build_object(
            'oblasts_processed', oblast_count,
            'total_hexagons', total_processed
        )
    );
    
    COMMIT;
END;
$$;

-- 7. Викликаємо процедуру
CALL process_h3_8_mapping_v2();

-- 8. Очищення
DROP PROCEDURE IF EXISTS process_h3_8_mapping_v2();
DROP TABLE IF EXISTS temp_h3_8_batch;

-- 9. Статистика результатів
WITH stats AS (
    SELECT 
        COUNT(*) as total_mapped,
        COUNT(DISTINCT oblast_id) as unique_oblasts,
        COUNT(raion_id) as with_raion,
        COUNT(gromada_id) as with_gromada,
        COUNT(settlement_id) as with_settlement
    FROM osm_ukraine.h3_admin_mapping
    WHERE h3_resolution = 8
)
SELECT 
    'H3-8 Statistics' as metric,
    total_mapped as "Всього прив'язано",
    unique_oblasts as "Областей",
    ROUND(100.0 * with_raion / NULLIF(total_mapped, 0), 2) as "% з районом",
    ROUND(100.0 * with_gromada / NULLIF(total_mapped, 0), 2) as "% з громадою",
    ROUND(100.0 * with_settlement / NULLIF(total_mapped, 0), 2) as "% з нас.пунктом"
FROM stats;