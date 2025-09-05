-- ================================================================
-- Файл: 42_update_h3_mapping_for_new_hierarchy.sql
-- Мета: Оновлення прив'язки H3 гексагонів до нової ієрархії
-- Дата: 2025-01-04
-- 
-- Нова ієрархія:
-- 4: Області (26)
-- 5: Райони (134)
-- 6: Громади (1465)
-- 7: Обласні центри (24)
-- 8: Районні центри (104)
-- 9: Села/селища (5382)
-- ================================================================

-- 1. Очищаємо таблицю для H3-7
DELETE FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 7;

-- 2. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('h3_7_mapping_v2', 'started', 'Початок прив''язки H3-7 до нової ієрархії');

-- 3. Заповнюємо прив'язку для H3-7 батчами по областях
DO $$
DECLARE
    oblast_rec RECORD;
    processed_count INTEGER := 0;
    oblast_count INTEGER := 0;
    batch_count INTEGER := 0;
BEGIN
    -- Проходимо по кожній області
    FOR oblast_rec IN 
        SELECT id, name_uk, geometry 
        FROM osm_ukraine.admin_boundaries 
        WHERE admin_level = 4
        ORDER BY name_uk
    LOOP
        oblast_count := oblast_count + 1;
        
        -- Вставляємо гексагони що попадають в цю область
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
            h.h3_index,
            h.resolution,
            oblast_rec.id as oblast_id,
            oblast_rec.name_uk as oblast_name,
            r.id as raion_id,
            r.name_uk as raion_name,
            g.id as gromada_id,
            g.name_uk as gromada_name,
            -- Пріоритет: села(9) → рай.центри(8) → обл.центри(7)
            COALESCE(s9.id, s8.id, s7.id) as settlement_id,
            COALESCE(s9.name_uk, s8.name_uk, s7.name_uk) as settlement_name,
            COALESCE(s9.admin_level, s8.admin_level, s7.admin_level) as settlement_admin_level
        FROM osm_ukraine.h3_grid h
        
        -- Район (рівень 5, був 6)
        LEFT JOIN osm_ukraine.admin_boundaries r 
            ON r.admin_level = 5 
            AND r.parent_id = oblast_rec.id
            AND ST_Contains(r.geometry, h.center_point)
            
        -- Громада (рівень 6, був 9)
        LEFT JOIN osm_ukraine.admin_boundaries g
            ON g.admin_level = 6
            AND g.parent_id = r.id
            AND ST_Contains(g.geometry, h.center_point)
            
        -- Обласний центр (рівень 7)
        LEFT JOIN osm_ukraine.admin_boundaries s7
            ON s7.admin_level = 7
            AND s7.parent_id = oblast_rec.id
            AND ST_Contains(s7.geometry, h.center_point)
            
        -- Районний центр (рівень 8)
        LEFT JOIN osm_ukraine.admin_boundaries s8
            ON s8.admin_level = 8
            AND s8.parent_id = r.id
            AND ST_Contains(s8.geometry, h.center_point)
            
        -- Село/селище (рівень 9)
        LEFT JOIN osm_ukraine.admin_boundaries s9
            ON s9.admin_level = 9
            AND s9.parent_id = g.id
            AND ST_Contains(s9.geometry, h.center_point)
            
        WHERE h.resolution = 7
            AND ST_Contains(oblast_rec.geometry, h.center_point)
        ON CONFLICT (h3_index) DO NOTHING;
        
        -- Підраховуємо скільки вже обробили
        GET DIAGNOSTICS batch_count = ROW_COUNT;
        processed_count := processed_count + batch_count;
        
        -- Логуємо прогрес кожні 5 областей
        IF oblast_count % 5 = 0 THEN
            INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
            VALUES (
                'h3_7_mapping_v2', 
                'progress', 
                'Оброблено областей: ' || oblast_count,
                jsonb_build_object(
                    'last_oblast', oblast_rec.name_uk,
                    'total_processed', processed_count
                )
            );
        END IF;
    END LOOP;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
    VALUES (
        'h3_7_mapping_v2', 
        'completed', 
        'Завершено обробку ' || oblast_count || ' областей',
        jsonb_build_object('total_hexagons', processed_count)
    );
END $$;

-- 4. Статистика результатів
SELECT 
    'H3-7 Mapping Statistics' as metric,
    COUNT(*) as total_mapped,
    COUNT(DISTINCT oblast_id) as unique_oblasts,
    COUNT(raion_id) as with_raion,
    COUNT(gromada_id) as with_gromada,
    COUNT(settlement_id) as with_settlement,
    ROUND(100.0 * COUNT(raion_id) / COUNT(*), 2) as pct_with_raion,
    ROUND(100.0 * COUNT(gromada_id) / COUNT(*), 2) as pct_with_gromada,
    ROUND(100.0 * COUNT(settlement_id) / COUNT(*), 2) as pct_with_settlement
FROM osm_ukraine.h3_admin_mapping
WHERE h3_resolution = 7;