-- =====================================================
-- Прив'язка H3-7 гексагонів до адміністративних одиниць
-- Файл: sql/14_populate_h3_7_mapping.sql
-- =====================================================

-- 1. Очищаємо таблицю для H3-7
DELETE FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 7;

-- 2. Логування початку
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
VALUES ('h3_7_mapping', 'started', 'Початок прив'язки H3-7 гексагонів (136035 штук)');

-- 3. Заповнюємо прив'язку для H3-7 батчами по областях
DO $$
DECLARE
    oblast_rec RECORD;
    processed_count INTEGER := 0;
    oblast_count INTEGER := 0;
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
            COALESCE(s9.id, s8.id, s7.id) as settlement_id,
            COALESCE(s9.name_uk, s8.name_uk, s7.name_uk) as settlement_name,
            COALESCE(s9.admin_level, s8.admin_level, s7.admin_level) as settlement_admin_level
        FROM osm_ukraine.h3_grid h
        -- Район
        LEFT JOIN osm_ukraine.admin_boundaries r 
            ON r.admin_level = 6 
            AND r.parent_id = oblast_rec.id
            AND ST_Contains(r.geometry, h.center_point)
        -- Територіальна громада (рівень 7)
        LEFT JOIN osm_ukraine.admin_boundaries s7
            ON s7.admin_level = 7
            AND ST_Contains(s7.geometry, h.center_point)
        -- Населений пункт (рівень 8)
        LEFT JOIN osm_ukraine.admin_boundaries s8
            ON s8.admin_level = 8
            AND ST_Contains(s8.geometry, h.center_point)
        -- Село/частина міста (рівень 9)
        LEFT JOIN osm_ukraine.admin_boundaries s9
            ON s9.admin_level = 9
            AND ST_Contains(s9.geometry, h.center_point)
        WHERE h.resolution = 7
            AND ST_Contains(oblast_rec.geometry, h.center_point)
        ON CONFLICT (h3_index) DO NOTHING;
        
        -- Підраховуємо скільки вже обробили
        GET DIAGNOSTICS processed_count = ROW_COUNT;
        
        -- Логуємо прогрес кожні 5 областей
        IF oblast_count % 5 = 0 THEN
            INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
            VALUES (
                'h3_7_mapping', 
                'progress', 
                'Оброблено областей: ' || oblast_count,
                jsonb_build_object(
                    'last_oblast', oblast_rec.name_uk,
                    'hexagons_in_last', processed_count
                )
            );
        END IF;
    END LOOP;
    
    -- Фінальне логування
    INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message)
    VALUES ('h3_7_mapping', 'completed', 'Завершено обробку ' || oblast_count || ' областей');
END $$;

-- 4. Статистика результатів
WITH stats AS (
    SELECT 
        COUNT(*) as total_mapped,
        COUNT(DISTINCT oblast_id) as unique_oblasts,
        COUNT(raion_id) as with_raion,
        COUNT(settlement_id) as with_settlement,
        COUNT(CASE WHEN settlement_admin_level = 7 THEN 1 END) as in_communities,
        COUNT(CASE WHEN settlement_admin_level = 8 THEN 1 END) as in_settlements_8,
        COUNT(CASE WHEN settlement_admin_level = 9 THEN 1 END) as in_settlements_9
    FROM osm_ukraine.h3_admin_mapping
    WHERE h3_resolution = 7
)
SELECT 
    'Статистика H3-7' as info,
    total_mapped as "Всього прив'язано",
    unique_oblasts as "Унікальних областей",
    ROUND(100.0 * with_raion / total_mapped, 2) as "% з районом",
    ROUND(100.0 * with_settlement / total_mapped, 2) as "% з населеним пунктом",
    in_communities as "В громадах",
    in_settlements_8 as "В нас. пунктах рівня 8",
    in_settlements_9 as "В нас. пунктах рівня 9"
FROM stats;

-- 5. Приклад результатів
SELECT 
    h.h3_index,
    h.oblast_name as "Область",
    h.raion_name as "Район",
    h.settlement_name as "Населений пункт",
    h.settlement_admin_level as "Рівень НП"
FROM osm_ukraine.h3_admin_mapping h
WHERE h.h3_resolution = 7
    AND h.settlement_name IS NOT NULL
LIMIT 10;

-- 6. Перевірка покриття
SELECT 
    'Покриття H3-7' as check_type,
    (SELECT COUNT(*) FROM osm_ukraine.h3_grid WHERE resolution = 7) as total_h3_7,
    (SELECT COUNT(*) FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 7) as mapped_h3_7,
    (SELECT COUNT(*) FROM osm_ukraine.h3_grid WHERE resolution = 7) - 
    (SELECT COUNT(*) FROM osm_ukraine.h3_admin_mapping WHERE h3_resolution = 7) as not_mapped;