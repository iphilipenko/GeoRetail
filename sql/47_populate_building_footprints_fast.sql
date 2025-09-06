-- ================================================================
-- Файл: 47_populate_building_footprints_all_truncated.sql
-- Мета: Імпорт з обрізанням ВСІХ текстових полів для гарантії успіху
-- ================================================================

-- Видаляємо старі версії
DROP FUNCTION IF EXISTS osm_ukraine.safe_json_extract CASCADE;
DROP FUNCTION IF EXISTS osm_ukraine.classify_building_category CASCADE;
DROP FUNCTION IF EXISTS osm_ukraine.estimate_residential_units CASCADE;

-- 1.1 Функція безпечного витягнення JSON
DROP FUNCTION IF EXISTS osm_ukraine.safe_json_extract CASCADE;
CREATE OR REPLACE FUNCTION osm_ukraine.safe_json_extract(json_text TEXT, key TEXT)
RETURNS TEXT AS $$
BEGIN
    IF json_text IS NULL OR json_text = '' THEN
        RETURN NULL;
    END IF;
    
    BEGIN
        RETURN (json_text::jsonb)->>key;
    EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 1.2 Функція класифікації будівель
DROP FUNCTION IF EXISTS osm_ukraine.classify_building_category CASCADE;
CREATE OR REPLACE FUNCTION osm_ukraine.classify_building_category(building_type TEXT) 
RETURNS VARCHAR(30) AS $$
BEGIN
    RETURN CASE 
        WHEN building_type IN ('apartments', 'residential', 'house', 'detached', 
                               'semidetached_house', 'terraced', 'dormitory', 'bungalow') 
            THEN 'residential'
        WHEN building_type IN ('commercial', 'retail', 'supermarket', 'kiosk', 
                               'shop', 'office', 'hotel', 'mall') 
            THEN 'commercial'
        WHEN building_type IN ('industrial', 'warehouse', 'factory', 'manufacture') 
            THEN 'industrial'
        WHEN building_type IN ('public', 'civic', 'hospital', 'school', 'university', 
                               'kindergarten', 'church', 'cathedral', 'chapel', 
                               'mosque', 'synagogue', 'temple', 'stadium', 'train_station') 
            THEN 'public'
        WHEN building_type IN ('barn', 'cowshed', 'farm_auxiliary', 'greenhouse', 
                               'stable', 'sty', 'farm') 
            THEN 'agricultural'
        ELSE 'other'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 1.3 Функція оцінки житлових одиниць
DROP FUNCTION IF EXISTS osm_ukraine.estimate_residential_units CASCADE;
CREATE OR REPLACE FUNCTION osm_ukraine.estimate_residential_units(
    building_type TEXT,
    area_sqm NUMERIC,
    levels INTEGER
) RETURNS INTEGER AS $$
DECLARE
    effective_levels INTEGER;
    total_area NUMERIC;
BEGIN
    IF area_sqm IS NULL OR area_sqm <= 0 THEN
        RETURN 0;
    END IF;
    
    effective_levels := COALESCE(levels, 
        CASE 
            WHEN building_type = 'apartments' THEN 5
            WHEN building_type = 'residential' THEN 3
            WHEN building_type IN ('house', 'detached', 'bungalow') THEN 1
            WHEN building_type IN ('semidetached_house', 'terraced') THEN 2
            WHEN building_type = 'dormitory' THEN 4
            ELSE 1
        END
    );
    
    total_area := area_sqm * effective_levels;
    
    RETURN CASE
        WHEN building_type IN ('house', 'detached', 'bungalow') THEN 1
        WHEN building_type = 'semidetached_house' THEN 2
        WHEN building_type = 'terraced' THEN GREATEST(2, (total_area / 100)::INTEGER)
        WHEN building_type IN ('apartments', 'residential') THEN 
            GREATEST(1, (total_area / 65)::INTEGER)
        WHEN building_type = 'dormitory' THEN 
            GREATEST(1, (total_area / 25)::INTEGER)
        WHEN building_type = 'yes' AND area_sqm > 200 THEN
            GREATEST(2, (total_area / 80)::INTEGER)
        ELSE 1
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;


CREATE TABLE IF NOT EXISTS osm_ukraine.building_import_progress (
    id SERIAL PRIMARY KEY,
    last_processed_id BIGINT DEFAULT 0,
    total_processed INTEGER DEFAULT 0,
    total_count INTEGER,
    batch_size INTEGER,
    status VARCHAR(20) CHECK (status IN ('in_progress', 'completed', 'error', 'paused')),
    started_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    error_message TEXT
);


DROP PROCEDURE IF EXISTS osm_ukraine.import_buildings_batch;

CREATE OR REPLACE PROCEDURE osm_ukraine.import_buildings_batch(
    batch_size_param INTEGER DEFAULT 10000,
    continue_from_last BOOLEAN DEFAULT TRUE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_count INTEGER := 0;
    v_processed_in_batch INTEGER;
    v_total_processed INTEGER := 0;
    v_last_id BIGINT := 0;
    v_max_id_in_batch BIGINT;
    v_total_to_process INTEGER;
    v_start_time TIMESTAMP;
    v_batch_start TIMESTAMP;
    v_elapsed_time INTERVAL;
    v_progress_id INTEGER;
    v_existing_processed INTEGER;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Перевіряємо попередній прогрес
    IF continue_from_last THEN
        SELECT 
            p.last_processed_id, 
            p.total_processed 
        INTO 
            v_last_id, 
            v_existing_processed
        FROM osm_ukraine.building_import_progress p
        WHERE p.status IN ('in_progress', 'paused', 'error')
        ORDER BY p.id DESC
        LIMIT 1;
        
        IF v_last_id IS NOT NULL THEN
            v_total_processed := v_existing_processed;
            RAISE NOTICE 'Продовжуємо з ID: %, вже оброблено: %', v_last_id, v_total_processed;
        ELSE
            v_last_id := 0;
            v_total_processed := 0;
        END IF;
    ELSE
        v_last_id := 0;
        v_total_processed := 0;
    END IF;
    
    -- Рахуємо залишок
    RAISE NOTICE 'Рахуємо кількість будівель...';
    SELECT COUNT(*) INTO v_total_to_process
    FROM osm_ukraine.osm_raw r
    WHERE r.id > v_last_id
      AND osm_ukraine.safe_json_extract(r.tags->>'tags', 'building') IS NOT NULL
      AND ST_GeometryType(r.geom) = 'ST_Polygon';
    
    -- Створюємо запис прогресу
    INSERT INTO osm_ukraine.building_import_progress 
        (last_processed_id, total_processed, total_count, batch_size, status, started_at)
    VALUES 
        (v_last_id, v_total_processed, v_total_to_process + v_total_processed, batch_size_param, 'in_progress', v_start_time)
    RETURNING id INTO v_progress_id;
    
    RAISE NOTICE '=====================================';
    RAISE NOTICE 'Початок: %', v_start_time;
    RAISE NOTICE 'Залишилось: %', v_total_to_process;
    RAISE NOTICE 'Батч: %', batch_size_param;
    RAISE NOTICE '=====================================';
    
    -- Основний цикл
    LOOP
        v_batch_start := clock_timestamp();
        v_batch_count := v_batch_count + 1;
        
        BEGIN
            -- Отримуємо максимальний ID для батчу
            SELECT MAX(id) INTO v_max_id_in_batch
            FROM (
                SELECT id 
                FROM osm_ukraine.osm_raw r
                WHERE r.id > v_last_id
                  AND osm_ukraine.safe_json_extract(r.tags->>'tags', 'building') IS NOT NULL
                  AND ST_GeometryType(r.geom) = 'ST_Polygon'
                ORDER BY r.id
                LIMIT batch_size_param
            ) batch_ids;
            
            IF v_max_id_in_batch IS NULL THEN
                EXIT;
            END IF;
            
            -- Вставляємо батч
            WITH batch_data AS (
                SELECT * FROM osm_ukraine.osm_raw r
                WHERE r.id > v_last_id
                  AND r.id <= v_max_id_in_batch
                  AND osm_ukraine.safe_json_extract(r.tags->>'tags', 'building') IS NOT NULL
                  AND ST_GeometryType(r.geom) = 'ST_Polygon'
                  AND r.tags->>'tags' LIKE '{%}'
                  AND r.tags->>'tags' LIKE '%}'
                  AND ST_Area(r.geom::geography) > 10
            )
            INSERT INTO osm_ukraine.building_footprints (
                osm_id,
                osm_raw_id,
                building_category,
                building_type,
                building_use,
                footprint,
                perimeter_m,
                h3_res_7,
                h3_res_8,
                h3_res_9,
                h3_res_10,
                building_levels,
                building_height_m,
                building_material,
                roof_shape,
                addr_street,
                addr_housenumber,
                addr_city,
                addr_postcode,
                name,
                total_floor_area_sqm,
                residential_units_estimate,
                commercial_units_estimate,
                population_estimate,
                data_completeness,
                confidence_score
            )
            SELECT 
                r.osm_id,
                r.id,
                LEFT(osm_ukraine.classify_building_category(osm_ukraine.safe_json_extract(r.tags->>'tags', 'building')), 30),
                LEFT(osm_ukraine.safe_json_extract(r.tags->>'tags', 'building'), 50),
                LEFT(COALESCE(
                    CASE WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'shop') IS NOT NULL 
                         THEN 'shop:' || osm_ukraine.safe_json_extract(r.tags->>'tags', 'shop') END,
                    CASE WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'amenity') IS NOT NULL 
                         THEN 'amenity:' || osm_ukraine.safe_json_extract(r.tags->>'tags', 'amenity') END,
                    CASE WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'office') IS NOT NULL 
                         THEN 'office:' || osm_ukraine.safe_json_extract(r.tags->>'tags', 'office') END
                ), 100),
                r.geom,
                ST_Perimeter(r.geom::geography),
                LEFT(r.h3_res_7, 15),
                LEFT(r.h3_res_8, 15),
                LEFT(r.h3_res_9, 15),
                LEFT(r.h3_res_10, 15),
                CASE 
                    WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels') ~ '^\d+$' 
                    THEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels')::INTEGER
                    ELSE NULL
                END,
                CASE 
                    WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'height') ~ '^\d+(\.\d+)?$' 
                    THEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'height')::NUMERIC
                    ELSE NULL
                END,
                LEFT(osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:material'), 50),
                LEFT(osm_ukraine.safe_json_extract(r.tags->>'tags', 'roof:shape'), 30),
                LEFT(COALESCE(r.addr_street, osm_ukraine.safe_json_extract(r.tags->>'tags', 'addr:street')), 255),
                LEFT(COALESCE(r.addr_housenumber, osm_ukraine.safe_json_extract(r.tags->>'tags', 'addr:housenumber')), 20),
                LEFT(COALESCE(r.addr_city, osm_ukraine.safe_json_extract(r.tags->>'tags', 'addr:city')), 255),
                LEFT(COALESCE(r.addr_postcode, osm_ukraine.safe_json_extract(r.tags->>'tags', 'addr:postcode')), 20),
                LEFT(COALESCE(r.name, osm_ukraine.safe_json_extract(r.tags->>'tags', 'name')), 255),
                ST_Area(r.geom::geography) * COALESCE(
                    CASE 
                        WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels') ~ '^\d+$' 
                        THEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels')::INTEGER
                        ELSE NULL
                    END,
                    CASE 
                        WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building') = 'apartments' THEN 5
                        WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building') = 'residential' THEN 3
                        ELSE 1
                    END
                ),
                osm_ukraine.estimate_residential_units(
                    osm_ukraine.safe_json_extract(r.tags->>'tags', 'building'),
                    ST_Area(r.geom::geography)::NUMERIC,
                    CASE 
                        WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels') ~ '^\d+$' 
                        THEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels')::INTEGER
                        ELSE NULL
                    END
                ),
                CASE 
                    WHEN osm_ukraine.classify_building_category(osm_ukraine.safe_json_extract(r.tags->>'tags', 'building')) = 'commercial'
                    THEN GREATEST(1, (ST_Area(r.geom::geography) * COALESCE(
                        CASE 
                            WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels') ~ '^\d+$' 
                            THEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels')::INTEGER
                            ELSE 1
                        END, 1) / 100)::INTEGER)
                    ELSE 0
                END,
                (osm_ukraine.estimate_residential_units(
                    osm_ukraine.safe_json_extract(r.tags->>'tags', 'building'),
                    ST_Area(r.geom::geography)::NUMERIC,
                    CASE 
                        WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels') ~ '^\d+$' 
                        THEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building:levels')::INTEGER
                        ELSE NULL
                    END
                ) * 2.5)::INTEGER,
                CASE 
                    WHEN r.addr_street IS NOT NULL AND r.addr_housenumber IS NOT NULL THEN 0.8
                    WHEN r.addr_city IS NOT NULL OR r.name IS NOT NULL THEN 0.6
                    ELSE 0.4
                END,
                CASE 
                    WHEN osm_ukraine.safe_json_extract(r.tags->>'tags', 'building') NOT IN ('yes', NULL) THEN 0.8
                    ELSE 0.5
                END
            FROM batch_data r
            ON CONFLICT (osm_id) DO NOTHING;
            
            GET DIAGNOSTICS v_processed_in_batch = ROW_COUNT;
            v_last_id := v_max_id_in_batch;
            
        EXCEPTION WHEN OTHERS THEN
            UPDATE osm_ukraine.building_import_progress
            SET error_message = SQLERRM,
                status = 'error',
                updated_at = NOW()
            WHERE id = v_progress_id;
            
            RAISE NOTICE 'Помилка: %', SQLERRM;
            COMMIT;
            RAISE;
        END;
        
        IF v_processed_in_batch = 0 THEN
            EXIT;
        END IF;
        
        v_total_processed := v_total_processed + v_processed_in_batch;
        
        UPDATE osm_ukraine.building_import_progress
        SET last_processed_id = v_last_id,
            total_processed = v_total_processed,
            updated_at = NOW()
        WHERE id = v_progress_id;
        
        v_elapsed_time := clock_timestamp() - v_start_time;
        
        RAISE NOTICE 'Батч %: +% | Всього: % (%.1f%%) | Час: %s', 
            v_batch_count,
            v_processed_in_batch,
            v_total_processed,
            (v_total_processed * 100.0 / NULLIF(v_total_to_process, 0)),
            EXTRACT(EPOCH FROM v_elapsed_time)::INTEGER;
        
        COMMIT;
    END LOOP;
    
    UPDATE osm_ukraine.building_import_progress
    SET status = 'completed',
        completed_at = clock_timestamp(),
        updated_at = NOW()
    WHERE id = v_progress_id;
    
    RAISE NOTICE 'ЗАВЕРШЕНО! Оброблено: %', v_total_processed;
    
    COMMIT;
END;
$$;


CALL osm_ukraine.import_buildings_batch(10000, FALSE); // Перший запуск з початку
CALL osm_ukraine.import_buildings_batch(10000, TRUE);  // Продовження з останнього місця
-- ================================================================
SELECT 
    status,
    TO_CHAR(total_processed, 'FM999,999,999') as processed,
    TO_CHAR(total_count, 'FM999,999,999') as total,
    ROUND(total_processed * 100.0 / NULLIF(total_count, 0), 2) || '%' as progress,
    NOW() - updated_at as since_last_update,
    error_message
FROM osm_ukraine.building_import_progress
ORDER BY id DESC
LIMIT 1;

-- ================================================================
SELECT 
    building_category,
    COUNT(*) as count,
    ROUND(AVG(area_sqm), 2) as avg_area_sqm,
    SUM(residential_units_estimate) as total_units,
    SUM(population_estimate) as total_population
FROM osm_ukraine.building_footprints
GROUP BY building_category
ORDER BY count DESC;

