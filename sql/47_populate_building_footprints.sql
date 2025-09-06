-- ================================================================
-- Файл: 47_populate_building_footprints.sql
-- Мета: Імпорт та класифікація будівель з osm_raw в building_footprints
-- Дата: 2025-01-05
-- 
-- Процес включає:
-- - Витягнення ~12.6 млн будівель з osm_raw
-- - Класифікацію за типами (житлова, комерційна, промислова)
-- - Розрахунок оцінок населення та домогосподарств
-- - Обробка батчами для оптимізації
-- 
-- Орієнтовний час виконання: 30-45 хвилин для всієї України
-- ================================================================

-- 1. Створюємо функцію для класифікації будівель
CREATE OR REPLACE FUNCTION classify_building_category(building_type TEXT) 
RETURNS VARCHAR(30) AS $$
BEGIN
    RETURN CASE 
        -- Житлові
        WHEN building_type IN ('apartments', 'residential', 'house', 'detached', 
                               'semidetached_house', 'terraced', 'dormitory', 'bungalow') 
            THEN 'residential'
        
        -- Комерційні
        WHEN building_type IN ('commercial', 'retail', 'supermarket', 'kiosk', 
                               'shop', 'office', 'hotel', 'mall') 
            THEN 'commercial'
        
        -- Промислові
        WHEN building_type IN ('industrial', 'warehouse', 'factory', 'manufacture') 
            THEN 'industrial'
        
        -- Громадські
        WHEN building_type IN ('public', 'civic', 'hospital', 'school', 'university', 
                               'kindergarten', 'church', 'cathedral', 'chapel', 
                               'mosque', 'synagogue', 'temple', 'stadium', 'train_station') 
            THEN 'public'
        
        -- Сільськогосподарські
        WHEN building_type IN ('barn', 'cowshed', 'farm_auxiliary', 'greenhouse', 
                               'stable', 'sty', 'farm') 
            THEN 'agricultural'
        
        -- Інші
        ELSE 'other'
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 2. Створюємо функцію для розрахунку кількості житлових одиниць
CREATE OR REPLACE FUNCTION estimate_residential_units(
    building_type TEXT,
    area_sqm NUMERIC,
    levels INTEGER
) RETURNS INTEGER AS $$
DECLARE
    effective_levels INTEGER;
    total_area NUMERIC;
BEGIN
    -- Визначаємо ефективну кількість поверхів
    effective_levels := COALESCE(levels, 
        CASE 
            WHEN building_type = 'apartments' THEN 5  -- типова 5-поверхівка
            WHEN building_type = 'residential' THEN 3  -- припускаємо багатоквартирний
            WHEN building_type IN ('house', 'detached', 'bungalow') THEN 1
            WHEN building_type IN ('semidetached_house', 'terraced') THEN 2
            ELSE 1
        END
    );
    
    -- Розраховуємо загальну площу
    total_area := area_sqm * effective_levels;
    
    -- Розраховуємо кількість квартир
    RETURN CASE
        WHEN building_type IN ('house', 'detached', 'bungalow') THEN 1
        WHEN building_type = 'semidetached_house' THEN 2
        WHEN building_type = 'terraced' THEN GREATEST(2, total_area::INTEGER / 100)
        WHEN building_type IN ('apartments', 'residential') THEN 
            GREATEST(1, (total_area / 65)::INTEGER)  -- 65 м² на квартиру
        WHEN building_type = 'dormitory' THEN 
            GREATEST(1, (total_area / 25)::INTEGER)  -- менша площа на кімнату
        ELSE 0
    END;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- 3. Логування процесу
CREATE TABLE IF NOT EXISTS osm_ukraine.building_import_log (
    log_id SERIAL PRIMARY KEY,
    batch_num INTEGER,
    records_processed INTEGER,
    status VARCHAR(20),
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 4. Основний імпорт даних (батчами по 100,000 записів)
DO $$
DECLARE
    batch_size INTEGER := 100000;
    total_count INTEGER;
    current_offset INTEGER := 0;
    batch_num INTEGER := 1;
    processed INTEGER;
BEGIN
    -- Отримуємо загальну кількість будівель
    SELECT COUNT(*) INTO total_count
    FROM osm_ukraine.osm_raw
    WHERE (tags->>'tags')::jsonb->>'building' IS NOT NULL
      AND ST_GeometryType(geom) = 'ST_Polygon';
    
    RAISE NOTICE 'Початок імпорту % будівель', total_count;
    
    -- Обробляємо батчами
    WHILE current_offset < total_count LOOP
        
        -- Вставляємо батч
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
            r.id as osm_raw_id,
            
            -- Класифікація
            classify_building_category((r.tags->>'tags')::jsonb->>'building'),
            (r.tags->>'tags')::jsonb->>'building' as building_type,
            CASE 
                WHEN (r.tags->>'tags')::jsonb->>'shop' IS NOT NULL 
                    THEN 'shop:' || (r.tags->>'tags')::jsonb->>'shop'
                WHEN (r.tags->>'tags')::jsonb->>'amenity' IS NOT NULL 
                    THEN 'amenity:' || (r.tags->>'tags')::jsonb->>'amenity'
                WHEN (r.tags->>'tags')::jsonb->>'office' IS NOT NULL 
                    THEN 'office:' || (r.tags->>'tags')::jsonb->>'office'
                ELSE NULL
            END as building_use,
            
            -- Геометрія
            r.geom as footprint,
            ST_Perimeter(r.geom::geography) as perimeter_m,
            
            -- H3 індекси (копіюємо готові)
            r.h3_res_7,
            r.h3_res_8,
            r.h3_res_9,
            r.h3_res_10,
            
            -- Характеристики будівлі
            CASE 
                WHEN (r.tags->>'tags')::jsonb->>'building:levels' ~ '^\d+(\.\d+)?$' 
                THEN ((r.tags->>'tags')::jsonb->>'building:levels')::NUMERIC::INTEGER
                ELSE NULL
            END as building_levels,
            
            CASE 
                WHEN (r.tags->>'tags')::jsonb->>'height' ~ '^\d+(\.\d+)?$' 
                THEN ((r.tags->>'tags')::jsonb->>'height')::NUMERIC
                ELSE NULL
            END as building_height_m,
            
            (r.tags->>'tags')::jsonb->>'building:material' as building_material,
            (r.tags->>'tags')::jsonb->>'roof:shape' as roof_shape,
            
            -- Адреса (частково вже витягнута в osm_raw)
            COALESCE(r.addr_street, (r.tags->>'tags')::jsonb->>'addr:street'),
            COALESCE(r.addr_housenumber, (r.tags->>'tags')::jsonb->>'addr:housenumber'),
            COALESCE(r.addr_city, (r.tags->>'tags')::jsonb->>'addr:city'),
            COALESCE(r.addr_postcode, (r.tags->>'tags')::jsonb->>'addr:postcode'),
            
            -- Назва
            COALESCE(r.name, (r.tags->>'tags')::jsonb->>'name'),
            
            -- Розрахунок площ
            ST_Area(r.geom::geography) * COALESCE(
                CASE 
                    WHEN (r.tags->>'tags')::jsonb->>'building:levels' ~ '^\d+(\.\d+)?$' 
                    THEN ((r.tags->>'tags')::jsonb->>'building:levels')::NUMERIC::INTEGER
                    ELSE NULL
                END,
                CASE 
                    WHEN (r.tags->>'tags')::jsonb->>'building' = 'apartments' THEN 5
                    WHEN (r.tags->>'tags')::jsonb->>'building' = 'residential' THEN 3
                    ELSE 1
                END
            ) as total_floor_area,
            
            -- Оцінка житлових одиниць
            estimate_residential_units(
                (r.tags->>'tags')::jsonb->>'building',
                ST_Area(r.geom::geography),
                CASE 
                    WHEN (r.tags->>'tags')::jsonb->>'building:levels' ~ '^\d+(\.\d+)?$' 
                    THEN ((r.tags->>'tags')::jsonb->>'building:levels')::NUMERIC::INTEGER
                    ELSE NULL
                END
            ) as residential_units,
            
            -- Оцінка комерційних одиниць
            CASE 
                WHEN classify_building_category((r.tags->>'tags')::jsonb->>'building') = 'commercial'
                THEN GREATEST(1, (ST_Area(r.geom::geography) * COALESCE(
                    CASE 
                        WHEN (r.tags->>'tags')::jsonb->>'building:levels' ~ '^\d+(\.\d+)?$' 
                        THEN ((r.tags->>'tags')::jsonb->>'building:levels')::NUMERIC::INTEGER
                        ELSE 1
                    END, 1) / 100)::INTEGER)
                ELSE 0
            END as commercial_units,
            
            -- Оцінка населення (2.5 людини на домогосподарство)
            (estimate_residential_units(
                (r.tags->>'tags')::jsonb->>'building',
                ST_Area(r.geom::geography),
                CASE 
                    WHEN (r.tags->>'tags')::jsonb->>'building:levels' ~ '^\d+(\.\d+)?$' 
                    THEN ((r.tags->>'tags')::jsonb->>'building:levels')::NUMERIC::INTEGER
                    ELSE NULL
                END
            ) * 2.5)::INTEGER as population,
            
            -- Якість даних
            CASE 
                WHEN r.addr_street IS NOT NULL AND r.addr_housenumber IS NOT NULL THEN 0.8
                WHEN r.name IS NOT NULL THEN 0.6
                ELSE 0.4
            END as data_completeness,
            
            CASE 
                WHEN (r.tags->>'tags')::jsonb->>'building' != 'yes' THEN 0.8
                ELSE 0.5
            END as confidence_score
            
        FROM osm_ukraine.osm_raw r
        WHERE (r.tags->>'tags')::jsonb->>'building' IS NOT NULL
          AND ST_GeometryType(r.geom) = 'ST_Polygon'
        ORDER BY r.id
        LIMIT batch_size
        OFFSET current_offset
        ON CONFLICT (osm_id) DO NOTHING;  -- пропускаємо дублікати
        
        GET DIAGNOSTICS processed = ROW_COUNT;
        
        -- Логуємо прогрес
        INSERT INTO osm_ukraine.building_import_log (batch_num, records_processed, status, message)
        VALUES (batch_num, processed, 'success', 
                format('Batch %s: оброблено %s з %s', batch_num, current_offset + processed, total_count));
        
        RAISE NOTICE 'Batch %: оброблено % з % (%.1f%%)', 
            batch_num, current_offset + processed, total_count, 
            (current_offset + processed) * 100.0 / total_count;
        
        -- Оновлюємо лічильники
        current_offset := current_offset + batch_size;
        batch_num := batch_num + 1;
        
        -- Commit після кожного батчу
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'Імпорт завершено! Оброблено % будівель', total_count;
    
END $$;

-- 5. Оновлюємо статистику для оптимізації запитів
ANALYZE osm_ukraine.building_footprints;

-- 6. Підсумкова статистика
SELECT 
    building_category,
    COUNT(*) as count,
    SUM(residential_units_estimate) as total_units,
    SUM(population_estimate) as total_population,
    ROUND(AVG(area_sqm), 2) as avg_area_sqm
FROM osm_ukraine.building_footprints
GROUP BY building_category
ORDER BY count DESC;