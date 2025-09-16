-- ================================================================
-- Файл: 05_cannibalization_competition_analysis_complete.sql
-- Шлях: /sql/analytics/05_cannibalization_competition_analysis_complete.sql
-- 
-- Опис: ПОВНИЙ скрипт аналізу канібалізації та конкуренції
--       з усіма VIEW та виправленою логікою фільтрації
-- 
-- Версія: 3.0 COMPLETE
-- Автор: Система геоаналітики Аврора
-- Дата: 2024-12
-- ================================================================

-- ================================================================
-- ЕТАП 1: ОНОВЛЕННЯ СТРУКТУРИ ТАБЛИЦЬ
-- ================================================================

-- 1.1 Додаємо коефіцієнт значимості до таблиці конкурентів
ALTER TABLE avrora.competitors
ADD COLUMN IF NOT EXISTS significance_coefficient DECIMAL(3,2) DEFAULT 1.0 
    CHECK (significance_coefficient > 0 AND significance_coefficient <= 2.0);

COMMENT ON COLUMN avrora.competitors.significance_coefficient IS 
'Коефіцієнт значимості конкурента: 0-1 = слабший за нас, 1-2 = сильніший за нас';

-- 1.2 Заповнюємо всіх конкурентів значенням 1.0
UPDATE avrora.competitors
SET significance_coefficient = 1.0
WHERE significance_coefficient IS NULL;

-- 1.3 Розширюємо таблицю перетинів ізохрон з новими полями
ALTER TABLE avrora.isochrone_overlaps
ADD COLUMN IF NOT EXISTS isochrone_polygon_1 geometry(Polygon, 4326),
ADD COLUMN IF NOT EXISTS isochrone_polygon_2 geometry(Polygon, 4326),
ADD COLUMN IF NOT EXISTS city_1 VARCHAR(255),
ADD COLUMN IF NOT EXISTS city_2 VARCHAR(255),
ADD COLUMN IF NOT EXISTS mode VARCHAR(20),
ADD COLUMN IF NOT EXISTS distance_meters INTEGER,
ADD COLUMN IF NOT EXISTS population_percent_1 DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS population_percent_2 DECIMAL(5,2),
ADD COLUMN IF NOT EXISTS significance_coefficient DECIMAL(3,2),
ADD COLUMN IF NOT EXISTS impact_coefficient DECIMAL(3,2),
ADD COLUMN IF NOT EXISTS affected_population INTEGER,
ADD COLUMN IF NOT EXISTS avg_transaction_sum DECIMAL(12,2),
ADD COLUMN IF NOT EXISTS purchasing_power_index DECIMAL(10,2);

-- Коментарі до нових полів
COMMENT ON COLUMN avrora.isochrone_overlaps.isochrone_polygon_1 IS 'Повна геометрія першої ізохрони';
COMMENT ON COLUMN avrora.isochrone_overlaps.isochrone_polygon_2 IS 'Повна геометрія другої ізохрони';
COMMENT ON COLUMN avrora.isochrone_overlaps.city_1 IS 'Місто першого об''єкта';
COMMENT ON COLUMN avrora.isochrone_overlaps.city_2 IS 'Місто другого об''єкта';
COMMENT ON COLUMN avrora.isochrone_overlaps.mode IS 'Режим транспорту (walk/drive)';
COMMENT ON COLUMN avrora.isochrone_overlaps.distance_meters IS 'Відстань ізохрони в метрах';
COMMENT ON COLUMN avrora.isochrone_overlaps.population_percent_1 IS 'Відсоток населення в перетині від першої ізохрони';
COMMENT ON COLUMN avrora.isochrone_overlaps.population_percent_2 IS 'Відсоток населення в перетині від другої ізохрони';

-- Створюємо індекси
CREATE INDEX IF NOT EXISTS idx_overlaps_affected_pop 
ON avrora.isochrone_overlaps(affected_population DESC);

CREATE INDEX IF NOT EXISTS idx_overlaps_cities 
ON avrora.isochrone_overlaps(city_1, city_2);

CREATE INDEX IF NOT EXISTS idx_overlaps_mode_distance 
ON avrora.isochrone_overlaps(mode, distance_meters);

CREATE INDEX IF NOT EXISTS idx_competitors_significance 
ON avrora.competitors(significance_coefficient);

-- ================================================================
-- ЕТАП 2: СТВОРЕННЯ ФУНКЦІЙ ДЛЯ РОЗРАХУНКІВ
-- ================================================================

-- 2.1 Функція розрахунку населення в ізохроні через H3-10
CREATE OR REPLACE FUNCTION avrora.calculate_isochrone_population(
    p_isochrone_id INTEGER
) RETURNS TABLE (
    total_population INTEGER,
    residential_buildings INTEGER,
    commercial_buildings INTEGER,
    avg_transaction_sum DECIMAL,
    avg_check DECIMAL,
    purchasing_power_index DECIMAL
) AS $$
DECLARE
    v_polygon geometry;
BEGIN
    SELECT polygon INTO v_polygon
    FROM avrora.isochrones
    WHERE isochrone_id = p_isochrone_id;
    
    IF v_polygon IS NULL THEN
        RETURN QUERY SELECT 0::INTEGER, 0::INTEGER, 0::INTEGER, 
                           0::DECIMAL, 0::DECIMAL, 0::DECIMAL;
        RETURN;
    END IF;
    
    RETURN QUERY
    WITH h3_10_in_isochrone AS (
        SELECT DISTINCT 
            b.h3_res_10 as h3_10_index,
            b.h3_res_8 as h3_8_index
        FROM osm_ukraine.building_footprints b
        WHERE ST_Within(b.footprint, v_polygon)
        AND b.h3_res_10 IS NOT NULL
    ),
    population_stats AS (
        SELECT 
            COUNT(DISTINCT h10.h3_10_index) as hex_count,
            COALESCE(SUM(b.population_corrected), 0) as total_pop,
            AVG(NULLIF(rbc.total_sum, 0)) as avg_sum,
            AVG(NULLIF(rbc.avg_check_per_client, 0)) as avg_check
        FROM h3_10_in_isochrone h10
        JOIN osm_ukraine.building_footprints b 
            ON b.h3_res_10 = h10.h3_10_index
        LEFT JOIN osm_ukraine.rbc_h3_data rbc 
            ON h10.h3_8_index = rbc.h3_index
    ),
    building_stats AS (
        SELECT 
            COUNT(DISTINCT CASE WHEN building_category = 'residential' 
                               THEN building_id END) as res_buildings,
            COUNT(DISTINCT CASE WHEN building_category = 'commercial' 
                               THEN building_id END) as com_buildings
        FROM osm_ukraine.building_footprints b
        WHERE ST_Within(b.footprint, v_polygon)
    )
    SELECT 
        ps.total_pop::INTEGER as total_population,
        bs.res_buildings::INTEGER as residential_buildings,
        bs.com_buildings::INTEGER as commercial_buildings,
        COALESCE(ps.avg_sum, 0)::DECIMAL as avg_transaction_sum,
        COALESCE(ps.avg_check, 500)::DECIMAL as avg_check,
        CASE 
            WHEN ps.avg_check > 0 THEN 
                LEAST(100, (ps.avg_check / 1000.0 * 100))::DECIMAL
            ELSE 50.0 
        END as purchasing_power_index
    FROM population_stats ps, building_stats bs;
END;
$$ LANGUAGE plpgsql;

-- 2.2 Функція розрахунку населення в полігоні перетину
CREATE OR REPLACE FUNCTION avrora.calculate_overlap_population(
    p_overlap_polygon geometry
) RETURNS TABLE (
    total_population INTEGER,
    buildings_count INTEGER,
    avg_income_index DECIMAL
) AS $$
BEGIN
    IF p_overlap_polygon IS NULL THEN
        RETURN QUERY SELECT 0::INTEGER, 0::INTEGER, 0::DECIMAL;
        RETURN;
    END IF;
    
    RETURN QUERY
    WITH h3_10_in_overlap AS (
        SELECT DISTINCT 
            b.h3_res_10 as h3_10_index,
            b.h3_res_8 as h3_8_index,
            b.population_corrected
        FROM osm_ukraine.building_footprints b
        WHERE ST_Within(b.footprint, p_overlap_polygon)
        AND b.h3_res_10 IS NOT NULL
    )
    SELECT 
        COALESCE(SUM(h.population_corrected), 0)::INTEGER as total_population,
        COUNT(DISTINCT h.h3_10_index)::INTEGER as buildings_count,
        COALESCE(AVG(
            CASE 
                WHEN rbc.avg_check_per_client > 1500 THEN 1.5
                WHEN rbc.avg_check_per_client > 1000 THEN 1.2
                WHEN rbc.avg_check_per_client > 500 THEN 1.0
                ELSE 0.8
            END
        ), 1.0)::DECIMAL as avg_income_index
    FROM h3_10_in_overlap h
    LEFT JOIN osm_ukraine.rbc_h3_data rbc 
        ON h.h3_8_index = rbc.h3_index;
END;
$$ LANGUAGE plpgsql;

-- ================================================================
-- ЕТАП 3: ПРОЦЕДУРА РОЗРАХУНКУ ПЕРЕТИНІВ (ВИПРАВЛЕНА)
-- ================================================================
CREATE OR REPLACE PROCEDURE avrora.calculate_isochrone_overlaps()
LANGUAGE plpgsql
AS $$
DECLARE
    v_count_walk INTEGER := 0;
    v_count_drive INTEGER := 0;
    v_count_total INTEGER := 0;
    v_total_isochrones INTEGER;
BEGIN
    RAISE NOTICE '🔄 Початок розрахунку перетинів ізохрон...';
    
    SELECT COUNT(*) INTO v_total_isochrones FROM avrora.isochrones WHERE is_current = true;
    RAISE NOTICE '📊 Знайдено % активних ізохрон', v_total_isochrones;
    
    TRUNCATE TABLE avrora.isochrone_overlaps;
    RAISE NOTICE '🗑️ Таблиця перетинів очищена';
    
    RAISE NOTICE '🚀 Розрахунок перетинів з посиленою фільтрацією...';
    
    INSERT INTO avrora.isochrone_overlaps (
        isochrone_id_1,
        isochrone_id_2,
        overlap_type,
        overlap_polygon,
        overlap_area_sqm,
        overlap_percent_1,
        overlap_percent_2,
        isochrone_polygon_1,
        isochrone_polygon_2,
        city_1,
        city_2,
        mode,
        distance_meters
    )
    SELECT 
        i1.isochrone_id,
        i2.isochrone_id,
        CASE 
            WHEN i1.entity_type = 'store' AND i2.entity_type = 'store' 
                AND i1.entity_id != i2.entity_id
            THEN 'cannibalization'
            WHEN (i1.entity_type = 'store' AND i2.entity_type = 'competitor') 
                OR (i1.entity_type = 'competitor' AND i2.entity_type = 'store')
            THEN 'competition'
        END as overlap_type,
        ST_Intersection(i1.polygon, i2.polygon) as overlap_polygon,
        ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) as overlap_area_sqm,
        (ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
         NULLIF(ST_Area(i1.polygon::geography), 0) * 100)::DECIMAL(5,2) as overlap_percent_1,
        (ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
         NULLIF(ST_Area(i2.polygon::geography), 0) * 100)::DECIMAL(5,2) as overlap_percent_2,
        i1.polygon as isochrone_polygon_1,
        i2.polygon as isochrone_polygon_2,
        COALESCE(s1.city, c1.settlement_name) as city_1,
        COALESCE(s2.city, c2.settlement_name) as city_2,
        i1.mode,
        i1.distance_meters
    FROM avrora.isochrones i1
    JOIN avrora.isochrones i2 
        ON i1.isochrone_id < i2.isochrone_id
        AND i1.mode = i2.mode
        AND i1.distance_meters = i2.distance_meters
        AND ST_Intersects(i1.polygon, i2.polygon)
    LEFT JOIN avrora.stores s1 
        ON i1.entity_type = 'store' AND i1.entity_id = s1.store_id
    LEFT JOIN avrora.competitors c1 
        ON i1.entity_type = 'competitor' AND i1.entity_id = c1.competitor_id
    LEFT JOIN avrora.stores s2 
        ON i2.entity_type = 'store' AND i2.entity_id = s2.store_id
    LEFT JOIN avrora.competitors c2 
        ON i2.entity_type = 'competitor' AND i2.entity_id = c2.competitor_id
    WHERE i1.is_current = true 
      AND i2.is_current = true
      AND (
          (i1.entity_type = 'store' AND i2.entity_type = 'store' AND i1.entity_id != i2.entity_id)
          OR 
          (i1.entity_type = 'store' AND i2.entity_type = 'competitor')
          OR 
          (i1.entity_type = 'competitor' AND i2.entity_type = 'store')
      )
      -- ФІЛЬТРАЦІЯ ДЛЯ DRIVE (БЕЗ ПОМИЛКОВОЇ УМОВИ)
      AND (
          i1.mode = 'walk'
          OR 
          (i1.mode = 'drive' 
           AND COALESCE(s1.city, c1.settlement_name) = COALESCE(s2.city, c2.settlement_name)
           AND COALESCE(s1.city, c1.settlement_name) IS NOT NULL
          )
      )
      -- Мінімальна площа перетину
      AND ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) > 
          CASE 
              WHEN i1.mode = 'walk' THEN 100
              WHEN i1.mode = 'drive' THEN 50000  -- 5 га
              ELSE 100
          END
      -- Мінімальний відсоток перетину
      AND (
          i1.mode = 'walk'
          OR (
              i1.mode = 'drive' 
              AND ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
                  NULLIF(ST_Area(i1.polygon::geography), 0) > 0.10
          )
      );
    
    GET DIAGNOSTICS v_count_total = ROW_COUNT;
    
    SELECT COUNT(*) INTO v_count_walk 
    FROM avrora.isochrone_overlaps 
    WHERE mode = 'walk';
    
    SELECT COUNT(*) INTO v_count_drive 
    FROM avrora.isochrone_overlaps 
    WHERE mode = 'drive';
    
    RAISE NOTICE '✅ Розраховано % перетинів:', v_count_total;
    RAISE NOTICE '   🚶 Пішохідні (walk): %', v_count_walk;
    RAISE NOTICE '   🚗 Автомобільні (drive): %', v_count_drive;
    
    RAISE NOTICE '📊 Деталізація:';
    RAISE NOTICE '   Канібалізація walk: %', 
        (SELECT COUNT(*) FROM avrora.isochrone_overlaps 
         WHERE overlap_type = 'cannibalization' AND mode = 'walk');
    RAISE NOTICE '   Канібалізація drive: %', 
        (SELECT COUNT(*) FROM avrora.isochrone_overlaps 
         WHERE overlap_type = 'cannibalization' AND mode = 'drive');
    RAISE NOTICE '   Конкуренція walk: %', 
        (SELECT COUNT(*) FROM avrora.isochrone_overlaps 
         WHERE overlap_type = 'competition' AND mode = 'walk');
    RAISE NOTICE '   Конкуренція drive: %', 
        (SELECT COUNT(*) FROM avrora.isochrone_overlaps 
         WHERE overlap_type = 'competition' AND mode = 'drive');
    
    COMMIT;
END;
$$;

-- ================================================================
-- ЕТАП 4: ПРОЦЕДУРА РОЗРАХУНКУ МЕТРИК ПЕРЕТИНІВ
-- ================================================================

CREATE OR REPLACE PROCEDURE avrora.calculate_overlap_metrics()
AS $$
DECLARE
    v_overlap RECORD;
    v_pop1 RECORD;
    v_pop2 RECORD;
    v_pop_overlap RECORD;
    v_significance_coef DECIMAL;
    v_impact_coef DECIMAL;
    v_processed INTEGER := 0;
    v_total INTEGER;
    v_start_time TIMESTAMP;
BEGIN
    v_start_time := clock_timestamp();
    
    SELECT COUNT(*) INTO v_total FROM avrora.isochrone_overlaps;
    
    RAISE NOTICE '🚀 Початок розрахунку метрик для % перетинів', v_total;
    RAISE NOTICE '📍 Використовуємо H3-10 для точного розрахунку населення';
    
    IF v_total = 0 THEN
        RAISE NOTICE '⚠️ Таблиця перетинів порожня! Спочатку запустіть CALL avrora.calculate_isochrone_overlaps();';
        RETURN;
    END IF;
    
    FOR v_overlap IN 
        SELECT 
            o.*,
            i1.entity_type as entity1_type,
            i1.entity_id as entity1_id,
            i2.entity_type as entity2_type,
            i2.entity_id as entity2_id
        FROM avrora.isochrone_overlaps o
        JOIN avrora.isochrones i1 ON o.isochrone_id_1 = i1.isochrone_id
        JOIN avrora.isochrones i2 ON o.isochrone_id_2 = i2.isochrone_id
    LOOP
        SELECT * INTO v_pop1
        FROM avrora.calculate_isochrone_population(v_overlap.isochrone_id_1);
        
        SELECT * INTO v_pop2
        FROM avrora.calculate_isochrone_population(v_overlap.isochrone_id_2);
        
        SELECT * INTO v_pop_overlap
        FROM avrora.calculate_overlap_population(v_overlap.overlap_polygon);
        
        IF v_overlap.overlap_type = 'cannibalization' THEN
            v_significance_coef := 1.0;
            v_impact_coef := CASE 
                WHEN v_overlap.mode = 'walk' THEN 0.3
                WHEN v_overlap.mode = 'drive' THEN 0.15
                ELSE 0.2
            END;
        ELSE
            SELECT c.significance_coefficient 
            INTO v_significance_coef
            FROM avrora.competitors c
            WHERE c.competitor_id = 
                CASE 
                    WHEN v_overlap.entity1_type = 'competitor' THEN v_overlap.entity1_id
                    ELSE v_overlap.entity2_id
                END;
            
            v_impact_coef := CASE 
                WHEN v_overlap.mode = 'walk' THEN 0.2
                WHEN v_overlap.mode = 'drive' THEN 0.1
                ELSE 0.15
            END * COALESCE(v_significance_coef, 1.0);
            
            v_impact_coef := LEAST(v_impact_coef, 0.5);
        END IF;
        
        UPDATE avrora.isochrone_overlaps
        SET 
            population_estimate = v_pop_overlap.total_population,
            buildings_count = v_pop_overlap.buildings_count,
            population_percent_1 = CASE 
                WHEN v_pop1.total_population > 0 
                THEN (v_pop_overlap.total_population * 100.0 / v_pop1.total_population)::DECIMAL(5,2)
                ELSE 0 
            END,
            population_percent_2 = CASE 
                WHEN v_pop2.total_population > 0 
                THEN (v_pop_overlap.total_population * 100.0 / v_pop2.total_population)::DECIMAL(5,2)
                ELSE 0 
            END,
            significance_coefficient = v_significance_coef,
            impact_coefficient = v_impact_coef,
            affected_population = (v_pop_overlap.total_population * v_impact_coef)::INTEGER,
            avg_transaction_sum = GREATEST(v_pop1.avg_transaction_sum, v_pop2.avg_transaction_sum),
            purchasing_power_index = GREATEST(v_pop1.purchasing_power_index, v_pop2.purchasing_power_index),
            calculated_at = NOW()
        WHERE overlap_id = v_overlap.overlap_id;
        
        v_processed := v_processed + 1;
        
        IF v_processed % 100 = 0 THEN
            RAISE NOTICE '⏳ Оброблено % з % перетинів (%.1f%%)', 
                v_processed, v_total, (v_processed * 100.0 / v_total);
            COMMIT;
        END IF;
    END LOOP;
    
    RAISE NOTICE '✅ Завершено за %s. Оброблено % перетинів', 
        (clock_timestamp() - v_start_time), v_processed;
END;
$$ LANGUAGE plpgsql;

-- ================================================================
-- ЕТАП 5: АНАЛІТИЧНІ ПРЕДСТАВЛЕННЯ (ПОВНИЙ НАБІР)
-- ================================================================

-- 5.1 Детальний аналіз впливу на кожен магазин
DROP VIEW IF EXISTS avrora.v_store_impact_analysis CASCADE;
CREATE VIEW avrora.v_store_impact_analysis AS
WITH store_population AS (
    SELECT 
        s.store_id,
        s.name,
        s.city,
        s.format,
        s.oblast_name,
        i.isochrone_id,
        i.mode,
        i.distance_meters,
        pop.total_population,
        pop.purchasing_power_index
    FROM avrora.stores s
    JOIN avrora.isochrones i 
        ON s.store_id = i.entity_id 
        AND i.entity_type = 'store'
        AND i.is_current = true
    CROSS JOIN LATERAL avrora.calculate_isochrone_population(i.isochrone_id) pop
    WHERE s.is_active = true
),
impact_metrics AS (
    SELECT 
        sp.store_id,
        sp.mode,
        sp.distance_meters,
        SUM(CASE 
            WHEN o.overlap_type = 'cannibalization' 
            THEN o.affected_population 
        END) as cannibalized_population,
        COUNT(DISTINCT CASE 
            WHEN o.overlap_type = 'cannibalization' 
            THEN CASE 
                WHEN o.isochrone_id_1 = sp.isochrone_id THEN o.isochrone_id_2
                ELSE o.isochrone_id_1
            END
        END) as cannibalization_count,
        SUM(CASE 
            WHEN o.overlap_type = 'competition' 
            THEN o.affected_population 
        END) as competitive_losses,
        COUNT(DISTINCT CASE 
            WHEN o.overlap_type = 'competition' 
            THEN CASE 
                WHEN i_comp.entity_type = 'competitor' THEN i_comp.entity_id
            END
        END) as competitor_count,
        AVG(CASE 
            WHEN o.overlap_type = 'competition' 
            THEN o.significance_coefficient 
        END) as avg_competitor_significance
    FROM store_population sp
    LEFT JOIN avrora.isochrone_overlaps o 
        ON sp.isochrone_id IN (o.isochrone_id_1, o.isochrone_id_2)
    LEFT JOIN avrora.isochrones i_comp 
        ON (o.isochrone_id_1 = i_comp.isochrone_id OR o.isochrone_id_2 = i_comp.isochrone_id)
        AND i_comp.entity_type = 'competitor'
    GROUP BY sp.store_id, sp.mode, sp.distance_meters
)
SELECT 
    sp.store_id,
    sp.name,
    sp.city,
    sp.format,
    sp.oblast_name,
    sp.mode as isochrone_mode,
    sp.distance_meters,
    sp.total_population,
    sp.purchasing_power_index,
    COALESCE(im.cannibalized_population, 0) as cannibalized_population,
    COALESCE(im.cannibalization_count, 0) as cannibalization_stores,
    COALESCE(im.competitive_losses, 0) as competitive_losses,
    COALESCE(im.competitor_count, 0) as competitors_nearby,
    COALESCE(im.avg_competitor_significance, 0) as avg_competitor_strength,
    sp.total_population - 
        COALESCE(im.cannibalized_population, 0) - 
        COALESCE(im.competitive_losses, 0) as effective_population,
    CASE 
        WHEN sp.total_population > 0 THEN
            ROUND((COALESCE(im.cannibalized_population, 0) * 100.0 / sp.total_population)::NUMERIC, 2)
        ELSE 0
    END as cannibalization_percent,
    CASE 
        WHEN sp.total_population > 0 THEN
            ROUND((COALESCE(im.competitive_losses, 0) * 100.0 / sp.total_population)::NUMERIC, 2)
        ELSE 0
    END as competition_percent,
    CASE 
        WHEN sp.total_population > 0 THEN
            ROUND(((sp.total_population - 
                   COALESCE(im.cannibalized_population, 0) - 
                   COALESCE(im.competitive_losses, 0)) * 100.0 / 
                  sp.total_population)::NUMERIC, 2)
        ELSE 0
    END as location_score
FROM store_population sp
LEFT JOIN impact_metrics im 
    ON sp.store_id = im.store_id 
    AND sp.mode = im.mode 
    AND sp.distance_meters = im.distance_meters;

-- 5.2 Топ проблемних перетинів
DROP VIEW IF EXISTS avrora.v_critical_overlaps CASCADE;
CREATE VIEW avrora.v_critical_overlaps AS
SELECT 
    o.overlap_id,
    o.overlap_type,
    o.mode,
    o.distance_meters,
    CASE 
        WHEN i1.entity_type = 'store' THEN s1.name
        ELSE c1.name
    END as entity1_name,
    CASE 
        WHEN i1.entity_type = 'store' THEN s1.city
        ELSE c1.settlement_name
    END as entity1_city,
    CASE 
        WHEN i2.entity_type = 'store' THEN s2.name
        ELSE c2.name
    END as entity2_name,
    CASE 
        WHEN i2.entity_type = 'store' THEN s2.city
        ELSE c2.settlement_name
    END as entity2_city,
    o.overlap_area_sqm / 10000 as overlap_area_hectares,
    o.overlap_percent_1 as area_percent_from_1,
    o.overlap_percent_2 as area_percent_from_2,
    o.population_estimate,
    o.population_percent_1 as population_percent_from_1,
    o.population_percent_2 as population_percent_from_2,
    o.significance_coefficient,
    o.impact_coefficient,
    o.affected_population,
    o.purchasing_power_index,
    CASE 
        WHEN o.affected_population > 3000 THEN 'CRITICAL'
        WHEN o.affected_population > 1500 THEN 'HIGH'
        WHEN o.affected_population > 500 THEN 'MEDIUM'
        ELSE 'LOW'
    END as impact_level,
    (o.affected_population * 300 * 12)::BIGINT as estimated_yearly_loss_uah
FROM avrora.isochrone_overlaps o
JOIN avrora.isochrones i1 ON o.isochrone_id_1 = i1.isochrone_id
JOIN avrora.isochrones i2 ON o.isochrone_id_2 = i2.isochrone_id
LEFT JOIN avrora.stores s1 ON i1.entity_id = s1.store_id AND i1.entity_type = 'store'
LEFT JOIN avrora.competitors c1 ON i1.entity_id = c1.competitor_id AND i1.entity_type = 'competitor'
LEFT JOIN avrora.stores s2 ON i2.entity_id = s2.store_id AND i2.entity_type = 'store'
LEFT JOIN avrora.competitors c2 ON i2.entity_id = c2.competitor_id AND i2.entity_type = 'competitor'
WHERE o.affected_population > 0
ORDER BY o.affected_population DESC;

-- 5.3 Зведена статистика по областях
DROP VIEW IF EXISTS avrora.v_oblast_competition_summary CASCADE;
CREATE VIEW avrora.v_oblast_competition_summary AS
SELECT 
    s.oblast_name,
    COUNT(DISTINCT s.store_id) as our_stores,
    COUNT(DISTINCT c.competitor_id) as total_competitors,
    COUNT(DISTINCT c.competitor_id) as equal_competitors,
    AVG(c.significance_coefficient) as avg_competitor_strength,
    SUM(sia.cannibalized_population) as total_cannibalized,
    SUM(sia.competitive_losses) as total_competitive_losses,
    AVG(sia.location_score) as avg_location_score,
    SUM(sia.effective_population) as total_effective_population
FROM avrora.stores s
LEFT JOIN avrora.competitors c 
    ON s.oblast_name = c.oblast_name AND c.is_active = true
LEFT JOIN avrora.v_store_impact_analysis sia 
    ON s.store_id = sia.store_id AND sia.isochrone_mode = 'walk'
WHERE s.is_active = true
GROUP BY s.oblast_name
ORDER BY our_stores DESC;

-- 5.4 Аналіз канібалізації по містах
DROP VIEW IF EXISTS avrora.v_city_cannibalization CASCADE;
CREATE VIEW avrora.v_city_cannibalization AS
SELECT 
    city_1 as city,
    mode,
    COUNT(*) as cannibalization_pairs,
    AVG(overlap_percent_1) as avg_overlap_percent,
    AVG(population_estimate) as avg_population_overlap,
    SUM(affected_population) as total_affected_population,
    MAX(affected_population) as max_affected_population
FROM avrora.isochrone_overlaps
WHERE overlap_type = 'cannibalization'
GROUP BY city_1, mode
ORDER BY total_affected_population DESC;

-- 5.5 Детальний аналіз конкуренції по брендах
DROP VIEW IF EXISTS avrora.v_competitor_brand_analysis CASCADE;
CREATE VIEW avrora.v_competitor_brand_analysis AS
SELECT 
    c.brand as competitor_brand,
    o.mode,
    COUNT(DISTINCT o.overlap_id) as overlap_count,
    COUNT(DISTINCT s.store_id) as affected_stores,
    AVG(o.population_estimate) as avg_population_overlap,
    SUM(o.affected_population) as total_affected_population,
    AVG(o.overlap_percent_1) as avg_area_overlap_percent
FROM avrora.isochrone_overlaps o
JOIN avrora.isochrones i_comp 
    ON (o.isochrone_id_1 = i_comp.isochrone_id OR o.isochrone_id_2 = i_comp.isochrone_id)
    AND i_comp.entity_type = 'competitor'
JOIN avrora.competitors c 
    ON i_comp.entity_id = c.competitor_id
JOIN avrora.isochrones i_store 
    ON (o.isochrone_id_1 = i_store.isochrone_id OR o.isochrone_id_2 = i_store.isochrone_id)
    AND i_store.entity_type = 'store'
JOIN avrora.stores s 
    ON i_store.entity_id = s.store_id
WHERE o.overlap_type = 'competition'
GROUP BY c.brand, o.mode
ORDER BY total_affected_population DESC;

-- ================================================================
-- ЕТАП ЗАПУСКИ
-- ================================================================

CALL avrora.calculate_isochrone_overlaps();
CALL avrora.calculate_overlap_metrics();
-- ================================================================
-- ЕТАП 6: КОНТРОЛЬНІ ЗАПИТИ
-- ================================================================

-- 6.1 Загальна статистика перетинів
SELECT 
    mode,
    distance_meters,
    overlap_type,
    COUNT(*) as count,
    AVG(overlap_area_sqm/10000) as avg_overlap_hectares,
    AVG(population_estimate) as avg_population
FROM avrora.isochrone_overlaps
GROUP BY mode, distance_meters, overlap_type
ORDER BY mode, distance_meters, overlap_type;

-- 6.2 Топ-10 магазинів з найбільшими втратами (walk)
SELECT 
    name,
    city,
    total_population,
    cannibalized_population,
    competitive_losses,
    effective_population,
    location_score
FROM avrora.v_store_impact_analysis
WHERE isochrone_mode = 'walk'
ORDER BY (cannibalized_population + competitive_losses) DESC
LIMIT 10;

-- 6.3 Критичні канібалізації
SELECT 
    overlap_type,
    mode,
    entity1_name,
    entity1_city,
    entity2_name,
    entity2_city,
    population_percent_from_1,
    population_percent_from_2,
    affected_population,
    impact_level,
    estimated_yearly_loss_uah
FROM avrora.v_critical_overlaps
WHERE impact_level IN ('CRITICAL', 'HIGH')
  AND overlap_type = 'cannibalization'
ORDER BY affected_population DESC
LIMIT 20;

-- 6.4 Аналіз по містах
SELECT * FROM avrora.v_city_cannibalization
WHERE mode = 'walk'
LIMIT 10;

-- 6.5 Аналіз конкуренції по брендах
SELECT * FROM avrora.v_competitor_brand_analysis;

-- ================================================================
-- ІНСТРУКЦІЯ ПО ЗАПУСКУ
-- ================================================================
/*
ПОСЛІДОВНІСТЬ ВИКОНАННЯ:

1. Виконайте всі етапи 1-5 послідовно

2. Перерахуйте перетини:
   CALL avrora.calculate_isochrone_overlaps();
   
3. Розрахуйте метрики населення:
   CALL avrora.calculate_overlap_metrics();
   
4. Перевірте результати контрольними запитами (ЕТАП 6)

ПРОБЛЕМА З DRIVE:
Якщо все ще забагато перетинів drive, додайте в процедуру 
додаткову умову з використанням геометричної відстані між центрами
або підвищіть мінімальну площу перетину до 100000 м² (10 га)
*/

-- ================================================================
-- КІНЕЦЬ СКРИПТА
-- ================================================================