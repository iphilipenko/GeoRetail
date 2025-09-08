-- =====================================================
-- ЕТАП 6: Створення матеріалізованих представлень
-- =====================================================

-- Видалення існуючих представлень (для розробки)
-- DROP MATERIALIZED VIEW IF EXISTS avrora.mv_store_cannibalization CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS avrora.mv_store_competition CASCADE;
-- DROP MATERIALIZED VIEW IF EXISTS avrora.mv_h3_8_analytics CASCADE;

-- =====================================================
-- Представлення для аналізу канібалізації між магазинами
-- =====================================================

CREATE MATERIALIZED VIEW avrora.mv_store_cannibalization AS
SELECT 
    -- Магазин 1
    s1.store_id as store_id_1,
    s1.shop_id as shop_id_1,
    s1.name as store_name_1,
    s1.format as format_1,
    s1.city as city_1,
    
    -- Магазин 2
    s2.store_id as store_id_2,
    s2.shop_id as shop_id_2,
    s2.name as store_name_2,
    s2.format as format_2,
    s2.city as city_2,
    
    -- Параметри ізохрон
    i1.mode,
    i1.distance_meters,
    
    -- Метрики перетину
    io.overlap_area_sqm,
    ROUND(io.overlap_percent_1, 2) as store_1_overlap_percent,
    ROUND(io.overlap_percent_2, 2) as store_2_overlap_percent,
    io.population_estimate,
    
    -- Відстань між магазинами
    ROUND(ST_Distance(s1.geometry::geography, s2.geometry::geography)) as direct_distance_meters,
    
    -- Критичність канібалізації
    CASE 
        WHEN io.overlap_percent_1 > 50 OR io.overlap_percent_2 > 50 THEN 'Висока'
        WHEN io.overlap_percent_1 > 25 OR io.overlap_percent_2 > 25 THEN 'Середня'
        ELSE 'Низька'
    END as cannibalization_level
FROM avrora.isochrone_overlaps io
JOIN avrora.isochrones i1 ON io.isochrone_id_1 = i1.isochrone_id
JOIN avrora.isochrones i2 ON io.isochrone_id_2 = i2.isochrone_id
JOIN avrora.stores s1 ON i1.entity_id = s1.store_id AND i1.entity_type = 'store'
JOIN avrora.stores s2 ON i2.entity_id = s2.store_id AND i2.entity_type = 'store'
WHERE io.overlap_type = 'cannibalization'
  AND i1.is_current = true
  AND i2.is_current = true
  AND s1.is_active = true
  AND s2.is_active = true
WITH DATA;

-- Індекси для швидкого доступу
CREATE INDEX idx_mv_avrora_cannibalization_stores 
    ON avrora.mv_store_cannibalization(store_id_1, store_id_2);
CREATE INDEX idx_mv_avrora_cannibalization_level 
    ON avrora.mv_store_cannibalization(cannibalization_level);
CREATE INDEX idx_mv_avrora_cannibalization_distance 
    ON avrora.mv_store_cannibalization(direct_distance_meters);

COMMENT ON MATERIALIZED VIEW avrora.mv_store_cannibalization IS 
    'Аналіз канібалізації між магазинами мережі';

-- =====================================================
-- Представлення для аналізу конкуренції
-- =====================================================

CREATE MATERIALIZED VIEW avrora.mv_store_competition AS
SELECT 
    -- Наш магазин
    s.store_id,
    s.shop_id,
    s.name as store_name,
    s.format as store_format,
    s.city as store_city,
    
    -- Конкурент
    c.competitor_id,
    c.brand as competitor_brand,
    c.name as competitor_name,
    
    -- Параметри ізохрон
    i1.mode,
    i1.distance_meters,
    
    -- Метрики перетину
    io.overlap_area_sqm,
    ROUND(io.overlap_percent_1, 2) as store_coverage_percent,
    ROUND(io.overlap_percent_2, 2) as competitor_coverage_percent,
    io.population_estimate,
    
    -- Відстань між магазинами
    ROUND(ST_Distance(s.geometry::geography, c.geometry::geography)) as direct_distance_meters,
    
    -- Рівень конкуренції
    CASE 
        WHEN io.overlap_percent_1 > 50 THEN 'Критична'
        WHEN io.overlap_percent_1 > 30 THEN 'Висока'
        WHEN io.overlap_percent_1 > 15 THEN 'Середня'
        ELSE 'Низька'
    END as competition_level
FROM avrora.isochrone_overlaps io
JOIN avrora.isochrones i1 ON io.isochrone_id_1 = i1.isochrone_id
JOIN avrora.isochrones i2 ON io.isochrone_id_2 = i2.isochrone_id
JOIN avrora.stores s ON i1.entity_id = s.store_id AND i1.entity_type = 'store'
JOIN avrora.competitors c ON i2.entity_id = c.competitor_id AND i2.entity_type = 'competitor'
WHERE io.overlap_type = 'competition'
  AND i1.is_current = true
  AND i2.is_current = true
  AND s.is_active = true
  AND c.is_active = true
WITH DATA;

-- Індекси
CREATE INDEX idx_mv_avrora_competition_store 
    ON avrora.mv_store_competition(store_id);
CREATE INDEX idx_mv_avrora_competition_brand 
    ON avrora.mv_store_competition(competitor_brand);
CREATE INDEX idx_mv_avrora_competition_level 
    ON avrora.mv_store_competition(competition_level);
CREATE INDEX idx_mv_avrora_competition_distance 
    ON avrora.mv_store_competition(direct_distance_meters);

COMMENT ON MATERIALIZED VIEW avrora.mv_store_competition IS 
    'Аналіз конкуренції між нашими магазинами та конкурентами';

-- =====================================================
-- Агрегована аналітика по H3-8 гексагонах
-- =====================================================

CREATE MATERIALIZED VIEW avrora.mv_h3_8_analytics AS
WITH store_metrics AS (
    SELECT 
        h3_8,
        COUNT(*) as store_count,
        STRING_AGG(DISTINCT format, ', ') as store_formats,
        AVG(population_x10k) as avg_population_x10k,
        SUM(avg_month_n_checks) as total_checks
    FROM avrora.stores
    WHERE is_active = true
      AND h3_8 IS NOT NULL
    GROUP BY h3_8
),
competitor_metrics AS (
    SELECT 
        h3_8,
        COUNT(*) as competitor_count,
        COUNT(DISTINCT brand) as unique_brands,
        STRING_AGG(DISTINCT brand, ', ' ORDER BY brand) as competitor_brands
    FROM avrora.competitors
    WHERE is_active = true
      AND h3_8 IS NOT NULL
    GROUP BY h3_8
)
SELECT 
    COALESCE(s.h3_8, c.h3_8) as h3_8,
    COALESCE(s.store_count, 0) as store_count,
    s.store_formats,
    COALESCE(c.competitor_count, 0) as competitor_count,
    COALESCE(c.unique_brands, 0) as unique_brands,
    c.competitor_brands,
    s.avg_population_x10k,
    s.total_checks,
    -- Індекс конкуренції (співвідношення конкурентів до наших магазинів)
    CASE 
        WHEN COALESCE(s.store_count, 0) = 0 THEN NULL
        ELSE ROUND(COALESCE(c.competitor_count, 0)::numeric / s.store_count, 2)
    END as competition_index,
    -- Категорія гексагону
    CASE 
        WHEN s.store_count > 0 AND c.competitor_count = 0 THEN 'Монополія'
        WHEN s.store_count > 0 AND c.competitor_count > 0 THEN 'Конкуренція'
        WHEN s.store_count = 0 AND c.competitor_count > 0 THEN 'Потенціал'
        ELSE 'Порожній'
    END as hex_category
FROM store_metrics s
FULL OUTER JOIN competitor_metrics c ON s.h3_8 = c.h3_8
WITH DATA;

-- Індекси
CREATE INDEX idx_mv_avrora_h3_analytics_hex ON avrora.mv_h3_8_analytics(h3_8);
CREATE INDEX idx_mv_avrora_h3_analytics_category ON avrora.mv_h3_8_analytics(hex_category);
CREATE INDEX idx_mv_avrora_h3_analytics_stores ON avrora.mv_h3_8_analytics(store_count);
CREATE INDEX idx_mv_avrora_h3_analytics_competition ON avrora.mv_h3_8_analytics(competition_index);

COMMENT ON MATERIALIZED VIEW avrora.mv_h3_8_analytics IS 
    'Агрегована аналітика по H3 гексагонах резолюції 8';

-- =====================================================
-- Функція для оновлення всіх матеріалізованих представлень
-- =====================================================

CREATE OR REPLACE FUNCTION avrora.refresh_all_materialized_views()
RETURNS TABLE (
    view_name TEXT,
    refresh_time INTERVAL
) AS $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
BEGIN
    -- Оновлюємо mv_store_cannibalization
    v_start := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY avrora.mv_store_cannibalization;
    v_end := clock_timestamp();
    view_name := 'mv_store_cannibalization';
    refresh_time := v_end - v_start;
    RETURN NEXT;
    
    -- Оновлюємо mv_store_competition
    v_start := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY avrora.mv_store_competition;
    v_end := clock_timestamp();
    view_name := 'mv_store_competition';
    refresh_time := v_end - v_start;
    RETURN NEXT;
    
    -- Оновлюємо mv_h3_8_analytics
    v_start := clock_timestamp();
    REFRESH MATERIALIZED VIEW CONCURRENTLY avrora.mv_h3_8_analytics;
    v_end := clock_timestamp();
    view_name := 'mv_h3_8_analytics';
    refresh_time := v_end - v_start;
    RETURN NEXT;
    
    RETURN;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION avrora.refresh_all_materialized_views() IS 
    'Оновлення всіх матеріалізованих представлень схеми avrora';

-- Перевірка створених представлень
SELECT 
    schemaname,
    matviewname,
    hasindexes,
    ispopulated
FROM pg_matviews
WHERE schemaname = 'avrora'
ORDER BY matviewname;