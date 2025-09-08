-- =====================================================
-- ЕТАП 5: Створення функцій та тригерів
-- =====================================================

-- Функція для автоматичного заповнення гео-атрибутів
CREATE OR REPLACE FUNCTION avrora.fill_geo_attributes() 
RETURNS TRIGGER AS $$
BEGIN
    -- Створюємо/оновлюємо geometry point
    NEW.geometry := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
    
    -- Генеруємо H3 індекси різних резолюцій
    -- Використовуємо правильну функцію з h3 extension
    NEW.h3_7 := h3_lat_lng_to_cell(NEW.lat, NEW.lon, 7)::text;
    NEW.h3_8 := h3_lat_lng_to_cell(NEW.lat, NEW.lon, 8)::text;
    NEW.h3_9 := h3_lat_lng_to_cell(NEW.lat, NEW.lon, 9)::text;
    NEW.h3_10 := h3_lat_lng_to_cell(NEW.lat, NEW.lon, 10)::text;
    
    -- Отримуємо адміністративні прив'язки з таблиці h3_admin_mapping
    -- Використовуємо H3-8 для прив'язки
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'osm_ukraine' 
        AND table_name = 'h3_admin_mapping'
    ) THEN
        SELECT 
            oblast_id, oblast_name, 
            raion_id, raion_name,
            gromada_id, gromada_name, 
            settlement_id, settlement_name
        INTO 
            NEW.oblast_id, NEW.oblast_name,
            NEW.raion_id, NEW.raion_name,
            NEW.gromada_id, NEW.gromada_name,
            NEW.settlement_id, NEW.settlement_name
        FROM osm_ukraine.h3_admin_mapping
        WHERE h3_index = NEW.h3_8 
          AND h3_resolution = 8
        LIMIT 1;
    END IF;
    
    -- Оновлюємо updated_at якщо це UPDATE
    IF TG_OP = 'UPDATE' THEN
        NEW.updated_at := CURRENT_TIMESTAMP;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION avrora.fill_geo_attributes() IS 'Автоматичне заповнення H3 індексів та адміністративних прив''язок';

-- =====================================================
-- Тригери для автоматичного заповнення
-- =====================================================

-- Тригер для таблиці stores
DROP TRIGGER IF EXISTS trg_avrora_stores_geo ON avrora.stores;
CREATE TRIGGER trg_avrora_stores_geo 
    BEFORE INSERT OR UPDATE OF lat, lon 
    ON avrora.stores
    FOR EACH ROW 
    EXECUTE FUNCTION avrora.fill_geo_attributes();

-- Тригер для таблиці competitors
DROP TRIGGER IF EXISTS trg_avrora_competitors_geo ON avrora.competitors;
CREATE TRIGGER trg_avrora_competitors_geo 
    BEFORE INSERT OR UPDATE OF lat, lon 
    ON avrora.competitors
    FOR EACH ROW 
    EXECUTE FUNCTION avrora.fill_geo_attributes();

-- =====================================================
-- Функція для розрахунку перетинів ізохрон
-- =====================================================

CREATE OR REPLACE FUNCTION avrora.calculate_isochrone_overlaps(
    p_isochrone_id INT,
    p_calculate_metrics BOOLEAN DEFAULT FALSE
) RETURNS TABLE (
    overlaps_count INT,
    cannibalization_count INT,
    competition_count INT
) AS $$
DECLARE
    v_entity_type VARCHAR(20);
    v_entity_id INT;
    v_mode VARCHAR(20);
    v_distance INT;
    v_overlaps_count INT := 0;
    v_cannibalization_count INT := 0;
    v_competition_count INT := 0;
BEGIN
    -- Отримуємо параметри ізохрони
    SELECT entity_type, entity_id, mode, distance_meters
    INTO v_entity_type, v_entity_id, v_mode, v_distance
    FROM avrora.isochrones
    WHERE isochrone_id = p_isochrone_id;
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Ізохрона з ID % не знайдена', p_isochrone_id;
    END IF;
    
    -- Видаляємо старі перетини для цієї ізохрони
    DELETE FROM avrora.isochrone_overlaps 
    WHERE isochrone_id_1 = p_isochrone_id 
       OR isochrone_id_2 = p_isochrone_id;
    
    -- Розраховуємо нові перетини
    INSERT INTO avrora.isochrone_overlaps (
        isochrone_id_1, 
        isochrone_id_2, 
        overlap_type,
        overlap_polygon, 
        overlap_area_sqm,
        overlap_percent_1, 
        overlap_percent_2
    )
    SELECT 
        LEAST(i1.isochrone_id, i2.isochrone_id),
        GREATEST(i1.isochrone_id, i2.isochrone_id),
        CASE 
            WHEN i1.entity_type = 'store' AND i2.entity_type = 'store' THEN 'cannibalization'
            ELSE 'competition'
        END,
        ST_Intersection(i1.polygon, i2.polygon),
        ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography),
        CASE 
            WHEN i1.isochrone_id < i2.isochrone_id THEN
                ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
                NULLIF(ST_Area(i1.polygon::geography), 0) * 100
            ELSE
                ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
                NULLIF(ST_Area(i2.polygon::geography), 0) * 100
        END,
        CASE 
            WHEN i1.isochrone_id < i2.isochrone_id THEN
                ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
                NULLIF(ST_Area(i2.polygon::geography), 0) * 100
            ELSE
                ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) / 
                NULLIF(ST_Area(i1.polygon::geography), 0) * 100
        END
    FROM avrora.isochrones i1
    CROSS JOIN avrora.isochrones i2
    WHERE i1.isochrone_id = p_isochrone_id
      AND i2.isochrone_id != p_isochrone_id
      AND i2.is_current = true
      AND i2.mode = v_mode
      AND i2.distance_meters = v_distance
      AND ST_Intersects(i1.polygon, i2.polygon)
      AND ST_Area(ST_Intersection(i1.polygon, i2.polygon)::geography) > 1; -- Ігноруємо дуже малі перетини
    
    -- Підраховуємо результати
    SELECT 
        COUNT(*),
        COUNT(*) FILTER (WHERE overlap_type = 'cannibalization'),
        COUNT(*) FILTER (WHERE overlap_type = 'competition')
    INTO v_overlaps_count, v_cannibalization_count, v_competition_count
    FROM avrora.isochrone_overlaps
    WHERE isochrone_id_1 = p_isochrone_id 
       OR isochrone_id_2 = p_isochrone_id;
    
    -- Повертаємо результати
    RETURN QUERY SELECT v_overlaps_count, v_cannibalization_count, v_competition_count;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION avrora.calculate_isochrone_overlaps IS 'Розрахунок перетинів ізохрони з іншими ізохронами';

-- =====================================================
-- Функція для заповнення H3 покриття ізохрони
-- =====================================================

CREATE OR REPLACE FUNCTION avrora.update_isochrone_h3_coverage(
    p_isochrone_id INT
) RETURNS VOID AS $$
DECLARE
    v_polygon geometry;
    v_h3_cells TEXT[];
BEGIN
    -- Отримуємо полігон ізохрони
    SELECT polygon INTO v_polygon
    FROM avrora.isochrones
    WHERE isochrone_id = p_isochrone_id;
    
    IF v_polygon IS NULL THEN
        RAISE EXCEPTION 'Ізохрона % не знайдена', p_isochrone_id;
    END IF;
    
    -- Знаходимо всі H3-8 клітинки, що перетинаються з полігоном
    -- Це приблизний алгоритм - для точного потрібна більш складна логіка
    WITH bounds AS (
        SELECT 
            ST_YMin(v_polygon) as min_lat,
            ST_XMin(v_polygon) as min_lon,
            ST_YMax(v_polygon) as max_lat,
            ST_XMax(v_polygon) as max_lon
    ),
    grid_points AS (
        SELECT 
            lat, lon
        FROM bounds,
        LATERAL generate_series(min_lat, max_lat, 0.01) AS lat,
        LATERAL generate_series(min_lon, max_lon, 0.01) AS lon
        WHERE ST_Contains(v_polygon, ST_SetSRID(ST_MakePoint(lon, lat), 4326))
    )
    SELECT ARRAY_AGG(DISTINCT h3_lat_lng_to_cell(lat, lon, 8)::text)
    INTO v_h3_cells
    FROM grid_points;
    
    -- Оновлюємо покриття
    UPDATE avrora.isochrones
    SET h3_8_coverage = v_h3_cells
    WHERE isochrone_id = p_isochrone_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION avrora.update_isochrone_h3_coverage IS 'Заповнення масиву H3 клітинок, які покриває ізохрона';

-- Перевірка створених об''єктів
SELECT 
    p.proname as function_name,
    pg_get_function_identity_arguments(p.oid) as arguments
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE n.nspname = 'avrora'
ORDER BY p.proname;