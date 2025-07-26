-- Fix H3 helper function
DROP FUNCTION IF EXISTS generate_h3_grid_for_bbox(DECIMAL, DECIMAL, DECIMAL, DECIMAL, INTEGER);

CREATE OR REPLACE FUNCTION generate_h3_grid_for_bbox(
    min_lat DECIMAL, min_lon DECIMAL, 
    max_lat DECIMAL, max_lon DECIMAL, 
    resolution INTEGER DEFAULT 8
) 
RETURNS TABLE(hex_id TEXT, center_lat DECIMAL, center_lon DECIMAL) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT
        h3_lat_lng_to_cell(POINT(x.lon, x.lat), resolution)::TEXT as hex_id,
        x.lat as center_lat,
        x.lon as center_lon
    FROM (
        SELECT 
            (min_lat + (row_number() OVER() * 0.01))::DECIMAL as lat,
            (min_lon + (row_number() OVER() * 0.01))::DECIMAL as lon
        FROM generate_series(1, 10)
    ) x
    WHERE x.lat <= max_lat AND x.lon <= max_lon;
END;
$$ LANGUAGE plpgsql;