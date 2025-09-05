-- ================================================================
-- Файл: 39_create_hierarchy_views.sql
-- Мета: Створення представлень для зручної роботи з ієрархією
-- Дата: 2025-01-04
-- ================================================================

-- Представлення повної ієрархії
CREATE OR REPLACE VIEW osm_ukraine.v_admin_hierarchy AS
SELECT 
    ab.id,
    ab.osm_id,
    ab.name_uk,
    ab.admin_level,
    ab.population,
    ab.geometry,
    -- Ієрархія назв
    o.name_uk as oblast,
    r.name_uk as raion,
    g.name_uk as gromada,
    -- Ієрархія ID
    CASE 
        WHEN ab.admin_level IN (7,8,9) THEN ab.id
        ELSE NULL
    END as settlement_id
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries g ON 
    (ab.parent_id = g.id AND g.admin_level = 6)
LEFT JOIN osm_ukraine.admin_boundaries r ON 
    (COALESCE(g.parent_id, ab.parent_id) = r.id AND r.admin_level = 5)
LEFT JOIN osm_ukraine.admin_boundaries o ON 
    (COALESCE(r.parent_id, ab.parent_id) = o.id AND o.admin_level = 4);

-- Індекс для швидкого пошуку
CREATE INDEX IF NOT EXISTS idx_admin_hierarchy_search 
ON osm_ukraine.admin_boundaries USING gin(
    to_tsvector('simple', COALESCE(name_uk, ''))
);