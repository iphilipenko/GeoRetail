-- ================================================================
-- Файл: 37_update_transcarpathian_settlements.sql
-- Мета: Оновлення parent_id для 15 сіл Закарпаття
-- Дата: 2025-01-04
-- ================================================================

-- 1. Оновлення parent_id на основі найближчих громад
UPDATE osm_ukraine.admin_boundaries
SET parent_id = CASE
    -- Богданська сільська громада (5 сіл)
    WHEN name_uk IN ('Хмелів', 'Ділове', 'Вільховатий', 'Костилівка', 'Круглий') THEN 2387
    -- Солотвинська селищна громада (9 сіл)
    WHEN name_uk IN ('Кобилецька Поляна', 'Великий Бичків', 'Луг', 'Косівська Поляна', 
                     'Верхнє Водяне', 'Плаюць', 'Водиця', 'Стримба', 'Росішка') THEN 2342
    -- Ясінянська селищна громада (1 село)
    WHEN name_uk = 'Білин' THEN 2343
END
WHERE admin_level = 9
AND parent_id = 92
AND name_uk IN ('Білин', 'Великий Бичків', 'Верхнє Водяне', 'Вільховатий', 'Водиця',
                'Ділове', 'Кобилецька Поляна', 'Косівська Поляна', 'Костилівка', 
                'Круглий', 'Луг', 'Плаюць', 'Росішка', 'Стримба', 'Хмелів');

-- Очікуваний результат: UPDATE 15

-- 2. Перевірка після оновлення
SELECT 
    ab.name_uk as settlement,
    p.name_uk as community,
    p.admin_level
FROM osm_ukraine.admin_boundaries ab
JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 9
AND ab.name_uk IN ('Білин', 'Великий Бичків', 'Хмелів', 'Ділове', 'Вільховатий')
ORDER BY ab.name_uk;

-- 3. ФІНАЛЬНА статистика всіх рівнів
SELECT 
    admin_level,
    COUNT(*) as count,
    STRING_AGG(DISTINCT boundary_type, ', ') as types
FROM osm_ukraine.admin_boundaries
GROUP BY admin_level
ORDER BY admin_level;

-- 4. Фінальна перевірка рівня 9
SELECT 
    'Рівень 9 - ФІНАЛ' as description,
    COUNT(*) as total_settlements,
    COUNT(CASE WHEN p.admin_level = 6 THEN 1 END) as with_community,
    COUNT(CASE WHEN p.admin_level = 5 THEN 1 END) as crimea_to_district,
    COUNT(CASE WHEN p.admin_level NOT IN (5,6) OR p.admin_level IS NULL THEN 1 END) as problems
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 9;