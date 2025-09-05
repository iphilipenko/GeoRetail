-- ================================================================
-- Файл: 30_update_community_parent_ids.sql
-- Мета: Оновлення parent_id для 3 громад на правильні райони
-- Дата: 2025-01-04
-- ================================================================

-- 1. Оновлення parent_id для 3 громад
UPDATE osm_ukraine.admin_boundaries
SET parent_id = CASE 
    WHEN osm_id = 11901182 THEN 9991  -- Ясінянська → Рахівський район
    WHEN osm_id = 11957887 THEN 9991  -- Богданська → Рахівський район
    WHEN osm_id = 12030241 THEN 479   -- Чуднівська → Житомирський район
END
WHERE osm_id IN (11901182, 11957887, 12030241);

-- Очікуваний результат: UPDATE 3

-- 2. Перевірка після оновлення
SELECT 
    ab.osm_id,
    ab.name_uk as community_name,
    ab.parent_id,
    p.name_uk as parent_district,
    p.admin_level as parent_level
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.osm_id IN (11957887, 12030241, 11901182)
ORDER BY ab.name_uk;

-- 3. Фінальна перевірка всіх громад (рівень 6)
SELECT 
    'Всі громади' as description,
    COUNT(*) as total,
    COUNT(CASE WHEN p.admin_level = 5 THEN 1 END) as correct_parent,
    COUNT(CASE WHEN p.admin_level != 5 OR p.admin_level IS NULL THEN 1 END) as wrong_parent
FROM osm_ukraine.admin_boundaries ab
LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
WHERE ab.admin_level = 6;

-- Очікуваний результат:
-- 3 громади оновлено
-- Всі 1,465 громад тепер мають parent на рівні 5