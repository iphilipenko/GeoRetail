-- =====================================================
-- Очищення даних районів - фінальна версія
-- Файл: sql/07_clean_raion_data.sql
-- =====================================================

-- 1. Початкова статистика
SELECT 
    'До очищення' as status,
    COUNT(*) as total_raions,
    COUNT(DISTINCT parent_id) as oblasti_with_raions
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 2. Видаляємо райони інших країн за назвами
DELETE FROM osm_ukraine.admin_boundaries 
WHERE admin_level = 6
AND (
    name LIKE '%повіт%' 
    OR name LIKE '%powiat%'
    OR name LIKE '%медьє%'
    OR name LIKE '%megye%'
    OR name LIKE '%міський округ%'
    OR name LIKE '%городской округ%'
    OR name_uk LIKE '%повіт%'
    OR name_uk LIKE '%медьє%'
    OR name_uk LIKE '%міський округ%'
    OR name LIKE '%район' AND name LIKE 'Берестейський%'
    OR name LIKE '%район' AND name LIKE 'Дрогичинський%'
    OR name LIKE '%район' AND name LIKE 'Брагінський%'
    OR name LIKE '%район' AND name LIKE 'Гомєльський%'
    OR name LIKE '%район' AND name LIKE 'Добруський%'
    OR name LIKE '%район' AND name LIKE 'Лоєвський%'
    OR name LIKE '%район' AND name LIKE 'Наровлянський%'
    OR name LIKE '%район' AND name LIKE 'Єльський%'
    OR name LIKE '%район' AND name LIKE 'Лєльчицький%'
    OR name LIKE '%район' AND name LIKE 'Пінський%'
    OR name LIKE '%район' AND name LIKE 'Хойніцький%'
    OR name LIKE 'Білгородський%'
    OR name LIKE 'Валуйський%'
    OR name LIKE 'Борисівський%'
    OR name LIKE 'Волоконівський%'
    OR name LIKE 'Грайворонський%'
    OR name LIKE 'Коренівський%'
    OR name LIKE 'Білівський%'
    OR name LIKE 'Ракитянський%'
    OR name LIKE 'Вейделівський%'
    OR name LIKE 'Климівський%'
);

-- 3. Видаляємо райони які не потрапляють повністю в жодну українську область
DELETE FROM osm_ukraine.admin_boundaries r
WHERE r.admin_level = 6 
AND NOT EXISTS (
    SELECT 1 
    FROM osm_ukraine.admin_boundaries o
    WHERE o.admin_level = 4 
    AND ST_Within(r.geometry, o.geometry)
);

-- 4. Оновлюємо parent_id через ST_Within для точності
UPDATE osm_ukraine.admin_boundaries r
SET parent_id = (
    SELECT o.id 
    FROM osm_ukraine.admin_boundaries o
    WHERE o.admin_level = 4 
    AND ST_Within(r.geometry, o.geometry)
    ORDER BY ST_Area(ST_Intersection(r.geometry, o.geometry)) DESC
    LIMIT 1
),
updated_at = CURRENT_TIMESTAMP
WHERE r.admin_level = 6
AND parent_id IS NOT NULL;

-- 5. Логування результату
INSERT INTO osm_ukraine.admin_processing_log (process_name, status, message, details)
SELECT 
    'clean_raions',
    'completed',
    'Очищення районів завершено',
    jsonb_build_object(
        'total_raions', COUNT(*),
        'oblasti_with_raions', COUNT(DISTINCT parent_id)
    )
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 6. Фінальна статистика
SELECT 
    'Після очищення' as status,
    COUNT(*) as total_raions,
    COUNT(DISTINCT parent_id) as oblasti_with_raions,
    MIN(area_km2) as min_area,
    MAX(area_km2) as max_area,
    ROUND(AVG(area_km2)::numeric, 2) as avg_area
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;

-- 7. Розподіл районів по областях
SELECT 
    p.name_uk as "Область",
    COUNT(r.id) as "Кількість районів",
    STRING_AGG(r.name_uk, ', ' ORDER BY r.name_uk) as "Райони"
FROM osm_ukraine.admin_boundaries r
JOIN osm_ukraine.admin_boundaries p ON r.parent_id = p.id
WHERE r.admin_level = 6
GROUP BY p.name_uk
ORDER BY COUNT(r.id) DESC, p.name_uk;

-- 8. Перевірка - чи є 136 районів як після реформи 2020
SELECT 
    CASE 
        WHEN COUNT(*) BETWEEN 130 AND 140 THEN 'OK - відповідає реформі 2020'
        ELSE 'Перевірити - очікувалось ~136 районів'
    END as check_result,
    COUNT(*) as actual_count,
    136 as expected_count
FROM osm_ukraine.admin_boundaries
WHERE admin_level = 6;