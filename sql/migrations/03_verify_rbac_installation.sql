-- =====================================================
-- Перевірка успішної інсталяції RBAC системи
-- Файл: sql/migrations/03_verify_rbac_installation.sql
-- База даних: georetail
-- =====================================================

-- =====================================================
-- 1. ПЕРЕВІРКА СТВОРЕНИХ ТАБЛИЦЬ
-- =====================================================
SELECT 
    '📊 ТАБЛИЦІ RBAC' as section,
    COUNT(*) as table_count,
    string_agg(table_name, ', ' ORDER BY table_name) as tables
FROM information_schema.tables
WHERE table_schema = 'public'
    AND table_name LIKE 'rbac_%';

-- =====================================================
-- 2. СТАТИСТИКА ПО ТАБЛИЦЯХ
-- =====================================================
SELECT 
    '📈 СТАТИСТИКА' as section,
    'Модулів' as entity,
    COUNT(*) as count
FROM rbac_modules
UNION ALL
SELECT 
    '📈 СТАТИСТИКА',
    'Дозволів',
    COUNT(*)
FROM rbac_permissions
UNION ALL
SELECT 
    '📈 СТАТИСТИКА',
    'Ролей',
    COUNT(*)
FROM rbac_roles
UNION ALL
SELECT 
    '📈 СТАТИСТИКА',
    'Користувачів',
    COUNT(*)
FROM rbac_users
UNION ALL
SELECT 
    '📈 СТАТИСТИКА',
    'Призначених ролей',
    COUNT(*)
FROM rbac_user_roles
ORDER BY entity;

-- =====================================================
-- 3. МОДУЛІ ТА ЇХ ДОЗВОЛИ
-- =====================================================
SELECT 
    m.code as module_code,
    m.name as module_name,
    m.icon,
    COUNT(p.id) as permission_count,
    string_agg(p.code, ', ' ORDER BY p.code) as permissions
FROM rbac_modules m
LEFT JOIN rbac_permissions p ON m.id = p.module_id
GROUP BY m.id, m.code, m.name, m.icon, m.display_order
ORDER BY m.display_order;

-- =====================================================
-- 4. РОЛІ ТА КІЛЬКІСТЬ ДОЗВОЛІВ
-- =====================================================
SELECT 
    r.code as role_code,
    r.name as role_name,
    r.is_system,
    r.max_sessions,
    COUNT(rp.permission_id) as permission_count
FROM rbac_roles r
LEFT JOIN rbac_role_permissions rp ON r.id = rp.role_id
GROUP BY r.id, r.code, r.name, r.is_system, r.max_sessions
ORDER BY permission_count DESC;

-- =====================================================
-- 5. КОРИСТУВАЧІ ТА ЇХ РОЛІ
-- =====================================================
SELECT 
    u.username,
    u.email,
    u.first_name || ' ' || u.last_name as full_name,
    u.department,
    u.is_superuser,
    u.is_active,
    string_agg(r.code, ', ' ORDER BY r.code) as roles
FROM rbac_users u
LEFT JOIN rbac_user_roles ur ON u.id = ur.user_id
LEFT JOIN rbac_roles r ON ur.role_id = r.id
GROUP BY u.id, u.username, u.email, u.first_name, u.last_name, 
         u.department, u.is_superuser, u.is_active
ORDER BY u.username;

-- =====================================================
-- 6. ТЕСТ ФУНКЦІЇ get_user_permissions
-- =====================================================
-- Перевіряємо дозволи для користувача marketing_analyst
WITH test_user AS (
    SELECT id, username FROM rbac_users WHERE username = 'marketing_analyst'
)
SELECT 
    '🔐 ДОЗВОЛИ для ' || tu.username as test_section,
    COUNT(*) as permission_count,
    string_agg(permission_code, ', ' ORDER BY permission_code) as permissions
FROM test_user tu
CROSS JOIN LATERAL get_user_permissions(tu.id) p
GROUP BY tu.username;

-- =====================================================
-- 7. ПЕРЕВІРКА ІНДЕКСІВ
-- =====================================================
SELECT 
    '🗂️ ІНДЕКСИ' as section,
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
    AND tablename LIKE 'rbac_%'
ORDER BY tablename, indexname;

-- =====================================================
-- 8. ПЕРЕВІРКА ТРИГЕРІВ
-- =====================================================
SELECT 
    '⚙️ ТРИГЕРИ' as section,
    event_object_table as table_name,
    trigger_name,
    event_manipulation,
    action_statement
FROM information_schema.triggers
WHERE event_object_schema = 'public'
    AND event_object_table LIKE 'rbac_%'
ORDER BY event_object_table, trigger_name;

-- =====================================================
-- 9. ДЕТАЛЬНА ПЕРЕВІРКА ДОЗВОЛІВ ДЛЯ КОЖНОЇ РОЛІ
-- =====================================================
SELECT 
    '📋 МАТРИЦЯ ДОЗВОЛІВ' as section,
    r.code as role,
    m.code as module,
    COUNT(p.id) as permissions,
    string_agg(
        REPLACE(p.code, m.code || '.', ''), 
        ', ' ORDER BY p.code
    ) as permission_list
FROM rbac_roles r
CROSS JOIN rbac_modules m
LEFT JOIN rbac_role_permissions rp ON r.id = rp.role_id
LEFT JOIN rbac_permissions p ON rp.permission_id = p.id AND p.module_id = m.id
WHERE r.code IN ('viewer', 'marketing_analyst', 'expansion_manager')
GROUP BY r.code, m.code, m.display_order
HAVING COUNT(p.id) > 0
ORDER BY r.code, m.display_order;

-- =====================================================
-- 10. QUICK TEST - LOGIN SIMULATION
-- =====================================================
-- Симуляція перевірки логіну для тестового користувача
SELECT 
    '🔑 ТЕСТ ЛОГІНУ' as test,
    u.username,
    u.email,
    CASE 
        WHEN u.password_hash = '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO' 
        THEN '✅ Password hash відповідає Test123!'
        ELSE '❌ Password hash не відповідає'
    END as password_check,
    u.is_active,
    u.is_superuser,
    COUNT(ur.role_id) as role_count
FROM rbac_users u
LEFT JOIN rbac_user_roles ur ON u.id = ur.user_id
WHERE u.username = 'admin'
GROUP BY u.id, u.username, u.email, u.password_hash, u.is_active, u.is_superuser;

-- =====================================================
-- ФІНАЛЬНЕ ПОВІДОМЛЕННЯ
-- =====================================================
DO $$
DECLARE
    all_ok BOOLEAN := true;
    table_count INTEGER;
    user_count INTEGER;
    role_count INTEGER;
    permission_count INTEGER;
BEGIN
    -- Перевірка кількості таблиць
    SELECT COUNT(*) INTO table_count 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_name LIKE 'rbac_%';
    
    IF table_count < 9 THEN
        all_ok := false;
        RAISE WARNING '❌ Недостатньо таблиць: % з 9', table_count;
    END IF;
    
    -- Перевірка користувачів
    SELECT COUNT(*) INTO user_count FROM rbac_users;
    IF user_count < 8 THEN
        all_ok := false;
        RAISE WARNING '❌ Недостатньо користувачів: % з 8', user_count;
    END IF;
    
    -- Перевірка ролей
    SELECT COUNT(*) INTO role_count FROM rbac_roles;
    IF role_count < 9 THEN
        all_ok := false;
        RAISE WARNING '❌ Недостатньо ролей: % з 9', role_count;
    END IF;
    
    -- Перевірка дозволів
    SELECT COUNT(*) INTO permission_count FROM rbac_permissions;
    IF permission_count < 20 THEN
        all_ok := false;
        RAISE WARNING '❌ Недостатньо дозволів: % з 20+', permission_count;
    END IF;
    
    IF all_ok THEN
        RAISE NOTICE '';
        RAISE NOTICE '========================================';
        RAISE NOTICE '✅ RBAC СИСТЕМА ВСТАНОВЛЕНА УСПІШНО!';
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Таблиць: %', table_count;
        RAISE NOTICE 'Користувачів: %', user_count;
        RAISE NOTICE 'Ролей: %', role_count;
        RAISE NOTICE 'Дозволів: %', permission_count;
        RAISE NOTICE '';
        RAISE NOTICE 'Наступний крок: інтеграція з FastAPI backend';
        RAISE NOTICE '========================================';
    ELSE
        RAISE NOTICE '';
        RAISE NOTICE '========================================';
        RAISE NOTICE '⚠️ RBAC СИСТЕМА ПОТРЕБУЄ ПЕРЕВІРКИ';
        RAISE NOTICE '========================================';
        RAISE NOTICE 'Перевірте попередження вище';
        RAISE NOTICE 'Можливо потрібно повторно виконати:';
        RAISE NOTICE '1. 01_create_rbac_tables.sql';
        RAISE NOTICE '2. 02_seed_rbac_data.sql';
        RAISE NOTICE '========================================';
    END IF;
END $$;