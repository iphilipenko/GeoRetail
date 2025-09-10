-- =====================================================
-- Заповнення RBAC таблиць початковими даними
-- Файл: sql/migrations/02_seed_rbac_data.sql
-- База даних: georetail
-- ВАЖЛИВО: Виконувати після 01_create_rbac_tables.sql
-- =====================================================

-- Очищення існуючих даних (для повторного запуску)
TRUNCATE TABLE rbac_audit_log CASCADE;
TRUNCATE TABLE rbac_user_sessions CASCADE;
TRUNCATE TABLE rbac_user_permission_overrides CASCADE;
TRUNCATE TABLE rbac_user_roles CASCADE;
TRUNCATE TABLE rbac_role_permissions CASCADE;
TRUNCATE TABLE rbac_users CASCADE;
TRUNCATE TABLE rbac_roles CASCADE;
TRUNCATE TABLE rbac_permissions CASCADE;
TRUNCATE TABLE rbac_modules CASCADE;

-- =====================================================
-- 1. МОДУЛІ СИСТЕМИ
-- =====================================================
INSERT INTO rbac_modules (code, name, description, icon, display_order) VALUES
('core', 'Базовий модуль', 'Карти, адмінодиниці, базова візуалізація', '🗺️', 1),
('competition', 'Аналіз конкурентів', 'Конкурентне середовище, канібалізація', '⚔️', 2),
('expansion', 'Розвиток мережі', 'Скрінінг, прогнозування, польові дослідження', '📈', 3),
('legal', 'Юридичний модуль', 'Оренда, договори, комунікація з орендодавцями', '📋', 4),
('partners', 'Управління партнерами', 'Постачальники, моніторинг якості', '🤝', 5),
('admin', 'Адміністрування', 'Управління користувачами та системою', '⚙️', 99);

-- =====================================================
-- 2. PERMISSIONS (ДОЗВОЛИ)
-- =====================================================

-- Core module permissions
INSERT INTO rbac_permissions (module_id, code, name, resource_type, risk_level, description) 
SELECT m.id, perm.code, perm.name, perm.resource_type, perm.risk_level, perm.description
FROM rbac_modules m
CROSS JOIN (VALUES
    ('core.view_map', 'Перегляд карти', 'ui', 'low', 'Базовий доступ до карти'),
    ('core.view_admin_units', 'Перегляд адмінодиниць', 'data', 'low', 'Перегляд областей, районів, громад'),
    ('core.view_h3_basic', 'Перегляд H3 гексагонів (рівні 7-8)', 'data', 'low', 'Базова візуалізація H3'),
    ('core.view_h3_detailed', 'Перегляд H3 гексагонів (рівні 9-10)', 'data', 'medium', 'Детальна візуалізація H3'),
    ('core.export_pdf', 'Експорт в PDF', 'api', 'low', 'Експорт карт та звітів в PDF'),
    ('core.export_data', 'Експорт даних', 'api', 'medium', 'Експорт даних в CSV/Excel')
) AS perm(code, name, resource_type, risk_level, description)
WHERE m.code = 'core';

-- Competition module permissions
INSERT INTO rbac_permissions (module_id, code, name, resource_type, risk_level, description)
SELECT m.id, perm.code, perm.name, perm.resource_type, perm.risk_level, perm.description
FROM rbac_modules m
CROSS JOIN (VALUES
    ('competition.view_competitors', 'Перегляд конкурентів на карті', 'data', 'medium', 'Показ конкурентів на карті'),
    ('competition.analyze_competitors', 'Аналіз конкурентного середовища', 'api', 'medium', 'Детальний аналіз конкурентів'),
    ('competition.cannibalization_analysis', 'Аналіз канібалізації', 'api', 'high', 'Аналіз канібалізації власної мережі'),
    ('competition.export_reports', 'Експорт звітів по конкурентах', 'api', 'medium', 'Експорт конкурентних звітів')
) AS perm(code, name, resource_type, risk_level, description)
WHERE m.code = 'competition';

-- Expansion module permissions
INSERT INTO rbac_permissions (module_id, code, name, resource_type, risk_level, description)
SELECT m.id, perm.code, perm.name, perm.resource_type, perm.risk_level, perm.description
FROM rbac_modules m
CROSS JOIN (VALUES
    ('expansion.run_screening', 'Запуск батчевого скрінінгу', 'api', 'medium', 'Масовий скрінінг локацій'),
    ('expansion.ml_prediction', 'ML прогнозування виторгу', 'api', 'high', 'Використання ML моделей для прогнозів'),
    ('expansion.create_field_report', 'Створення польового звіту', 'data', 'low', 'Звіти з польових досліджень'),
    ('expansion.upload_media', 'Завантаження фото/відео', 'data', 'low', 'Завантаження медіа файлів'),
    ('expansion.manage_projects', 'Управління проектами експансії', 'data', 'medium', 'Керування проектами розвитку')
) AS perm(code, name, resource_type, risk_level, description)
WHERE m.code = 'expansion';

-- Legal module permissions
INSERT INTO rbac_permissions (module_id, code, name, resource_type, risk_level, description)
SELECT m.id, perm.code, perm.name, perm.resource_type, perm.risk_level, perm.description
FROM rbac_modules m
CROSS JOIN (VALUES
    ('legal.view_rental_listings', 'Перегляд об''єктів оренди', 'data', 'medium', 'Перегляд доступних приміщень'),
    ('legal.manage_contracts', 'Управління договорами', 'data', 'high', 'Робота з договорами оренди'),
    ('legal.contact_landlords', 'Комунікація з орендодавцями', 'api', 'high', 'Зв''язок з власниками приміщень'),
    ('legal.approve_contracts', 'Затвердження договорів', 'api', 'critical', 'Фінальне затвердження договорів')
) AS perm(code, name, resource_type, risk_level, description)
WHERE m.code = 'legal';

-- Partners module permissions
INSERT INTO rbac_permissions (module_id, code, name, resource_type, risk_level, description)
SELECT m.id, perm.code, perm.name, perm.resource_type, perm.risk_level, perm.description
FROM rbac_modules m
CROSS JOIN (VALUES
    ('partners.view_suppliers', 'Перегляд постачальників', 'data', 'low', 'Список постачальників'),
    ('partners.monitor_quality', 'Моніторинг якості', 'api', 'medium', 'Контроль якості постачань'),
    ('partners.manage_partner_contracts', 'Управління договорами з партнерами', 'data', 'high', 'Договори з постачальниками')
) AS perm(code, name, resource_type, risk_level, description)
WHERE m.code = 'partners';

-- Admin module permissions
INSERT INTO rbac_permissions (module_id, code, name, resource_type, risk_level, description)
SELECT m.id, perm.code, perm.name, perm.resource_type, perm.risk_level, perm.description
FROM rbac_modules m
CROSS JOIN (VALUES
    ('admin.view_users', 'Перегляд користувачів', 'ui', 'high', 'Список користувачів системи'),
    ('admin.manage_users', 'Управління користувачами', 'api', 'critical', 'Створення/редагування користувачів'),
    ('admin.manage_roles', 'Управління ролями', 'api', 'critical', 'Створення/редагування ролей'),
    ('admin.view_audit_log', 'Перегляд аудит логу', 'data', 'high', 'Доступ до логів системи'),
    ('admin.system_settings', 'Системні налаштування', 'api', 'critical', 'Зміна системних параметрів')
) AS perm(code, name, resource_type, risk_level, description)
WHERE m.code = 'admin';

-- =====================================================
-- 3. СИСТЕМНІ РОЛІ
-- =====================================================
INSERT INTO rbac_roles (code, name, description, is_system, max_sessions, session_duration_hours) VALUES
('viewer', 'Переглядач', 'Базовий доступ тільки для перегляду', true, 3, 8),
('marketing_analyst', 'Аналітик маркетингу', 'Аналіз ринку та конкурентів', true, 2, 8),
('expansion_manager', 'Менеджер експансії', 'Управління розвитком мережі', true, 2, 10),
('field_researcher', 'Польовий дослідник', 'Збір даних на місцях', true, 1, 12),
('legal_specialist', 'Юридичний спеціаліст', 'Робота з договорами оренди', true, 1, 8),
('partner_manager', 'Менеджер партнерів', 'Управління постачальниками', true, 1, 8),
('regional_manager', 'Регіональний менеджер', 'Управління регіоном', true, 2, 10),
('admin', 'Адміністратор', 'Повний доступ до системи', true, 1, 8),
('superuser', 'Суперкористувач', 'Необмежений доступ', true, 3, 24);

-- =====================================================
-- 4. ЗВ'ЯЗОК РОЛЕЙ ТА ДОЗВОЛІВ
-- =====================================================

-- Viewer role (базовий перегляд)
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'viewer'
  AND p.code IN (
    'core.view_map',
    'core.view_admin_units',
    'core.view_h3_basic'
  );

-- Marketing Analyst role
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'marketing_analyst'
  AND p.code IN (
    'core.view_map',
    'core.view_admin_units',
    'core.view_h3_basic',
    'core.view_h3_detailed',
    'core.export_pdf',
    'core.export_data',
    'competition.view_competitors',
    'competition.analyze_competitors',
    'competition.export_reports'
  );

-- Expansion Manager role
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'expansion_manager'
  AND p.code IN (
    'core.view_map',
    'core.view_admin_units',
    'core.view_h3_basic',
    'core.view_h3_detailed',
    'core.export_pdf',
    'core.export_data',
    'competition.view_competitors',
    'competition.analyze_competitors',
    'competition.cannibalization_analysis',
    'expansion.run_screening',
    'expansion.ml_prediction',
    'expansion.create_field_report',
    'expansion.manage_projects'
  );

-- Field Researcher role
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'field_researcher'
  AND p.code IN (
    'core.view_map',
    'core.view_admin_units',
    'core.view_h3_basic',
    'expansion.create_field_report',
    'expansion.upload_media'
  );

-- Legal Specialist role
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'legal_specialist'
  AND p.code IN (
    'core.view_map',
    'core.view_admin_units',
    'legal.view_rental_listings',
    'legal.manage_contracts',
    'legal.contact_landlords'
  );

-- Partner Manager role
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'partner_manager'
  AND p.code IN (
    'core.view_map',
    'partners.view_suppliers',
    'partners.monitor_quality',
    'partners.manage_partner_contracts'
  );

-- Regional Manager role (широкі повноваження)
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'regional_manager'
  AND p.code NOT LIKE 'admin.%'
  AND p.code NOT IN ('legal.approve_contracts');

-- Admin role (все крім критичних операцій)
INSERT INTO rbac_role_permissions (role_id, permission_id)
SELECT r.id, p.id
FROM rbac_roles r
CROSS JOIN rbac_permissions p
WHERE r.code = 'admin';

-- =====================================================
-- 5. ТЕСТОВІ КОРИСТУВАЧІ
-- =====================================================

-- Пароль для всіх тестових користувачів: Test123!
-- Hash генерований через bcrypt з cost factor 12
-- В реальному коді використовувати: bcrypt.hashpw(b'Test123!', bcrypt.gensalt(12))
-- Цей хеш відповідає паролю 'Test123!'
INSERT INTO rbac_users (
    email, username, password_hash, 
    first_name, last_name, department, position,
    is_active, is_superuser
) VALUES
-- Superuser
('admin@georetail.com', 'admin', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO', 
 'System', 'Administrator', 'IT', 'Системний адміністратор', true, true),

-- Viewer
('viewer@georetail.com', 'viewer_user', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Іван', 'Переглядач', 'Загальний', 'Стажер', true, false),

-- Marketing Analyst
('marketing@georetail.com', 'marketing_analyst', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Марія', 'Аналітик', 'Маркетинг', 'Аналітик маркетингу', true, false),

-- Expansion Manager
('expansion@georetail.com', 'expansion_mgr', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Петро', 'Менеджер', 'Розвиток', 'Менеджер з розвитку', true, false),

-- Field Researcher
('field@georetail.com', 'field_researcher', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Оксана', 'Дослідник', 'Польові дослідження', 'Польовий дослідник', true, false),

-- Legal Specialist
('legal@georetail.com', 'legal_spec', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Андрій', 'Юрист', 'Юридичний', 'Юрисконсульт', true, false),

-- Partner Manager
('partners@georetail.com', 'partner_mgr', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Олена', 'Менеджер', 'Закупівлі', 'Менеджер з постачальників', true, false),

-- Regional Manager
('regional@georetail.com', 'regional_mgr', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewY/N5n2YpAXNJVO',
 'Василь', 'Керівник', 'Регіональний офіс', 'Регіональний директор', true, false);

-- =====================================================
-- 6. ПРИЗНАЧЕННЯ РОЛЕЙ КОРИСТУВАЧАМ
-- =====================================================

-- Admin -> admin role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, u.id
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'admin' AND r.code = 'admin';

-- Viewer -> viewer role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'viewer_user' AND r.code = 'viewer';

-- Marketing -> marketing_analyst role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'marketing_analyst' AND r.code = 'marketing_analyst';

-- Expansion -> expansion_manager role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'expansion_mgr' AND r.code = 'expansion_manager';

-- Field -> field_researcher role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'field_researcher' AND r.code = 'field_researcher';

-- Legal -> legal_specialist role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'legal_spec' AND r.code = 'legal_specialist';

-- Partners -> partner_manager role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'partner_mgr' AND r.code = 'partner_manager';

-- Regional -> regional_manager role
INSERT INTO rbac_user_roles (user_id, role_id, assigned_by)
SELECT u.id, r.id, (SELECT id FROM rbac_users WHERE username = 'admin')
FROM rbac_users u
CROSS JOIN rbac_roles r
WHERE u.username = 'regional_mgr' AND r.code = 'regional_manager';

-- =====================================================
-- 7. ПЕРЕВІРКА РЕЗУЛЬТАТІВ
-- =====================================================
DO $$
DECLARE
    module_count INTEGER;
    permission_count INTEGER;
    role_count INTEGER;
    user_count INTEGER;
    assignment_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO module_count FROM rbac_modules;
    SELECT COUNT(*) INTO permission_count FROM rbac_permissions;
    SELECT COUNT(*) INTO role_count FROM rbac_roles;
    SELECT COUNT(*) INTO user_count FROM rbac_users;
    SELECT COUNT(*) INTO assignment_count FROM rbac_user_roles;
    
    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE '✅ RBAC SEED DATA ЗАВАНТАЖЕНО УСПІШНО!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Модулів створено: %', module_count;
    RAISE NOTICE 'Дозволів створено: %', permission_count;
    RAISE NOTICE 'Ролей створено: %', role_count;
    RAISE NOTICE 'Користувачів створено: %', user_count;
    RAISE NOTICE 'Призначень ролей: %', assignment_count;
    RAISE NOTICE '';
    RAISE NOTICE '📋 ТЕСТОВІ КОРИСТУВАЧІ:';
    RAISE NOTICE '========================';
    RAISE NOTICE 'admin@georetail.com / Test123! - Адміністратор';
    RAISE NOTICE 'viewer@georetail.com / Test123! - Переглядач';
    RAISE NOTICE 'marketing@georetail.com / Test123! - Маркетинг аналітик';
    RAISE NOTICE 'expansion@georetail.com / Test123! - Менеджер експансії';
    RAISE NOTICE 'field@georetail.com / Test123! - Польовий дослідник';
    RAISE NOTICE 'legal@georetail.com / Test123! - Юрист';
    RAISE NOTICE 'partners@georetail.com / Test123! - Менеджер партнерів';
    RAISE NOTICE 'regional@georetail.com / Test123! - Регіональний менеджер';
    RAISE NOTICE '========================================';
END $$;