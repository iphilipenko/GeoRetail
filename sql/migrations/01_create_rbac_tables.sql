-- =====================================================
-- Створення RBAC таблиць для GeoRetail MVP
-- Файл: sql/migrations/01_create_rbac_tables.sql
-- База даних: georetail
-- Схема: public
-- =====================================================

-- Спочатку видаляємо таблиці якщо вони існують (для чистого старту)
-- УВАГА: Це видалить всі дані! Використовувати тільки для початкової установки
/*
DROP TABLE IF EXISTS public.rbac_audit_log CASCADE;
DROP TABLE IF EXISTS public.rbac_user_sessions CASCADE;
DROP TABLE IF EXISTS public.rbac_user_permission_overrides CASCADE;
DROP TABLE IF EXISTS public.rbac_user_roles CASCADE;
DROP TABLE IF EXISTS public.rbac_users CASCADE;
DROP TABLE IF EXISTS public.rbac_role_permissions CASCADE;
DROP TABLE IF EXISTS public.rbac_roles CASCADE;
DROP TABLE IF EXISTS public.rbac_permissions CASCADE;
DROP TABLE IF EXISTS public.rbac_modules CASCADE;
*/

-- =====================================================
-- 1. МОДУЛІ СИСТЕМИ
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_modules (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    is_active BOOLEAN DEFAULT true,
    icon VARCHAR(20),
    display_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.rbac_modules IS 'Модулі системи GeoRetail';
COMMENT ON COLUMN public.rbac_modules.code IS 'Унікальний код модуля (core, competition, expansion)';
COMMENT ON COLUMN public.rbac_modules.icon IS 'Emoji або CSS class для UI';

-- =====================================================
-- 2. ДОЗВОЛИ (PERMISSIONS)
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_permissions (
    id SERIAL PRIMARY KEY,
    module_id INTEGER REFERENCES rbac_modules(id) ON DELETE CASCADE,
    code VARCHAR(100) UNIQUE NOT NULL,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    resource_type VARCHAR(50),
    risk_level VARCHAR(20) DEFAULT 'low',
    is_active BOOLEAN DEFAULT true,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_risk_level CHECK (risk_level IN ('low', 'medium', 'high', 'critical'))
);

COMMENT ON TABLE public.rbac_permissions IS 'Атомарні дозволи в системі';
COMMENT ON COLUMN public.rbac_permissions.resource_type IS 'Тип ресурсу: api, ui, data';
COMMENT ON COLUMN public.rbac_permissions.risk_level IS 'Рівень ризику операції';

-- =====================================================
-- 3. РОЛІ
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_roles (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    is_system BOOLEAN DEFAULT false,
    is_active BOOLEAN DEFAULT true,
    max_sessions INTEGER DEFAULT 1,
    session_duration_hours INTEGER DEFAULT 8,
    metadata JSONB DEFAULT '{}',
    created_by INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.rbac_roles IS 'Ролі користувачів';
COMMENT ON COLUMN public.rbac_roles.is_system IS 'Системна роль (не можна видалити)';
COMMENT ON COLUMN public.rbac_roles.max_sessions IS 'Максимум одночасних сесій';

-- =====================================================
-- 4. ЗВ'ЯЗОК РОЛІ-ДОЗВОЛИ
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_role_permissions (
    role_id INTEGER REFERENCES rbac_roles(id) ON DELETE CASCADE,
    permission_id INTEGER REFERENCES rbac_permissions(id) ON DELETE CASCADE,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    granted_by INTEGER,
    PRIMARY KEY (role_id, permission_id)
);

COMMENT ON TABLE public.rbac_role_permissions IS 'Зв''язок між ролями та дозволами';

-- =====================================================
-- 5. КОРИСТУВАЧІ
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    username VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(20),
    department VARCHAR(100),
    position VARCHAR(100),
    is_active BOOLEAN DEFAULT true,
    is_superuser BOOLEAN DEFAULT false,
    email_verified BOOLEAN DEFAULT false,
    phone_verified BOOLEAN DEFAULT false,
    last_login TIMESTAMP,
    failed_login_attempts INTEGER DEFAULT 0,
    locked_until TIMESTAMP,
    password_changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    must_change_password BOOLEAN DEFAULT false,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.rbac_users IS 'Користувачі системи';
COMMENT ON COLUMN public.rbac_users.is_superuser IS 'Суперкористувач має всі дозволи';
COMMENT ON COLUMN public.rbac_users.locked_until IS 'Блокування після невдалих спроб входу';

-- =====================================================
-- 6. ЗВ'ЯЗОК КОРИСТУВАЧІ-РОЛІ
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_user_roles (
    user_id INTEGER REFERENCES rbac_users(id) ON DELETE CASCADE,
    role_id INTEGER REFERENCES rbac_roles(id) ON DELETE CASCADE,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    assigned_by INTEGER REFERENCES rbac_users(id),
    expires_at TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    PRIMARY KEY (user_id, role_id)
);

COMMENT ON TABLE public.rbac_user_roles IS 'Ролі призначені користувачам';
COMMENT ON COLUMN public.rbac_user_roles.expires_at IS 'Для тимчасових ролей';

-- =====================================================
-- 7. ПЕРСОНАЛЬНІ OVERRIDES
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_user_permission_overrides (
    user_id INTEGER REFERENCES rbac_users(id) ON DELETE CASCADE,
    permission_id INTEGER REFERENCES rbac_permissions(id) ON DELETE CASCADE,
    action VARCHAR(10) NOT NULL CHECK (action IN ('grant', 'revoke')),
    reason TEXT,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by INTEGER REFERENCES rbac_users(id),
    PRIMARY KEY (user_id, permission_id)
);

COMMENT ON TABLE public.rbac_user_permission_overrides IS 'Індивідуальні налаштування дозволів';
COMMENT ON COLUMN public.rbac_user_permission_overrides.action IS 'grant - додати дозвіл, revoke - відібрати';

-- =====================================================
-- 8. СЕСІЇ КОРИСТУВАЧІВ (замість Redis для MVP)
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_user_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id INTEGER REFERENCES rbac_users(id) ON DELETE CASCADE,
    token_hash VARCHAR(255) UNIQUE NOT NULL,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    metadata JSONB DEFAULT '{}'
);

COMMENT ON TABLE public.rbac_user_sessions IS 'Активні сесії користувачів';
COMMENT ON COLUMN public.rbac_user_sessions.token_hash IS 'Хеш JWT токена для валідації';

-- =====================================================
-- 9. АУДИТ ЛОГ
-- =====================================================
CREATE TABLE IF NOT EXISTS public.rbac_audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES rbac_users(id),
    session_id UUID REFERENCES rbac_user_sessions(id),
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id VARCHAR(100),
    module_code VARCHAR(50),
    permission_code VARCHAR(100),
    ip_address INET,
    user_agent TEXT,
    request_method VARCHAR(10),
    request_path TEXT,
    request_body JSONB,
    response_status INTEGER,
    response_time_ms INTEGER,
    error_message TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE public.rbac_audit_log IS 'Логування всіх дій користувачів';
COMMENT ON COLUMN public.rbac_audit_log.action IS 'Тип дії: login, logout, view_h3, export_data';
COMMENT ON COLUMN public.rbac_audit_log.response_time_ms IS 'Час відповіді в мілісекундах';

-- =====================================================
-- ІНДЕКСИ ДЛЯ ОПТИМІЗАЦІЇ
-- =====================================================

-- Індекси для permissions
CREATE INDEX IF NOT EXISTS idx_rbac_permissions_module 
    ON rbac_permissions(module_id);
CREATE INDEX IF NOT EXISTS idx_rbac_permissions_active 
    ON rbac_permissions(is_active) WHERE is_active = true;

-- Індекси для users
CREATE INDEX IF NOT EXISTS idx_rbac_users_email 
    ON rbac_users(email);
CREATE INDEX IF NOT EXISTS idx_rbac_users_username 
    ON rbac_users(username);
CREATE INDEX IF NOT EXISTS idx_rbac_users_active 
    ON rbac_users(is_active) WHERE is_active = true;

-- Індекси для sessions
CREATE INDEX IF NOT EXISTS idx_rbac_user_sessions_token 
    ON rbac_user_sessions(token_hash);
CREATE INDEX IF NOT EXISTS idx_rbac_user_sessions_user 
    ON rbac_user_sessions(user_id) WHERE is_active = true;
CREATE INDEX IF NOT EXISTS idx_rbac_user_sessions_expires 
    ON rbac_user_sessions(expires_at) WHERE is_active = true;

-- Індекси для audit log
CREATE INDEX IF NOT EXISTS idx_rbac_audit_log_user 
    ON rbac_audit_log(user_id);
CREATE INDEX IF NOT EXISTS idx_rbac_audit_log_created 
    ON rbac_audit_log(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rbac_audit_log_action 
    ON rbac_audit_log(action);
CREATE INDEX IF NOT EXISTS idx_rbac_audit_log_module 
    ON rbac_audit_log(module_code) WHERE module_code IS NOT NULL;

-- =====================================================
-- ТРИГЕР ДЛЯ ОНОВЛЕННЯ updated_at
-- =====================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Створюємо тригери для всіх таблиць з updated_at
CREATE TRIGGER update_rbac_modules_updated_at 
    BEFORE UPDATE ON rbac_modules
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rbac_permissions_updated_at 
    BEFORE UPDATE ON rbac_permissions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rbac_roles_updated_at 
    BEFORE UPDATE ON rbac_roles
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rbac_users_updated_at 
    BEFORE UPDATE ON rbac_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =====================================================
-- ФУНКЦІЯ ДЛЯ ОТРИМАННЯ PERMISSIONS КОРИСТУВАЧА
-- (оптимізована для MVP без Redis)
-- =====================================================
CREATE OR REPLACE FUNCTION get_user_permissions(p_user_id INTEGER)
RETURNS TABLE(permission_code VARCHAR, source VARCHAR)
LANGUAGE SQL
STABLE
AS $$
    -- Permissions від ролей
    SELECT DISTINCT p.code, 'role'::VARCHAR as source
    FROM rbac_permissions p
    JOIN rbac_role_permissions rp ON p.id = rp.permission_id
    JOIN rbac_user_roles ur ON rp.role_id = ur.role_id
    WHERE ur.user_id = p_user_id 
      AND ur.is_active = true
      AND p.is_active = true
      AND (ur.expires_at IS NULL OR ur.expires_at > NOW())
    
    UNION
    
    -- Permission overrides (grant)
    SELECT p.code, 'override_grant'::VARCHAR
    FROM rbac_permissions p
    JOIN rbac_user_permission_overrides upo ON p.id = upo.permission_id
    WHERE upo.user_id = p_user_id 
      AND upo.action = 'grant'
      AND (upo.expires_at IS NULL OR upo.expires_at > NOW())
    
    EXCEPT
    
    -- Виключаємо revoked permissions
    SELECT p.code, 'override_revoke'::VARCHAR
    FROM rbac_permissions p
    JOIN rbac_user_permission_overrides upo ON p.id = upo.permission_id
    WHERE upo.user_id = p_user_id 
      AND upo.action = 'revoke'
      AND (upo.expires_at IS NULL OR upo.expires_at > NOW());
$$;

COMMENT ON FUNCTION get_user_permissions IS 'Отримати всі активні дозволи користувача';

-- =====================================================
-- ПІДТВЕРДЖЕННЯ СТВОРЕННЯ
-- =====================================================
DO $$
BEGIN
    RAISE NOTICE 'RBAC таблиці успішно створені в схемі public';
    RAISE NOTICE 'Наступний крок: виконати 02_seed_rbac_data.sql';
END $$;