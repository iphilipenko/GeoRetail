-- =====================================================
-- ЕТАП 1: Створення схеми та підготовка extensions
-- =====================================================

-- Створення схеми для мережі Avrora
CREATE SCHEMA IF NOT EXISTS avrora;

-- Встановлення прав доступу (адаптуйте під свого користувача)
-- GRANT ALL ON SCHEMA avrora TO your_user;

-- Перевірка наявності необхідних extensions
DO $$
BEGIN
    -- PostGIS для роботи з геоданими
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'postgis') THEN
        CREATE EXTENSION postgis;
        RAISE NOTICE 'Extension postgis встановлено';
    ELSE
        RAISE NOTICE 'Extension postgis вже існує';
    END IF;
    
    -- H3 для роботи з гексагонами
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'h3') THEN
        CREATE EXTENSION h3;
        RAISE NOTICE 'Extension h3 встановлено';
    ELSE
        RAISE NOTICE 'Extension h3 вже існує';
    END IF;
END
$$;

-- Перевірка версій
SELECT 
    'PostgreSQL' as component,
    version() as version
UNION ALL
SELECT 
    'PostGIS',
    PostGIS_version()
UNION ALL
SELECT 
    'H3',
    extversion
FROM pg_extension 
WHERE extname = 'h3';

-- Встановлення пошукового шляху (опціонально)
-- SET search_path TO avrora, public;

COMMENT ON SCHEMA avrora IS 'Схема для даних мережі магазинів Avrora';