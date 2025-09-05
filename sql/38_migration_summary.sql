-- ================================================================
-- Файл: 38_migration_summary.sql
-- Мета: Фінальний звіт про результати міграції
-- Дата: 2025-01-04
-- ================================================================

-- Загальна статистика по рівнях
SELECT 
    admin_level,
    COUNT(*) as count,
    CASE admin_level
        WHEN 4 THEN 'Області'
        WHEN 5 THEN 'Райони'
        WHEN 6 THEN 'Громади'
        WHEN 7 THEN 'Обласні центри'
        WHEN 8 THEN 'Районні центри'
        WHEN 9 THEN 'Населені пункти'
    END as level_description
FROM osm_ukraine.admin_boundaries
GROUP BY admin_level
ORDER BY admin_level;

-- Перевірка ієрархічних зв'язків
WITH hierarchy_check AS (
    SELECT 
        ab.admin_level as child_level,
        p.admin_level as parent_level,
        COUNT(*) as count
    FROM osm_ukraine.admin_boundaries ab
    LEFT JOIN osm_ukraine.admin_boundaries p ON ab.parent_id = p.id
    WHERE ab.admin_level > 4
    GROUP BY ab.admin_level, p.admin_level
)
SELECT 
    child_level,
    parent_level,
    count,
    CASE 
        WHEN child_level = 5 AND parent_level = 4 THEN '✅ Райони → Області'
        WHEN child_level = 6 AND parent_level = 5 THEN '✅ Громади → Райони'
        WHEN child_level = 7 AND parent_level = 4 THEN '✅ Обл.центри → Області'
        WHEN child_level = 8 AND parent_level = 5 THEN '✅ Рай.центри → Райони'
        WHEN child_level = 9 AND parent_level = 6 THEN '✅ Села → Громади'
        WHEN child_level = 9 AND parent_level = 5 THEN '⚠️ Крим: Села → Райони'
        ELSE '❌ Потребує уваги'
    END as status
FROM hierarchy_check
ORDER BY child_level, parent_level;

-- Створення індексів для покращення продуктивності
CREATE INDEX IF NOT EXISTS idx_admin_level ON osm_ukraine.admin_boundaries(admin_level);
CREATE INDEX IF NOT EXISTS idx_parent_id ON osm_ukraine.admin_boundaries(parent_id);
CREATE INDEX IF NOT EXISTS idx_admin_level_parent ON osm_ukraine.admin_boundaries(admin_level, parent_id);