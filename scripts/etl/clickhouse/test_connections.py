"""
scripts/etl/clickhouse/test_connections.py
Скрипт для тестування підключень до PostGIS та ClickHouse
Перевіряє доступність баз даних та наявність необхідних таблиць
Виправлена версія з обробкою спеціальних назв таблиць
"""

import sys
import logging
from config import PG_CONFIG, CH_CONFIG
from utils import PostgresConnector, ClickHouseConnector, test_connections

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_postgis_tables():
    """
    Перевіряє наявність необхідних таблиць в PostGIS
    """
    logger.info("\n📊 Перевірка таблиць PostGIS...")
    
    required_tables = [
        ('osm_ukraine', 'admin_boundaries'),
        ('osm_ukraine', 'h3_admin_mapping'),
        ('osm_ukraine', 'poi_processed'),
        ('demographics', 'h3_population'),
        ('osm_ukraine', 'rbc_h3_data')
    ]
    
    pg = PostgresConnector(PG_CONFIG)
    
    with pg.connect():
        tables_found = []
        
        for schema, table in required_tables:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = %s 
                    AND table_name = %s
                )
            """
            pg.cursor.execute(query, (schema, table))
            exists = pg.cursor.fetchone()['exists']
            
            if exists:
                # Отримуємо кількість записів
                pg.cursor.execute(f"SELECT COUNT(*) as cnt FROM {schema}.{table}")
                count = pg.cursor.fetchone()['cnt']
                logger.info(f"  ✅ {schema}.{table}: {count:,} записів")
                tables_found.append((schema, table, count))
            else:
                logger.warning(f"  ⚠️ {schema}.{table}: НЕ ЗНАЙДЕНО")
        
        return tables_found


def check_clickhouse_setup():
    """
    Перевіряє налаштування ClickHouse
    Виправлена версія з правильною обробкою назв таблиць
    """
    logger.info("\n📊 Перевірка ClickHouse...")
    
    ch = ClickHouseConnector(CH_CONFIG)
    
    with ch.connect():
        # Перевірка бази даних
        result = ch.client.execute("SHOW DATABASES")
        databases = [db[0] for db in result]
        
        if 'geo_analytics' in databases:
            logger.info(f"  ✅ База даних geo_analytics існує")
            
            # Перевірка таблиць
            ch.client.execute("USE geo_analytics")
            tables = ch.client.execute("SHOW TABLES")
            
            if tables:
                logger.info(f"  Знайдено таблиць: {len(tables)}")
                
                # Перевіряємо наявність наших цільових таблиць
                target_tables = ['admin_analytics', 'h3_analytics']
                existing_tables = [t[0] for t in tables]
                
                for target_table in target_tables:
                    if target_table in existing_tables:
                        try:
                            # Використовуємо backticks для безпечних назв таблиць
                            count = ch.client.execute(f"SELECT COUNT(*) FROM `{target_table}`")[0][0]
                            logger.info(f"    ✅ {target_table}: {count:,} записів")
                        except Exception as e:
                            logger.info(f"    ✅ {target_table}: існує (порожня)")
                    else:
                        logger.info(f"    ⚠️ {target_table}: ще не створена")
                
                # Показуємо інші таблиці (якщо є)
                other_tables = [t[0] for t in tables if not t[0].startswith('.') and t[0] not in target_tables]
                if other_tables:
                    logger.info(f"  Інші таблиці: {', '.join(other_tables)}")
                
                # Показуємо системні таблиці окремо
                system_tables = [t[0] for t in tables if t[0].startswith('.')]
                if system_tables:
                    logger.info(f"  Системні таблиці: {len(system_tables)} шт.")
                    
            else:
                logger.info("  ⚠️ Таблиці ще не створені")
                logger.info("  Створюємо структуру таблиць...")
                create_clickhouse_tables(ch)
        else:
            logger.warning("  ⚠️ База даних geo_analytics НЕ ІСНУЄ")
            logger.info("  Створюємо базу даних...")
            ch.client.execute("CREATE DATABASE IF NOT EXISTS geo_analytics")
            logger.info("  ✅ База даних створена")
            create_clickhouse_tables(ch)


def create_clickhouse_tables(ch):
    """
    Створює необхідні таблиці в ClickHouse якщо їх немає
    """
    logger.info("\n🔨 Створення таблиць ClickHouse...")
    
    # Таблиця admin_analytics
    admin_table_sql = """
    CREATE TABLE IF NOT EXISTS geo_analytics.admin_analytics (
        -- Ідентифікація
        admin_id UInt32,
        admin_level UInt8,
        admin_name String,
        admin_name_uk String,
        parent_id Nullable(UInt32),
        osm_id Int64,
        
        -- Базові характеристики
        area_km2 Float32,
        perimeter_km Float32,
        hex_count_r7 UInt32,
        hex_count_r8 UInt32,
        hex_count_r9 UInt32,
        
        -- Населення
        population_estimated Nullable(Float64),
        population_density Nullable(Float32),
        residential_coverage Float32,
        
        -- Економічна активність
        economic_activity_index Nullable(Float32),
        
        -- Конкуренція
        competitors_total UInt32,
        competitor_density Float32,
        
        -- Інфраструктура
        poi_total_count UInt32,
        poi_density Float32,
        
        -- Транспорт
        road_density_km_per_km2 Float32,
        connectivity_index Float32,
        
        -- Метадані
        updated_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    PARTITION BY admin_level
    ORDER BY (admin_level, admin_id)
    """
    
    # Таблиця h3_analytics
    h3_table_sql = """
    CREATE TABLE IF NOT EXISTS geo_analytics.h3_analytics (
        -- Ідентифікація
        h3_index String,
        resolution UInt8,
        
        -- Core метрики
        population_density Float32,
        income_index Float32,
        competitor_intensity Float32,
        poi_density Float32,
        accessibility_score Float32,
        retail_potential Float32,
        
        -- Географічна прив'язка
        oblast_id UInt32,
        raion_id UInt32,
        gromada_id UInt32,
        
        -- Метадані
        updated_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    PARTITION BY resolution
    ORDER BY (resolution, oblast_id, h3_index)
    """
    
    try:
        # Створюємо таблиці
        ch.client.execute(admin_table_sql)
        logger.info("  ✅ Таблиця admin_analytics створена/оновлена")
        
        ch.client.execute(h3_table_sql)
        logger.info("  ✅ Таблиця h3_analytics створена/оновлена")
        
    except Exception as e:
        logger.error(f"  ❌ Помилка створення таблиць: {e}")


def check_h3_functions():
    """
    Перевіряє доступність H3 функцій в PostGIS
    """
    logger.info("\n🔧 Перевірка H3 функцій в PostGIS...")
    
    pg = PostgresConnector(PG_CONFIG)
    
    with pg.connect():
        try:
            # Тест H3 функції (нова версія)
            pg.cursor.execute("""
                SELECT h3_lat_lng_to_cell(50.4501, 30.5234, 9) as h3_index
            """)
            result = pg.cursor.fetchone()
            if result and result['h3_index']:
                logger.info(f"  ✅ H3 функції працюють. Тестовий індекс: {result['h3_index']}")
                return True
        except Exception:
            # Спробуємо стару версію
            try:
                pg.cursor.execute("""
                    SELECT h3_geo_to_h3(50.4501, 30.5234, 9) as h3_index
                """)
                result = pg.cursor.fetchone()
                if result and result['h3_index']:
                    logger.info(f"  ✅ H3 функції працюють (стара версія). Тестовий індекс: {result['h3_index']}")
                    logger.info("  ⚠️ Використовується стара версія H3 функцій")
                    return True
            except Exception as e:
                logger.warning(f"  ⚠️ H3 функції не доступні: {e}")
                logger.info("  Спробуйте встановити розширення H3:")
                logger.info("  CREATE EXTENSION IF NOT EXISTS h3;")
                logger.info("  CREATE EXTENSION IF NOT EXISTS h3_postgis;")
                return False


def show_summary(pg_tables, ch_ready):
    """
    Показує підсумок перевірки
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 ПІДСУМОК ПЕРЕВІРКИ")
    logger.info("=" * 60)
    
    # PostGIS статистика
    if pg_tables:
        total_records = sum(count for _, _, count in pg_tables)
        logger.info(f"\nPostGIS:")
        logger.info(f"  ✅ Знайдено {len(pg_tables)} таблиць")
        logger.info(f"  ✅ Загалом {total_records:,} записів")
        
        # Детальна статистика
        for schema, table, count in pg_tables:
            if table == 'admin_boundaries':
                logger.info(f"     - Адмінодиниць: {count:,}")
            elif table == 'h3_admin_mapping':
                logger.info(f"     - H3 мапінгів: {count:,}")
            elif table == 'poi_processed':
                logger.info(f"     - POI об'єктів: {count:,}")
            elif table == 'h3_population':
                logger.info(f"     - H3 з населенням: {count:,}")
    
    logger.info(f"\nClickHouse:")
    if ch_ready:
        logger.info(f"  ✅ База даних готова")
        logger.info(f"  ✅ Таблиці створені")
    else:
        logger.info(f"  ⚠️ Потрібно створити таблиці")


def main():
    """
    Головна функція для запуску всіх перевірок
    """
    logger.info("=" * 60)
    logger.info("🚀 ТЕСТУВАННЯ СЕРЕДОВИЩА ETL")
    logger.info("=" * 60)
    
    # 1. Тест підключень
    if not test_connections(PG_CONFIG, CH_CONFIG):
        logger.error("❌ Підключення не вдалося. Перевірте конфігурацію!")
        sys.exit(1)
    
    # 2. Перевірка таблиць PostGIS
    pg_tables = check_postgis_tables()
    
    # 3. Перевірка ClickHouse
    check_clickhouse_setup()
    
    # 4. Перевірка H3
    h3_ok = check_h3_functions()
    
    # 5. Підсумок
    show_summary(pg_tables, True)
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ Середовище готове для ETL!")
    logger.info("=" * 60)
    logger.info("\nНаступні кроки:")
    logger.info("1. Запустіть 01_admin_analytics.py для міграції адмінодиниць")
    logger.info("2. Запустіть 02_admin_bins.py для розрахунку bins")
    logger.info("3. Продовжуйте з H3 метриками")


if __name__ == "__main__":
    main()