#!/usr/bin/env python3
"""
Module 2: Database Initialization Script
Перевіряє з'єднання з БД та створює всі необхідні таблиці
"""

import os
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pathlib import Path
import logging
from datetime import datetime

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection string
DB_CONNECTION_STRING = os.getenv(
    'DB_CONNECTION_STRING',
    "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"
)


class DatabaseInitializer:
    """Клас для ініціалізації бази даних Модуля 2"""
    
    def __init__(self, connection_string=DB_CONNECTION_STRING):
        self.connection_string = connection_string
        self.conn = None
        self.cur = None
        
    def connect(self):
        """Підключення до бази даних"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cur = self.conn.cursor()
            logger.info("✅ Успішно підключено до бази даних")
            return True
        except Exception as e:
            logger.error(f"❌ Помилка підключення до БД: {e}")
            return False
    
    def check_extensions(self):
        """Перевірка необхідних PostgreSQL розширень"""
        required_extensions = ['postgis', 'uuid-ossp', 'h3', 'h3_postgis']
        
        logger.info("Перевірка розширень PostgreSQL...")
        
        for ext in required_extensions:
            try:
                self.cur.execute(f"CREATE EXTENSION IF NOT EXISTS \"{ext}\" CASCADE;")
                logger.info(f"✅ Розширення {ext} доступне")
            except Exception as e:
                logger.warning(f"⚠️  Не вдалося створити розширення {ext}: {e}")
                if ext in ['h3', 'h3_postgis']:
                    logger.info("   H3 розширення опціональні, продовжуємо без них")
                else:
                    raise
    
    def check_existing_tables(self):
        """Перевірка існуючих таблиць"""
        logger.info("\nПеревірка існуючих таблиць...")
        
        self.cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'osm_ukraine' 
            ORDER BY table_name;
        """)
        
        existing_tables = [row[0] for row in self.cur.fetchall()]
        
        if existing_tables:
            logger.info(f"Знайдено таблиці в схемі osm_ukraine:")
            for table in existing_tables:
                # Отримуємо кількість записів
                try:
                    self.cur.execute(f"SELECT COUNT(*) FROM osm_ukraine.{table}")
                    count = self.cur.fetchone()[0]
                    logger.info(f"  - {table}: {count:,} записів")
                except:
                    logger.info(f"  - {table}")
        else:
            logger.info("Схема osm_ukraine порожня")
            
        return existing_tables
    
    def backup_existing_data(self, tables_to_backup):
        """Створення резервних копій існуючих таблиць"""
        if not tables_to_backup:
            return
            
        backup_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info(f"\nСтворення резервних копій таблиць (timestamp: {backup_timestamp})...")
        
        for table in tables_to_backup:
            if table in ['poi_processed', 'h3_analytics_current', 'h3_analytics_changes']:
                try:
                    backup_name = f"{table}_backup_{backup_timestamp}"
                    self.cur.execute(f"""
                        CREATE TABLE osm_ukraine.{backup_name} AS 
                        SELECT * FROM osm_ukraine.{table};
                    """)
                    logger.info(f"✅ Створено резервну копію: {backup_name}")
                except Exception as e:
                    logger.warning(f"⚠️  Не вдалося створити резервну копію {table}: {e}")
    
    def load_schema_sql(self, sql_file_path=None):
        """Завантаження та виконання SQL скрипту схеми"""
        if sql_file_path is None:
            # Шукаємо файл в тій же директорії або в sql/
            current_dir = Path(__file__).parent
            possible_paths = [
                current_dir / "module2_schema.sql",
                current_dir / "sql" / "module2_schema.sql",
                current_dir / ".." / "sql" / "module2_schema.sql",
            ]
            
            for path in possible_paths:
                if path.exists():
                    sql_file_path = path
                    break
            else:
                # Якщо файл не знайдено, використовуємо вбудований SQL
                return self.execute_embedded_schema()
        
        logger.info(f"\nЗавантаження SQL схеми з файлу: {sql_file_path}")
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as f:
                sql_content = f.read()
            
            self.cur.execute(sql_content)
            logger.info("✅ SQL схема успішно виконана")
            return True
            
        except Exception as e:
            logger.error(f"❌ Помилка виконання SQL схеми: {e}")
            return False
    
    def execute_embedded_schema(self):
        """Виконання вбудованої схеми (якщо файл не знайдено)"""
        logger.info("\n⚠️  SQL файл не знайдено, створюємо базові таблиці...")
        
        # Створюємо схему
        self.cur.execute("CREATE SCHEMA IF NOT EXISTS osm_ukraine;")
        
        # Створюємо таблицю poi_processed
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS osm_ukraine.poi_processed (
                entity_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                osm_id BIGINT NOT NULL,
                entity_type VARCHAR(20) NOT NULL,
                primary_category VARCHAR(50) NOT NULL,
                secondary_category VARCHAR(50) NOT NULL,
                name_original VARCHAR(200),
                brand_normalized VARCHAR(100),
                functional_group VARCHAR(50),
                influence_weight DECIMAL(3,2) DEFAULT 0.0,
                geom GEOMETRY NOT NULL,
                h3_res_9 VARCHAR(15),
                quality_score DECIMAL(3,2) DEFAULT 0.0,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        logger.info("✅ Створено базову структуру таблиць")
        logger.info("⚠️  Для повної функціональності виконайте повний SQL скрипт")
        return True
    
    def verify_installation(self):
        """Перевірка успішності встановлення"""
        logger.info("\n🔍 Перевірка встановлення...")
        
        required_tables = [
            'poi_processed',
            'h3_analytics_current', 
            'h3_analytics_changes'
        ]
        
        all_good = True
        
        for table in required_tables:
            self.cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'osm_ukraine' 
                    AND table_name = %s
                );
            """, (table,))
            
            exists = self.cur.fetchone()[0]
            if exists:
                logger.info(f"✅ Таблиця {table} існує")
            else:
                logger.error(f"❌ Таблиця {table} НЕ знайдена")
                all_good = False
        
        # Перевірка views
        self.cur.execute("""
            SELECT viewname 
            FROM pg_views 
            WHERE schemaname = 'osm_ukraine';
        """)
        views = [row[0] for row in self.cur.fetchall()]
        
        if views:
            logger.info(f"\n✅ Знайдено views: {', '.join(views)}")
        
        return all_good
    
    def close(self):
        """Закриття з'єднання з БД"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("\n👋 З'єднання з БД закрито")
    
    def run(self, backup=True, sql_file=None):
        """Основний метод запуску ініціалізації"""
        logger.info("🚀 Початок ініціалізації бази даних для Модуля 2")
        logger.info("=" * 60)
        
        # Підключення
        if not self.connect():
            return False
        
        try:
            # Перевірка розширень
            self.check_extensions()
            
            # Перевірка існуючих таблиць
            existing_tables = self.check_existing_tables()
            
            # Резервне копіювання (якщо потрібно)
            if backup and existing_tables:
                response = input("\n⚠️  Створити резервні копії існуючих таблиць? (y/n): ")
                if response.lower() == 'y':
                    self.backup_existing_data(existing_tables)
            
            # Завантаження та виконання схеми
            if not self.load_schema_sql(sql_file):
                return False
            
            # Перевірка встановлення
            success = self.verify_installation()
            
            if success:
                logger.info("\n✅ Ініціалізація завершена успішно!")
                logger.info("\n📊 Наступні кроки:")
                logger.info("1. Перевірте створені таблиці в pgAdmin або psql")
                logger.info("2. Запустіть тестовий імпорт даних")
                logger.info("3. Перейдіть до розробки Tag Parser")
            else:
                logger.error("\n❌ Ініціалізація завершена з помилками")
            
            return success
            
        except Exception as e:
            logger.error(f"\n❌ Критична помилка: {e}")
            return False
        finally:
            self.close()


def main():
    """Точка входу для скрипта"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Ініціалізація бази даних для GeoRetail Module 2'
    )
    parser.add_argument(
        '--no-backup', 
        action='store_true',
        help='Пропустити створення резервних копій'
    )
    parser.add_argument(
        '--sql-file',
        type=str,
        help='Шлях до SQL файлу схеми'
    )
    parser.add_argument(
        '--connection-string',
        type=str,
        default=DB_CONNECTION_STRING,
        help='PostgreSQL connection string'
    )
    
    args = parser.parse_args()
    
    # Створюємо та запускаємо ініціалізатор
    initializer = DatabaseInitializer(args.connection_string)
    success = initializer.run(
        backup=not args.no_backup,
        sql_file=args.sql_file
    )
    
    # Повертаємо код виходу
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()