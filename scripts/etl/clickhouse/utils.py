"""
scripts/etl/clickhouse/utils.py
Утиліти для роботи з PostGIS та ClickHouse
ТЕРМІНОВЕ ВИПРАВЛЕННЯ - правильний autocommit та ClickHouse синтаксис
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from clickhouse_driver import Client
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List, Any, Optional

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgresConnector:
    """
    Клас для роботи з PostGIS базою даних
    ВИПРАВЛЕНО: правильний autocommit режим
    """
    
    def __init__(self, config: Dict):
        """
        Ініціалізація конектора
        
        Args:
            config: Словник з параметрами підключення
        """
        self.config = config
        self.connection = None
        self.cursor = None
        
    @contextmanager
    def connect(self, autocommit: bool = False):
        """
        Контекстний менеджер для безпечного підключення
        ВИПРАВЛЕНО: autocommit встановлюється ДО створення курсора
        
        Args:
            autocommit: Чи використовувати autocommit режим
        
        Yields:
            Self з активним підключенням
        """
        try:
            # Створюємо підключення
            self.connection = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            
            # ВАЖЛИВО: встановлюємо autocommit ДО створення курсора
            if autocommit:
                self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                logger.debug("Autocommit режим увімкнено")
            
            # Тепер створюємо курсор з RealDictCursor
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            logger.info(f"✅ Підключено до PostGIS: {self.config['database']}")
            
            yield self
            
            # Commit тільки якщо НЕ в autocommit режимі
            if not autocommit and self.connection:
                self.connection.commit()
                
        except psycopg2.Error as e:
            # Rollback тільки якщо НЕ в autocommit режимі
            if self.connection and not autocommit:
                try:
                    self.connection.rollback()
                    logger.warning("⚠️ Виконано rollback транзакції")
                except:
                    pass  # Ігноруємо помилки rollback
            logger.error(f"❌ Помилка підключення до PostGIS: {e}")
            raise
            
        finally:
            # Закриваємо підключення
            if self.cursor:
                try:
                    self.cursor.close()
                except:
                    pass
            if self.connection:
                try:
                    self.connection.close()
                except:
                    pass
            logger.info("Закрито підключення до PostGIS")
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """
        Виконує SELECT запит та повертає результати
        
        Args:
            query: SQL запит
            params: Параметри для запиту
            
        Returns:
            Список словників з результатами
        """
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            results = self.cursor.fetchall()
            logger.info(f"Отримано {len(results)} записів")
            return results
            
        except psycopg2.Error as e:
            logger.error(f"Помилка виконання запиту: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Тестує підключення до бази даних
        
        Returns:
            True якщо підключення успішне
        """
        try:
            with self.connect(autocommit=True):
                self.cursor.execute("SELECT version()")
                result = self.cursor.fetchone()
                logger.info(f"PostgreSQL версія: {result['version']}")
                return True
        except Exception as e:
            logger.error(f"Тест підключення не вдався: {e}")
            return False


class ClickHouseConnector:
    """
    Клас для роботи з ClickHouse базою даних
    ВИПРАВЛЕНО: правильний синтаксис для системних запитів
    """
    
    def __init__(self, config: Dict):
        """
        Ініціалізація конектора
        
        Args:
            config: Словник з параметрами підключення
        """
        self.config = config
        self.client = None
        
    @contextmanager
    def connect(self):
        """
        Контекстний менеджер для безпечного підключення
        
        Yields:
            Self з активним підключенням
        """
        try:
            self.client = Client(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            logger.info(f"✅ Підключено до ClickHouse: {self.config['database']}")
            yield self
            
        except Exception as e:
            logger.error(f"❌ Помилка підключення до ClickHouse: {e}")
            raise
            
        finally:
            if self.client:
                self.client.disconnect()
            logger.info("Закрито підключення до ClickHouse")
    
    def get_table_columns(self, table: str) -> List[str]:
        """
        Отримує список колонок таблиці
        ВИПРАВЛЕНО: використовуємо правильний синтаксис без параметризації
        
        Args:
            table: Назва таблиці (може бути з базою даних: db.table)
            
        Returns:
            Список назв колонок
        """
        try:
            # Розділяємо базу даних та таблицю
            if '.' in table:
                db_name, table_name = table.split('.')
            else:
                db_name = self.config['database']
                table_name = table
            
            # ВИПРАВЛЕНО: використовуємо f-string замість параметризації
            # ClickHouse не підтримує параметризацію для системних запитів
            query = f"""
            SELECT name
            FROM system.columns
            WHERE database = '{db_name}' AND table = '{table_name}'
            ORDER BY position
            """
            
            result = self.client.execute(query)
            columns = [row[0] for row in result]
            
            logger.debug(f"Знайдено {len(columns)} колонок в таблиці {table}")
            return columns
            
        except Exception as e:
            logger.error(f"Помилка отримання структури таблиці {table}: {e}")
            return []
    
    def insert_data(self, table: str, data: List[Dict], columns: List[str] = None) -> int:
        """
        Вставляє дані в таблицю ClickHouse
        ВИПРАВЛЕНО: перевірка на порожній список колонок
        
        Args:
            table: Назва таблиці
            data: Список словників з даними
            columns: Список колонок
            
        Returns:
            Кількість вставлених записів
        """
        if not data:
            logger.warning("Немає даних для вставки")
            return 0
        
        try:
            # Отримуємо існуючі колонки
            existing_columns = self.get_table_columns(table)
            
            if not existing_columns:
                logger.error(f"❌ Не вдалося отримати колонки таблиці {table}")
                return 0
            
            # Визначаємо колонки для вставки
            if columns:
                # Фільтруємо тільки існуючі
                valid_columns = [col for col in columns if col in existing_columns]
            else:
                # Беремо з першого запису, але тільки існуючі
                first_record_cols = list(data[0].keys())
                valid_columns = [col for col in first_record_cols if col in existing_columns]
            
            if not valid_columns:
                logger.error("❌ Жодна колонка не відповідає структурі таблиці!")
                logger.error(f"Колонки в даних: {list(data[0].keys())[:10]}")
                logger.error(f"Колонки в таблиці: {existing_columns[:10]}")
                return 0
            
            # Логуємо різницю
            if columns:
                missing = set(columns) - set(valid_columns)
                if missing:
                    logger.warning(f"⚠️ Пропущено {len(missing)} відсутніх колонок")
                    logger.debug(f"Відсутні: {missing}")
            
            # Підготовка даних
            values = []
            for row in data:
                row_values = []
                for col in valid_columns:
                    value = row.get(col)
                    # Конвертуємо None в відповідні значення
                    if value is None:
                        if 'String' in str(type(value)):
                            value = ''
                        elif any(x in col for x in ['count', 'total', 'id']):
                            value = 0
                        elif any(x in col for x in ['score', 'index', 'density', 'ratio']):
                            value = 0.0
                    row_values.append(value)
                values.append(row_values)
            
            # Формуємо запит
            columns_str = ', '.join(f'`{col}`' for col in valid_columns)
            placeholders = ', '.join(['%s'] * len(valid_columns))
            query = f"INSERT INTO {table} ({columns_str}) VALUES"
            
            # Виконуємо вставку
            self.client.execute(query, values)
            
            logger.info(f"✅ Вставлено {len(values)} записів в {table}")
            logger.info(f"📊 Використано {len(valid_columns)} з {len(existing_columns)} колонок")
            return len(values)
            
        except Exception as e:
            logger.error(f"Помилка вставки даних: {e}")
            # Додаткова діагностика
            if "Syntax error" in str(e):
                logger.error("Можлива проблема з SQL синтаксисом")
                logger.debug(f"Колонки: {valid_columns[:5]}...")
            raise
    
    def test_connection(self) -> bool:
        """
        Тестує підключення до бази даних
        
        Returns:
            True якщо підключення успішне
        """
        try:
            with self.connect():
                result = self.client.execute("SELECT version()")
                logger.info(f"ClickHouse версія: {result[0][0]}")
                return True
        except Exception as e:
            logger.error(f"Тест підключення не вдався: {e}")
            return False


class ETLProgress:
    """
    Клас для відстеження прогресу ETL процесу
    """
    
    def __init__(self, total_records: int, task_name: str):
        """
        Ініціалізація прогрес-трекера
        
        Args:
            total_records: Загальна кількість записів
            task_name: Назва завдання
        """
        self.total = total_records
        self.current = 0
        self.task_name = task_name
        self.start_time = datetime.now()
    
    def update(self, processed: int):
        """
        Оновлює прогрес
        
        Args:
            processed: Кількість оброблених записів
        """
        self.current += processed
        percentage = (self.current / self.total) * 100 if self.total > 0 else 0
        
        # Розрахунок часу
        elapsed = datetime.now() - self.start_time
        if self.current > 0:
            eta = (elapsed / self.current) * (self.total - self.current)
        else:
            eta = None
        
        # Виводимо прогрес
        bar_length = 40
        filled = int(bar_length * self.current / self.total) if self.total > 0 else 0
        bar = '█' * filled + '░' * (bar_length - filled)
        
        status = f"\r{self.task_name}: [{bar}] {percentage:.1f}% ({self.current}/{self.total})"
        if eta:
            status += f" ETA: {str(eta).split('.')[0]}"
        
        print(status, end='', flush=True)
        
        if self.current >= self.total:
            print()  # Новий рядок після завершення
            logger.info(f"✅ {self.task_name} завершено за {elapsed}")


# Допоміжні функції
def test_connections(pg_config: Dict, ch_config: Dict) -> bool:
    """
    Тестує підключення до обох баз даних
    
    Args:
        pg_config: Конфігурація PostGIS
        ch_config: Конфігурація ClickHouse
        
    Returns:
        True якщо обидва підключення успішні
    """
    logger.info("=" * 60)
    logger.info("🔧 Тестування підключень до баз даних")
    logger.info("=" * 60)
    
    # Тест PostGIS
    pg_connector = PostgresConnector(pg_config)
    pg_ok = pg_connector.test_connection()
    
    # Тест ClickHouse
    ch_connector = ClickHouseConnector(ch_config)
    ch_ok = ch_connector.test_connection()
    
    if pg_ok and ch_ok:
        logger.info("✅ Всі підключення працюють!")
        return True
    else:
        logger.error("❌ Проблеми з підключенням!")
        return False