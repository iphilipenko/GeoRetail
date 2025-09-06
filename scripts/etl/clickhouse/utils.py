"""
scripts/etl/clickhouse/utils.py
Утиліти для роботи з базами даних PostGIS та ClickHouse
Забезпечує підключення, виконання запитів та обробку помилок
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from clickhouse_driver import Client
from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import sys
from datetime import datetime

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PostgresConnector:
    """
    Клас для роботи з PostGIS базою даних
    Забезпечує безпечне підключення та виконання запитів
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Ініціалізація конектора
        
        Args:
            config: Словник з параметрами підключення
        """
        self.config = config
        self.connection = None
        self.cursor = None
    
    @contextmanager
    def connect(self):
        """
        Контекстний менеджер для безпечного підключення
        Автоматично закриває з'єднання після використання
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
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            logger.info(f"✅ Підключено до PostGIS: {self.config['database']}")
            
            yield self
            
        except psycopg2.Error as e:
            logger.error(f"❌ Помилка підключення до PostGIS: {e}")
            raise
        finally:
            # Закриваємо підключення
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                logger.info("Закрито підключення до PostGIS")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict]:
        """
        Виконує SELECT запит та повертає результати
        
        Args:
            query: SQL запит
            params: Параметри для запиту (опційно)
            
        Returns:
            Список словників з результатами
        """
        try:
            self.cursor.execute(query, params)
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
            with self.connect():
                self.cursor.execute("SELECT version()")
                version = self.cursor.fetchone()
                logger.info(f"PostgreSQL версія: {version['version']}")
                return True
        except Exception as e:
            logger.error(f"Тест підключення не вдався: {e}")
            return False


class ClickHouseConnector:
    """
    Клас для роботи з ClickHouse базою даних
    Забезпечує підключення та виконання запитів
    """
    
    def __init__(self, config: Dict[str, Any]):
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
        """
        try:
            # Створюємо клієнт ClickHouse
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
            # Закриваємо підключення
            if self.client:
                self.client.disconnect()
                logger.info("Закрито підключення до ClickHouse")
    
    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[tuple]:
        """
        Виконує запит та повертає результати
        
        Args:
            query: SQL запит
            params: Параметри для запиту (опційно)
            
        Returns:
            Список кортежів з результатами
        """
        try:
            result = self.client.execute(query, params or {})
            logger.info(f"Виконано запит, отримано {len(result)} записів")
            return result
        except Exception as e:
            logger.error(f"Помилка виконання запиту: {e}")
            raise
    
    def insert_data(self, table: str, data: List[Dict], columns: List[str]) -> int:
        """
        Вставляє дані в таблицю ClickHouse
        
        Args:
            table: Назва таблиці
            data: Список словників з даними
            columns: Список колонок для вставки
            
        Returns:
            Кількість вставлених записів
        """
        try:
            # Підготовка даних для вставки
            values = []
            for row in data:
                values.append([row.get(col) for col in columns])
            
            # Формуємо запит
            query = f"INSERT INTO {table} ({','.join(columns)}) VALUES"
            
            # Виконуємо вставку
            self.client.execute(query, values)
            logger.info(f"✅ Вставлено {len(values)} записів в {table}")
            return len(values)
            
        except Exception as e:
            logger.error(f"Помилка вставки даних: {e}")
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
    Показує прогрес-бар та статистику
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