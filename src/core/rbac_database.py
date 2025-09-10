#!/usr/bin/env python3
"""
Database connection та session management для RBAC (FIXED для SQLAlchemy 2.0)
Файл: GeoRetail\src\core\rbac_database.py
Шлях від кореня: GeoRetail\src\core\rbac_database.py
Опис: Підключення до БД та управління сесіями для RBAC
"""

import os
from typing import Generator, Optional
from contextlib import contextmanager
import logging

from sqlalchemy import create_engine, pool, text  # Додаємо text
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError

# Налаштування логування
logger = logging.getLogger(__name__)

# =====================================================
# КОНФІГУРАЦІЯ БАЗИ ДАНИХ
# =====================================================

class DatabaseConfig:
    """Конфігурація підключення до БД"""
    
    # Отримуємо параметри з environment або використовуємо defaults
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "georetail")
    DB_USER = os.getenv("DB_USER", "georetail_user")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "georetail_secure_2024")
    
    # Додаткові параметри
    DB_ECHO = os.getenv("DB_ECHO", "false").lower() == "true"
    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))
    
    @classmethod
    def get_database_url(cls, async_mode: bool = False) -> str:
        """
        Отримати URL для підключення до БД
        
        Args:
            async_mode: Використовувати asyncpg замість psycopg2
            
        Returns:
            Database URL string
        """
        if async_mode:
            # Для асинхронного підключення (якщо потрібно в майбутньому)
            driver = "postgresql+asyncpg"
        else:
            # Синхронне підключення через psycopg2
            driver = "postgresql+psycopg2"
            
        return f"{driver}://{cls.DB_USER}:{cls.DB_PASSWORD}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"
    
    @classmethod
    def get_engine_config(cls) -> dict:
        """Отримати конфігурацію для SQLAlchemy engine"""
        return {
            "echo": cls.DB_ECHO,
            "pool_size": cls.DB_POOL_SIZE,
            "max_overflow": cls.DB_MAX_OVERFLOW,
            "pool_timeout": cls.DB_POOL_TIMEOUT,
            "pool_recycle": cls.DB_POOL_RECYCLE,
            "pool_pre_ping": True,  # Перевірка з'єднання перед використанням
        }

# =====================================================
# DATABASE ENGINE ТА SESSION
# =====================================================

class DatabaseManager:
    """Менеджер підключення до бази даних"""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Ініціалізація менеджера БД
        
        Args:
            database_url: URL бази даних (якщо не вказано, береться з конфігурації)
        """
        self.database_url = database_url or DatabaseConfig.get_database_url()
        self.engine = None
        self.SessionLocal = None
        self._scoped_session = None
        
    def initialize(self):
        """Ініціалізація engine та session factory"""
        try:
            # Створюємо engine
            self.engine = create_engine(
                self.database_url,
                **DatabaseConfig.get_engine_config()
            )
            
            # Створюємо session factory
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self.engine,
                expire_on_commit=False
            )
            
            # Створюємо scoped session для thread safety
            self._scoped_session = scoped_session(self.SessionLocal)
            
            logger.info("Database engine initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def get_session(self) -> Session:
        """Отримати нову сесію БД"""
        if not self.SessionLocal:
            self.initialize()
        return self.SessionLocal()
    
    def get_scoped_session(self) -> Session:
        """Отримати scoped сесію (thread-safe)"""
        if not self._scoped_session:
            self.initialize()
        return self._scoped_session()
    
    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Context manager для роботи з сесією.
        Автоматично commit або rollback.
        
        Usage:
            with db_manager.session_scope() as session:
                session.query(User).all()
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        except Exception as e:
            session.rollback()
            logger.error(f"Unexpected error: {e}")
            raise
        finally:
            session.close()
    
    def close(self):
        """Закрити всі з'єднання"""
        if self._scoped_session:
            self._scoped_session.remove()
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")
    
    def test_connection(self) -> bool:
        """Перевірити з'єднання з БД"""
        try:
            with self.session_scope() as session:
                # FIX: Використовуємо text() для SQLAlchemy 2.0
                session.execute(text("SELECT 1"))
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False

# =====================================================
# SINGLETON INSTANCE
# =====================================================

# Створюємо глобальний інстанс менеджера
db_manager = DatabaseManager()

def get_db() -> Generator[Session, None, None]:
    """
    Dependency для FastAPI endpoints.
    Отримати сесію БД для запиту.
    
    Usage:
        @app.get("/users")
        def get_users(db: Session = Depends(get_db)):
            return db.query(User).all()
    """
    session = db_manager.get_session()
    try:
        yield session
    finally:
        session.close()

def get_scoped_db() -> Session:
    """
    Отримати scoped сесію для background tasks.
    Thread-safe версія.
    """
    return db_manager.get_scoped_session()

# =====================================================
# UTILITY FUNCTIONS
# =====================================================

def init_database():
    """Ініціалізувати базу даних при старті додатку"""
    try:
        db_manager.initialize()
        
        # Перевіряємо з'єднання
        if not db_manager.test_connection():
            raise ConnectionError("Cannot connect to database")
            
        logger.info("Database initialized successfully")
        
        # Перевіряємо наявність RBAC таблиць
        with db_manager.session_scope() as session:
            # FIX: Використовуємо text() для SQLAlchemy 2.0
            result = session.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'rbac_%'
            """)).scalar()
            
            if result == 0:
                logger.warning("RBAC tables not found! Run migration scripts.")
            else:
                logger.info(f"Found {result} RBAC tables")
                
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise

def close_database():
    """Закрити з'єднання з БД при зупинці додатку"""
    db_manager.close()
    logger.info("Database connections closed")

# =====================================================
# TRANSACTION HELPERS
# =====================================================

@contextmanager
def transaction(session: Session):
    """
    Helper для явних транзакцій
    
    Usage:
        with transaction(session):
            session.add(new_user)
            session.add(new_role)
    """
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise

def bulk_insert(session: Session, objects: list):
    """
    Bulk insert для великої кількості об'єктів
    
    Args:
        session: SQLAlchemy session
        objects: List of ORM objects to insert
    """
    try:
        session.bulk_save_objects(objects)
        session.commit()
        logger.info(f"Bulk inserted {len(objects)} objects")
    except Exception as e:
        session.rollback()
        logger.error(f"Bulk insert failed: {e}")
        raise

# =====================================================
# QUERY HELPERS
# =====================================================

class QueryHelper:
    """Helper функції для частих запитів"""
    
    @staticmethod
    def get_user_permissions(session: Session, user_id: int) -> list:
        """
        Отримати всі permissions користувача
        Використовує SQL функцію get_user_permissions()
        """
        # FIX: Використовуємо text() для SQLAlchemy 2.0
        result = session.execute(
            text("SELECT * FROM get_user_permissions(:user_id)"),
            {"user_id": user_id}
        ).fetchall()
        
        return [{"code": row[0], "source": row[1]} for row in result]
    
    @staticmethod
    def check_permission(session: Session, user_id: int, permission_code: str) -> bool:
        """Перевірити чи має користувач конкретний permission"""
        permissions = QueryHelper.get_user_permissions(session, user_id)
        return any(p["code"] == permission_code for p in permissions)
    
    @staticmethod
    def get_active_sessions_count(session: Session, user_id: int) -> int:
        """Отримати кількість активних сесій користувача"""
        # FIX: Використовуємо text() для SQLAlchemy 2.0
        return session.execute(text("""
            SELECT COUNT(*) 
            FROM rbac_user_sessions 
            WHERE user_id = :user_id 
            AND is_active = true 
            AND expires_at > NOW()
        """), {"user_id": user_id}).scalar()

# =====================================================
# TESTING
# =====================================================

if __name__ == "__main__":
    """Тестування підключення при запуску модуля"""
    
    # Налаштування логування
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Тест підключення
    print("Testing database connection...")
    init_database()
    
    # Тест запиту
    with db_manager.session_scope() as session:
        # Перевіряємо кількість користувачів
        # FIX: Використовуємо text() для SQLAlchemy 2.0
        user_count = session.execute(
            text("SELECT COUNT(*) FROM rbac_users")
        ).scalar()
        print(f"Found {user_count} users in database")
        
        # Перевіряємо permissions для admin
        if user_count > 0:
            admin_perms = QueryHelper.get_user_permissions(session, 1)
            print(f"Admin has {len(admin_perms)} permissions")
    
    # Закриваємо з'єднання
    close_database()
    print("Database test completed successfully!")