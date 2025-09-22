"""
connections module for src\database
"""

# TODO: Implement connections
"""
Database Connections
PostGIS and ClickHouse connection management
"""

import os
from typing import AsyncGenerator, Optional
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncSession,
    async_sessionmaker
)
from sqlalchemy.orm import declarative_base
from clickhouse_driver import Client as ClickHouseClient
import redis.asyncio as redis
from dotenv import load_dotenv

# Завантажуємо змінні середовища
load_dotenv()

# ================== Configuration ==================

class DatabaseConfig:
    """Конфігурація підключень до БД"""
    
    # PostgreSQL/PostGIS
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "georetail")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "georetail_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "georetail_secure_2024")
    
    # ClickHouse
    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "32769")
    CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "geo_analytics")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "webuser")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "password123")
    
    # Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = os.getenv("REDIS_PORT", "6379")
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "redis_secure_2024")
    REDIS_DB = os.getenv("REDIS_DB", "0")
    
    @property
    def postgres_url(self) -> str:
        """PostgreSQL connection string"""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    @property
    def redis_url(self) -> str:
        """Redis connection string"""
        return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


# Створюємо інстанс конфігурації
db_config = DatabaseConfig()

# ================== PostgreSQL/PostGIS ==================

# Async engine для PostgreSQL
postgres_engine = create_async_engine(
    db_config.postgres_url,
    echo=False,  # True для debug
    pool_size=20,
    max_overflow=40,
    pool_pre_ping=True,  # Перевірка з'єднання перед використанням
    pool_recycle=3600,  # Оновлення з'єднань кожну годину
)

# Session factory
AsyncSessionLocal = async_sessionmaker(
    postgres_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Base для моделей SQLAlchemy
Base = declarative_base()


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Dependency для отримання сесії PostgreSQL
    
    Yields:
        AsyncSession: Сесія бази даних
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ================== ClickHouse ==================

class ClickHouseConnection:
    """Менеджер підключення до ClickHouse"""
    
    def __init__(self):
        self._client: Optional[ClickHouseClient] = None
    
    def get_client(self) -> ClickHouseClient:
        """Отримати клієнт ClickHouse"""
        if not self._client:
            self._client = ClickHouseClient(
                host=db_config.CLICKHOUSE_HOST,
                port=int(db_config.CLICKHOUSE_PORT),
                user=db_config.CLICKHOUSE_USER,
                password=db_config.CLICKHOUSE_PASSWORD,
                database=db_config.CLICKHOUSE_DB,
                settings={
                    'use_numpy': True,
                    'max_query_size': 1000000,
                    'max_memory_usage': 10000000000  # 10GB
                }
            )
        return self._client
    
    def close(self):
        """Закрити з'єднання"""
        if self._client:
            self._client.disconnect()
            self._client = None
    
    async def execute_query(self, query: str, params: dict = None):
        """
        Виконати запит до ClickHouse
        
        Args:
            query: SQL запит
            params: Параметри запиту
        
        Returns:
            Результат запиту
        """
        client = self.get_client()
        return client.execute(query, params or {})


# Singleton інстанс для ClickHouse
clickhouse = ClickHouseConnection()


def get_clickhouse() -> ClickHouseConnection:
    """Dependency для отримання ClickHouse клієнта"""
    return clickhouse


# ================== Redis ==================

class RedisConnection:
    """Менеджер підключення до Redis"""
    
    def __init__(self):
        self._redis: Optional[redis.Redis] = None
    
    async def get_redis(self) -> redis.Redis:
        """Отримати асинхронний клієнт Redis"""
        if not self._redis:
            self._redis = await redis.from_url(
                db_config.redis_url,
                encoding="utf-8",
                decode_responses=True,
                max_connections=50
            )
        return self._redis
    
    async def close(self):
        """Закрити з'єднання"""
        if self._redis:
            await self._redis.close()
            self._redis = None
    
    @asynccontextmanager
    async def get_lock(self, key: str, timeout: int = 10):
        """
        Отримати розподілений lock
        
        Args:
            key: Ключ для блокування
            timeout: Таймаут в секундах
        """
        redis_client = await self.get_redis()
        lock = redis_client.lock(f"lock:{key}", timeout=timeout)
        try:
            await lock.acquire()
            yield lock
        finally:
            await lock.release()
    
    async def cache_get(self, key: str) -> Optional[str]:
        """Отримати значення з кешу"""
        redis_client = await self.get_redis()
        return await redis_client.get(key)
    
    async def cache_set(
        self,
        key: str,
        value: str,
        expire: int = 3600
    ):
        """
        Зберегти значення в кеші
        
        Args:
            key: Ключ
            value: Значення
            expire: TTL в секундах
        """
        redis_client = await self.get_redis()
        await redis_client.setex(key, expire, value)
    
    async def cache_delete(self, pattern: str):
        """Видалити ключі за патерном"""
        redis_client = await self.get_redis()
        async for key in redis_client.scan_iter(pattern):
            await redis_client.delete(key)


# Singleton інстанс для Redis
redis_connection = RedisConnection()


async def get_redis() -> redis.Redis:
    """Dependency для отримання Redis клієнта"""
    return await redis_connection.get_redis()


# ================== Lifecycle Management ==================

async def init_databases():
    """Ініціалізація підключень до БД при старті"""
    
    # Перевірка PostgreSQL
    async with postgres_engine.begin() as conn:
        # Перевірка розширень
        result = await conn.execute(text("""
            SELECT extname, extversion 
            FROM pg_extension 
            WHERE extname IN ('postgis', 'h3', 'h3_postgis')
        """))
        extensions = result.fetchall()
        
        print("✅ PostgreSQL connected")
        print(f"   Extensions: {[f'{e[0]} v{e[1]}' for e in extensions]}")
    
    # Перевірка ClickHouse
    try:
        ch_client = clickhouse.get_client()
        version = ch_client.execute("SELECT version()")[0][0]
        print(f"✅ ClickHouse connected: {version}")
    except Exception as e:
        print(f"⚠️ ClickHouse connection failed: {e}")
    
    # Перевірка Redis
    try:
        redis_client = await redis_connection.get_redis()
        pong = await redis_client.ping()
        if pong:
            print("✅ Redis connected")
    except Exception as e:
        print(f"⚠️ Redis connection failed: {e}")


async def close_databases():
    """Закриття підключень до БД при зупинці"""
    
    # Закриття PostgreSQL
    await postgres_engine.dispose()
    print("✅ PostgreSQL disconnected")
    
    # Закриття ClickHouse
    clickhouse.close()
    print("✅ ClickHouse disconnected")
    
    # Закриття Redis
    await redis_connection.close()
    print("✅ Redis disconnected")


# ================== Utility Functions ==================

from sqlalchemy import text

async def check_table_exists(
    session: AsyncSession,
    schema: str,
    table: str
) -> bool:
    """
    Перевірка існування таблиці
    
    Args:
        session: Сесія БД
        schema: Назва схеми
        table: Назва таблиці
    
    Returns:
        True якщо таблиця існує
    """
    query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name = :table
        )
    """)
    
    result = await session.execute(
        query,
        {"schema": schema, "table": table}
    )
    return result.scalar()


async def get_table_count(
    session: AsyncSession,
    schema: str,
    table: str
) -> int:
    """
    Отримати кількість записів в таблиці
    
    Args:
        session: Сесія БД
        schema: Назва схеми
        table: Назва таблиці
    
    Returns:
        Кількість записів
    """
    query = text(f"SELECT COUNT(*) FROM {schema}.{table}")
    result = await session.execute(query)
    return result.scalar()