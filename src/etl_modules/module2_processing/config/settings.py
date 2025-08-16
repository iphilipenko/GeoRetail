"""
GeoRetail Settings Configuration
Pydantic v2 compatible settings with environment variables support
"""

import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings with Pydantic v2 compatibility
    Reads from environment variables and .env file
    """
    
    # Конфігурація моделі для Pydantic v2
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra='allow'  # Ключове: дозволяє додаткові поля
    )
    
    # Neo4j Configuration
    neo4j_uri: str = Field(
        default="bolt://localhost:7687",
        description="Neo4j database URI"
    )
    neo4j_user: str = Field(
        default="neo4j",
        description="Neo4j username"
    )
    neo4j_password: str = Field(
        default="password",
        description="Neo4j password"
    )
    neo4j_database: str = Field(
        default="georetail",
        description="Neo4j database name"
    )
    
    # PostgreSQL Configuration
    postgres_host: str = Field(
        default="localhost",
        description="PostgreSQL host"
    )
    postgres_port: int = Field(
        default=5432,
        description="PostgreSQL port"
    )
    postgres_db: str = Field(
        default="georetail",
        description="PostgreSQL database name"
    )
    postgres_user: str = Field(
        default="georetail_user",
        description="PostgreSQL username"
    )
    postgres_password: str = Field(
        default="georetail_secure_2024",
        description="PostgreSQL password"
    )
    
    # Redis Configuration
    redis_host: str = Field(
        default="localhost",
        description="Redis host"
    )
    redis_port: int = Field(
        default=6379,
        description="Redis port"
    )
    redis_db: int = Field(
        default=0,
        description="Redis database number"
    )
    
    # Application Configuration
    environment: str = Field(
        default="development",
        description="Application environment"
    )
    debug_mode: bool = Field(
        default=True,
        description="Debug mode flag"
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    # H3 Configuration
    h3_default_resolution: int = Field(
        default=9,
        ge=7,
        le=10,
        description="Default H3 resolution for spatial analysis"
    )
    
    # OSM Configuration
    osm_cache_enabled: bool = Field(
        default=True,
        description="Enable OSM data caching"
    )
    osm_cache_ttl: int = Field(
        default=3600,
        description="OSM cache TTL in seconds"
    )
    
    # API Configuration
    api_host: str = Field(
        default="0.0.0.0",
        description="API host binding"
    )
    api_port: int = Field(
        default=8000,
        description="API port"
    )
    api_reload: bool = Field(
        default=True,
        description="API auto-reload in development"
    )
    
    @property
    def postgres_url(self) -> str:
        """PostgreSQL connection URL"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def postgres_async_url(self) -> str:
        """PostgreSQL async connection URL"""
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def redis_url(self) -> str:
        """Redis connection URL"""
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment.lower() == "production"
    
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.environment.lower() == "development"


# Глобальний екземпляр settings
settings = Settings()


# Функції для backward compatibility
def get_neo4j_uri() -> str:
    """Get Neo4j URI"""
    return settings.neo4j_uri


def get_postgres_url() -> str:
    """Get PostgreSQL URL"""
    return settings.postgres_url


def get_redis_url() -> str:
    """Get Redis URL"""
    return settings.redis_url


# Експорт для зручності
__all__ = [
    "Settings",
    "settings",
    "get_neo4j_uri",
    "get_postgres_url", 
    "get_redis_url"
]