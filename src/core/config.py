"""
Application Configuration
Settings and environment variables management
"""

import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class Settings(BaseSettings):
    """Налаштування додатку з environment variables"""
    
    # ================== Application ==================
    APP_NAME: str = "GeoRetail API"
    APP_VERSION: str = "2.0.0"
    ENVIRONMENT: str = Field("development", env="ENVIRONMENT")
    DEBUG: bool = Field(True, env="DEBUG")
    PORT: int = Field(8000, env="PORT")
    
    # ================== Security ==================
    JWT_SECRET_KEY: str = Field(
        "your-secret-key-here-min-32-chars-change-in-production",
        env="JWT_SECRET_KEY"
    )
    JWT_ALGORITHM: str = Field("HS256", env="JWT_ALGORITHM")
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(60, env="JWT_ACCESS_TOKEN_EXPIRE_MINUTES")
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = Field(30, env="JWT_REFRESH_TOKEN_EXPIRE_DAYS")
    
    # ================== Database ==================
    # PostgreSQL/PostGIS
    POSTGRES_HOST: str = Field("localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(5432, env="POSTGRES_PORT")
    POSTGRES_DB: str = Field("georetail", env="POSTGRES_DB")
    POSTGRES_USER: str = Field("georetail_user", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field("georetail_secure_2024", env="POSTGRES_PASSWORD")
    
    # ClickHouse
    CLICKHOUSE_HOST: str = Field("localhost", env="CLICKHOUSE_HOST")
    CLICKHOUSE_PORT: int = Field(32769, env="CLICKHOUSE_PORT")
    CLICKHOUSE_DB: str = Field("geo_analytics", env="CLICKHOUSE_DB")
    CLICKHOUSE_USER: str = Field("webuser", env="CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str = Field("password123", env="CLICKHOUSE_PASSWORD")
    
    # Redis
    REDIS_HOST: str = Field("localhost", env="REDIS_HOST")
    REDIS_PORT: int = Field(6379, env="REDIS_PORT")
    REDIS_PASSWORD: str = Field("redis_secure_2024", env="REDIS_PASSWORD")
    REDIS_DB: int = Field(0, env="REDIS_DB")
    
    # ================== CORS ==================
    CORS_ORIGINS: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://localhost:5173",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:5173"
        ]
    )
    
    # ================== API Limits ==================
    MAX_HEXAGONS_PER_REQUEST: int = Field(10000, env="MAX_HEXAGONS_PER_REQUEST")
    MAX_ADMIN_UNITS_PER_REQUEST: int = Field(5000, env="MAX_ADMIN_UNITS_PER_REQUEST")
    API_RATE_LIMIT_PER_MINUTE: int = Field(100, env="API_RATE_LIMIT_PER_MINUTE")
    
    # ================== Cache ==================
    CACHE_TTL_ADMIN_GEOMETRIES: int = Field(86400, env="CACHE_TTL_ADMIN_GEOMETRIES")  # 24 hours
    CACHE_TTL_ADMIN_METRICS: int = Field(3600, env="CACHE_TTL_ADMIN_METRICS")  # 1 hour
    CACHE_TTL_H3_HEXAGONS: int = Field(1800, env="CACHE_TTL_H3_HEXAGONS")  # 30 minutes
    
    # ================== Logging ==================
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    LOG_FILE: Optional[str] = Field(None, env="LOG_FILE")
    
    # ================== Features ==================
    ENABLE_REDIS_CACHE: bool = Field(True, env="ENABLE_REDIS_CACHE")
    ENABLE_AUDIT_LOG: bool = Field(True, env="ENABLE_AUDIT_LOG")
    ENABLE_RATE_LIMITING: bool = Field(True, env="ENABLE_RATE_LIMITING")
    
    # ================== Computed Properties ==================
    
    @property
    def postgres_url(self) -> str:
        """PostgreSQL connection URL"""
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    @property
    def postgres_async_url(self) -> str:
        """PostgreSQL async connection URL"""
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )
    
    @property
    def redis_url(self) -> str:
        """Redis connection URL"""
        return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    @property
    def is_production(self) -> bool:
        """Check if running in production"""
        return self.ENVIRONMENT.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development"""
        return self.ENVIRONMENT.lower() == "development"
    
    # ================== Validators ==================
    
    @validator("JWT_SECRET_KEY")
    def validate_jwt_secret(cls, v, values):
        """Validate JWT secret key"""
        if values.get("ENVIRONMENT") == "production" and len(v) < 32:
            raise ValueError("JWT_SECRET_KEY must be at least 32 characters in production")
        return v
    
    @validator("CORS_ORIGINS", pre=True)
    def parse_cors_origins(cls, v):
        """Parse CORS origins from string or list"""
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v
    
    class Config:
        """Pydantic config"""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# Create settings instance
settings = Settings()


# ================== Logging Configuration ==================

import logging
import sys


def configure_logging():
    """Configure application logging"""
    
    # Log format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, settings.LOG_LEVEL.upper()),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Add file handler if configured
    if settings.LOG_FILE:
        file_handler = logging.FileHandler(settings.LOG_FILE)
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.getLogger().addHandler(file_handler)
    
    # Set specific loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    
    return logging.getLogger(__name__)


# Configure logging on import
logger = configure_logging()


# ================== Environment File Template ==================

ENV_TEMPLATE = """
# GeoRetail API v2 Environment Variables

# Application
ENVIRONMENT=development
DEBUG=True
PORT=8000

# Security
JWT_SECRET_KEY=your-secret-key-here-min-32-chars-change-in-production
JWT_ALGORITHM=HS256
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=60
JWT_REFRESH_TOKEN_EXPIRE_DAYS=30

# PostgreSQL/PostGIS
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=georetail
POSTGRES_USER=georetail_user
POSTGRES_PASSWORD=georetail_secure_2024

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=32769
CLICKHOUSE_DB=geo_analytics
CLICKHOUSE_USER=webuser
CLICKHOUSE_PASSWORD=password123

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=redis_secure_2024
REDIS_DB=0

# CORS (comma-separated)
CORS_ORIGINS=http://localhost:3000,http://localhost:5173

# API Limits
MAX_HEXAGONS_PER_REQUEST=10000
MAX_ADMIN_UNITS_PER_REQUEST=5000
API_RATE_LIMIT_PER_MINUTE=100

# Cache TTL (seconds)
CACHE_TTL_ADMIN_GEOMETRIES=86400
CACHE_TTL_ADMIN_METRICS=3600
CACHE_TTL_H3_HEXAGONS=1800

# Logging
LOG_LEVEL=INFO
# LOG_FILE=/var/log/georetail/api.log

# Features
ENABLE_REDIS_CACHE=True
ENABLE_AUDIT_LOG=True
ENABLE_RATE_LIMITING=True
"""


def create_env_file():
    """Create .env file template if it doesn't exist"""
    env_file_path = ".env"
    
    if not os.path.exists(env_file_path):
        with open(env_file_path, "w") as f:
            f.write(ENV_TEMPLATE.strip())
        logger.info(f"Created {env_file_path} template file")
        logger.warning("Please update .env file with your actual configuration")
    else:
        logger.info(f"{env_file_path} already exists")


# Print configuration on import (for debugging)
if settings.DEBUG:
    logger.info(f"Environment: {settings.ENVIRONMENT}")
    logger.info(f"Debug mode: {settings.DEBUG}")
    logger.info(f"API Port: {settings.PORT}")
    logger.info(f"PostgreSQL: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
    logger.info(f"ClickHouse: {settings.CLICKHOUSE_HOST}:{settings.CLICKHOUSE_PORT}/{settings.CLICKHOUSE_DB}")
    logger.info(f"Redis: {settings.REDIS_HOST}:{settings.REDIS_PORT}")