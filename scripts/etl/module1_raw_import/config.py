#!/usr/bin/env python3
"""
Модуль 1: Raw Data Import - Конфігурація
Спрощена конфігурація для імпорту OSM GPKG файлів
"""

from dataclasses import dataclass
from typing import List
import os


@dataclass
class ImportConfig:
    """Конфігурація для імпорту сирих OSM даних"""
    
    # Database connection
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "georetail"
    db_user: str = "georetail_user"
    db_password: str = "georetail_secure_2024"
    
    # Data processing
    data_directory: str = r"C:\OSMData"
    batch_size: int = 5000
    
    # Надійність
    retry_attempts: int = 3
    retry_delay: int = 5
    
    # Логування
    log_level: str = "INFO"
    log_file: str = "module1_import.log"
    
    @property
    def connection_string(self) -> str:
        """SQLAlchemy connection string"""
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    @classmethod
    def from_env(cls) -> 'ImportConfig':
        """Створення конфігурації з environment variables"""
        return cls(
            db_host=os.getenv('DB_HOST', 'localhost'),
            db_port=int(os.getenv('DB_PORT', 5432)),
            db_name=os.getenv('DB_NAME', 'georetail'),
            db_user=os.getenv('DB_USER', 'georetail_user'),
            db_password=os.getenv('DB_PASSWORD', 'georetail_secure_2024'),
            data_directory=os.getenv('DATA_DIR', r"C:\OSMData"),
            batch_size=int(os.getenv('BATCH_SIZE', 5000)),
        )