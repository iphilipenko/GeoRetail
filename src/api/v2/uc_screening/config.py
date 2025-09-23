"""
Uc Screening Configuration
Configuration settings for Uc Screening
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Uc ScreeningConfig(BaseSettings):
    """Configuration for Uc Screening"""
    
    # Cache settings
    cache_enabled: bool = True
    cache_ttl: int = 300
    
    # Batch processing
    max_batch_size: int = 1000
    batch_timeout: int = 600
    
    # ML settings (if applicable)
    model_path: Optional[str] = None
    model_version: str = "2.0.0"
    
    # Export settings
    export_formats: list = ["json", "csv", "xlsx", "pdf"]
    max_export_rows: int = 10000
    
    class Config:
        env_prefix = "SCREENING_"
        env_file = ".env"


# Singleton instance
config = Uc ScreeningConfig()
