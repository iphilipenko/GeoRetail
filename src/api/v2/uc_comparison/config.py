"""
Uc Comparison Configuration
Configuration settings for Uc Comparison
"""

from pydantic_settings import BaseSettings
from typing import Optional


class Uc ComparisonConfig(BaseSettings):
    """Configuration for Uc Comparison"""
    
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
        env_prefix = "COMPARISON_"
        env_file = ".env"


# Singleton instance
config = Uc ComparisonConfig()
