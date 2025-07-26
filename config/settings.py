from pydantic_settings import BaseSettings
from pathlib import Path
from typing import Optional

class Settings(BaseSettings):
    """Application settings and configuration"""
    
    # Project paths
    project_root: Path = Path(__file__).parent.parent
    data_dir: Path = project_root / "data"
    
    # Neo4j Configuration
    neo4j_uri: str = "neo4j://127.0.0.1:7687"
    neo4j_user: str = "neo4j" 
    neo4j_password: str = "Nopassword"
    neo4j_database: str = "georetail"
    
    # OSM Configuration
    osm_radius_meters: int = 500
    osm_cache_dir: str = "data/osm_cache"
    
    # Machine Learning Configuration
    embedding_dimensions: int = 128
    model_random_state: int = 42
    
    # Development Configuration
    debug: bool = True
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = False

# Global settings instance
settings = Settings()