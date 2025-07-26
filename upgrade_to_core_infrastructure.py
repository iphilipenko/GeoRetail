#!/usr/bin/env python3
"""
GeoRetail Core Infrastructure Upgrade Script
Extends existing project to production-ready Core Infrastructure
"""

import os
from pathlib import Path
import shutil

def upgrade_existing_project():
    """Upgrade existing GeoRetail project to Core Infrastructure"""
    
    print("🚀 Upgrading GeoRetail to Core Infrastructure")
    print("=" * 60)
    
    # Check if we're in the right directory
    current_dir = Path.cwd()
    if not (current_dir / "src").exists():
        print("❌ Error: Please run this script from the GeoRetail project root directory")
        print("   (should contain src/, config/, data/ folders)")
        return False
    
    print(f"📁 Working in: {current_dir}")
    
    # Additional directories to create (extending existing structure)
    new_directories = [
        # Enhanced src structure
        "src/h3",
        "src/h3/grid", 
        "src/h3/metrics",
        "src/h3/processors",
        "src/api",
        "src/api/endpoints",
        "src/api/models", 
        "src/api/dependencies",
        "src/database",
        "src/database/models",
        "src/database/connections",
        "src/ml",
        "src/ml/models",
        "src/ml/training",
        "src/ml/inference",
        "src/tasks",
        "src/tasks/workers",
        "src/core",
        "src/core/config",
        "src/core/security",
        
        # Enhanced config structure
        "config/docker",
        "config/postgres", 
        "config/postgres/init",
        "config/redis",
        "config/neo4j",
        "config/nginx",
        "config/monitoring",
        
        # Enhanced data structure
        "data/demographics",
        "data/stores",
        "data/traffic",
        "data/embeddings",
        "data/cache",
        "data/exports",
        "data/imports",
        
        # Docker infrastructure
        "docker",
        "docker/images",
        "docker/volumes",
        
        # Enhanced scripts
        "scripts/setup",
        "scripts/data_import", 
        "scripts/maintenance",
        "scripts/deployment",
        "scripts/monitoring",
        
        # Documentation
        "docs",
        "docs/api",
        "docs/deployment", 
        "docs/development",
        "docs/architecture",
        
        # Logs (if not exists)
        "logs",
        "logs/app",
        "logs/worker",
        "logs/postgres", 
        "logs/redis",
        "logs/neo4j",
        
        # Enhanced tests
        "tests/integration",
        "tests/performance",
        "tests/api",
        "tests/h3",
        
        # Notebooks for analysis
        "notebooks",
        "notebooks/exploration",
        "notebooks/analysis", 
        "notebooks/visualization"
    ]
    
    # Create new directories
    print("\n📁 Creating additional directory structure...")
    for directory in new_directories:
        dir_path = Path(directory)
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✅ Created: {directory}")
        else:
            print(f"⏭️  Exists: {directory}")
    
    # Create __init__.py files for new Python packages
    new_init_files = [
        "src/h3/__init__.py",
        "src/h3/grid/__init__.py", 
        "src/h3/metrics/__init__.py",
        "src/h3/processors/__init__.py",
        "src/api/__init__.py",
        "src/api/endpoints/__init__.py",
        "src/api/models/__init__.py",
        "src/api/dependencies/__init__.py",
        "src/database/__init__.py",
        "src/database/models/__init__.py",
        "src/database/connections/__init__.py",
        "src/ml/__init__.py",
        "src/ml/models/__init__.py",
        "src/ml/training/__init__.py",
        "src/ml/inference/__init__.py",
        "src/tasks/__init__.py",
        "src/tasks/workers/__init__.py",
        "src/core/__init__.py",
        "src/core/config/__init__.py",
        "src/core/security/__init__.py",
        "tests/integration/__init__.py",
        "tests/performance/__init__.py",
        "tests/api/__init__.py",
        "tests/h3/__init__.py"
    ]
    
    print("\n📝 Creating __init__.py files...")
    for init_file in new_init_files:
        init_path = Path(init_file)
        if not init_path.exists():
            init_path.touch()
            print(f"✅ Created: {init_file}")
    
    # Create .gitkeep files for empty directories
    gitkeep_files = [
        "data/demographics/.gitkeep",
        "data/stores/.gitkeep", 
        "data/traffic/.gitkeep",
        "data/embeddings/.gitkeep",
        "data/cache/.gitkeep",
        "data/exports/.gitkeep",
        "data/imports/.gitkeep",
        "logs/app/.gitkeep",
        "logs/worker/.gitkeep",
        "logs/postgres/.gitkeep",
        "logs/redis/.gitkeep", 
        "logs/neo4j/.gitkeep",
        "docker/volumes/.gitkeep"
    ]
    
    print("\n📌 Creating .gitkeep files...")
    for gitkeep in gitkeep_files:
        gitkeep_path = Path(gitkeep)
        if not gitkeep_path.exists():
            gitkeep_path.touch()
            print(f"✅ Created: {gitkeep}")

def backup_existing_files():
    """Backup existing configuration files before upgrade"""
    
    print("\n💾 Creating backup of existing files...")
    
    backup_dir = Path("backup_before_upgrade") 
    backup_dir.mkdir(exist_ok=True)
    
    files_to_backup = [
        "requirements.txt",
        ".env.example", 
        ".gitignore",
        "README.md"
    ]
    
    for file_name in files_to_backup:
        file_path = Path(file_name)
        if file_path.exists():
            backup_path = backup_dir / f"{file_name}.backup"
            shutil.copy2(file_path, backup_path)
            print(f"✅ Backed up: {file_name} → backup_before_upgrade/{file_name}.backup")

def update_requirements():
    """Update requirements.txt with new dependencies"""
    
    print("\n📦 Updating requirements.txt...")
    
    # Read existing requirements
    existing_requirements = set()
    req_path = Path("requirements.txt")
    if req_path.exists():
        with open(req_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Extract package name before version specifier
                    pkg_name = line.split('>=')[0].split('==')[0].split('<')[0].split('>')[0]
                    existing_requirements.add(pkg_name.lower())
    
    # New requirements for Core Infrastructure
    new_requirements = """
# ==========================================
# GeoRetail Core Infrastructure Extensions
# ==========================================

# H3 and Spatial Processing (if not already added)
h3>=3.7.6
geopandas>=0.14.1
shapely>=2.0.2

# FastAPI Framework (if not already added)
fastapi>=0.104.1
uvicorn[standard]>=0.24.0
pydantic>=2.5.1
pydantic-settings>=2.1.0

# Database Drivers
psycopg2-binary>=2.9.9
redis[hiredis]>=5.0.1

# Async and Task Processing
celery>=5.3.4
celery[redis]>=5.3.4

# Machine Learning Extensions
xgboost>=2.0.2
lightgbm>=4.1.0

# Graph Processing for Neo4j
node2vec>=0.4.6

# Performance Monitoring
psutil>=5.9.6
memory-profiler>=0.61.0

# Production Server
gunicorn>=21.2.0

# Development Tools (if not present)
black>=23.11.0
isort>=5.12.0
pytest-asyncio>=0.21.1
"""
    
    # Append new requirements to existing file
    with open(req_path, 'a') as f:
        f.write(new_requirements)
    
    print("✅ Updated requirements.txt with Core Infrastructure dependencies")

def create_docker_infrastructure():
    """Create Docker infrastructure files"""
    
    print("\n🐳 Creating Docker infrastructure...")
    
    # Docker Compose for development
    docker_compose_dev = """version: '3.8'

services:
  # PostGIS with H3 (extends existing setup)
  postgis:
    image: postgis/postgis:15-3.4
    container_name: georetail-postgis
    environment:
      POSTGRES_DB: georetail
      POSTGRES_USER: georetail_user
      POSTGRES_PASSWORD: georetail_secure_2024
    ports:
      - "5432:5432"
    volumes:
      - postgis_data:/var/lib/postgresql/data
      - ./config/postgres/init:/docker-entrypoint-initdb.d
      - ./data/imports:/imports
    restart: unless-stopped
    
  # Redis for caching and embeddings
  redis:
    image: redis/redis-stack:7.2.0-v8
    container_name: georetail-redis
    ports:
      - "6379:6379"
      - "8001:8001"  # RedisInsight UI
    volumes:
      - redis_data:/data
      - ./config/redis:/usr/local/etc/redis
    restart: unless-stopped
    
  # Use existing Neo4j Desktop or add containerized version
  # neo4j:
  #   image: neo4j:5.15-community
  #   container_name: georetail-neo4j
  #   environment:
  #     NEO4J_AUTH: neo4j/georetail_password
  #   ports:
  #     - "7474:7474"
  #     - "7687:7687"
  #   volumes:
  #     - neo4j_data:/data

volumes:
  postgis_data:
  redis_data:
  # neo4j_data:  # If using containerized Neo4j

networks:
  default:
    name: georetail-network
"""
    
    with open("docker-compose.dev.yml", "w") as f:
        f.write(docker_compose_dev)
    print("✅ Created: docker-compose.dev.yml")
    
    # Dockerfile for the application
    dockerfile = """# GeoRetail Application Dockerfile
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    build-essential \\
    gdal-bin \\
    libgdal-dev \\
    libpq-dev \\
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV GDAL_CONFIG=/usr/bin/gdal-config

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

EXPOSE 8000

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
"""
    
    with open("docker/Dockerfile", "w") as f:
        f.write(dockerfile)
    print("✅ Created: docker/Dockerfile")

def create_makefile():
    """Create Makefile for common operations"""
    
    makefile_content = """# GeoRetail Development Makefile

.PHONY: help install dev-setup test clean

help:
	@echo "GeoRetail Development Commands:"
	@echo "  make install        - Install dependencies"
	@echo "  make dev-setup      - Setup development environment"
	@echo "  make docker-up      - Start Docker services"
	@echo "  make docker-down    - Stop Docker services"
	@echo "  make test           - Run tests"
	@echo "  make lint           - Code formatting and linting"
	@echo "  make clean          - Clean cache and temp files"

install:
	pip install -r requirements.txt

dev-setup:
	pip install -r requirements.txt
	@echo "✅ Development environment ready!"
	@echo "💡 Start Docker services: make docker-up"

docker-up:
	docker-compose -f docker-compose.dev.yml up -d
	@echo "🐳 Docker services started"
	@echo "   PostGIS: localhost:5432"
	@echo "   Redis: localhost:6379"
	@echo "   RedisInsight: http://localhost:8001"

docker-down:
	docker-compose -f docker-compose.dev.yml down

test:
	pytest tests/ -v

lint:
	black src/ tests/
	isort src/ tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
"""
    
    with open("Makefile", "w") as f:
        f.write(makefile_content)
    print("✅ Created: Makefile")

def update_gitignore():
    """Update .gitignore with additional patterns"""
    
    print("\n📝 Updating .gitignore...")
    
    additional_gitignore = """
# Core Infrastructure additions
data/demographics/*
data/stores/*
data/traffic/*
data/embeddings/*
data/cache/*
data/exports/*
!data/*/.gitkeep

# Docker
docker/volumes/*
!docker/volumes/.gitkeep

# Logs
logs/*.log
logs/*/*.log

# Backup
backup_before_upgrade/

# ML Models
*.pkl
*.joblib
*.model

# Large data files
*.parquet
*.feather
*.h5
"""
    
    gitignore_path = Path(".gitignore")
    if gitignore_path.exists():
        with open(gitignore_path, 'a') as f:
            f.write(additional_gitignore)
        print("✅ Updated .gitignore")
    else:
        print("⚠️  .gitignore not found, please create one")

def main():
    """Main upgrade process"""
    
    print("🎯 GeoRetail Core Infrastructure Upgrade")
    print("This will extend your existing project with:")
    print("  • H3 spatial processing")
    print("  • PostGIS + Redis infrastructure") 
    print("  • FastAPI endpoints")
    print("  • ML/Graph capabilities")
    print("  • Docker development environment")
    print()
    
    # Check if user wants to proceed
    response = input("Continue with upgrade? (y/N): ")
    if response.lower() != 'y':
        print("Upgrade cancelled.")
        return
    
    # Backup existing files
    backup_existing_files()
    
    # Upgrade structure
    upgrade_existing_project()
    
    # Update configurations
    update_requirements()
    update_gitignore()
    
    # Create Docker infrastructure
    create_docker_infrastructure()
    
    # Create Makefile
    create_makefile()
    
    print("\n" + "=" * 60)
    print("🎉 GeoRetail Core Infrastructure Upgrade Complete!")
    print("\n📋 Next Steps:")
    print("1. Review updated requirements.txt")
    print("2. Update your .env file with new variables")
    print("3. Run 'make dev-setup' to install new dependencies")
    print("4. Run 'make docker-up' to start infrastructure services")
    print("5. Start integrating H3 and API components!")
    print("\n💡 Your existing code is preserved and extended!")
    print("💾 Backups created in: backup_before_upgrade/")

if __name__ == "__main__":
    main()