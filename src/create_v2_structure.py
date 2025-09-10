"""
Скрипт для створення структури папок API v2
Domain-Driven Design Architecture
"""

import os
from pathlib import Path

# Базова директорія
BASE_DIR = Path(__file__).parent

# Структура папок для API v2
API_V2_STRUCTURE = {
    "api/v2": {
        "__init__.py": "# API v2 Root",
        
        # Core infrastructure
        "core": {
            "__init__.py": "# Core utilities and base classes",
            "dependencies.py": "# Dependency injection (get_current_user, etc.)",
            "responses.py": "# Standard response builders",
            "exceptions.py": "# Custom exceptions",
            "permissions.py": "# Permission checking utilities",
            "pagination.py": "# Pagination helpers",
            "filters.py": "# Query filters and validators",
        },
        
        # Middleware
        "middleware": {
            "__init__.py": "# Middleware components",
            "auth.py": "# JWT validation middleware",
            "permissions.py": "# Permission checking middleware",
            "audit.py": "# Audit logging middleware",
            "rate_limit.py": "# Rate limiting middleware",
            "cors.py": "# CORS configuration",
            "error_handler.py": "# Global error handling",
        },
        
        # Domain: Territories (Discovery)
        "territories": {
            "__init__.py": "# Territories domain",
            "router.py": "# Territory endpoints router",
            "service.py": "# Business logic for territories",
            "schemas.py": "# Pydantic models for territories",
            "dependencies.py": "# Territory-specific dependencies",
            
            "endpoints": {
                "__init__.py": "# Territory endpoints",
                "search.py": "# Search territories endpoints",
                "hexagons.py": "# Hexagon-related endpoints",
                "clusters.py": "# Cluster discovery endpoints",
                "compare.py": "# Territory comparison endpoints",
            },
            
            "utils": {
                "__init__.py": "# Territory utilities",
                "h3_helpers.py": "# H3-specific utilities",
                "scoring.py": "# Territory scoring algorithms",
                "filters.py": "# Territory-specific filters",
            },
        },
        
        # Domain: Insights (Analysis)
        "insights": {
            "__init__.py": "# Insights domain",
            "router.py": "# Insights endpoints router",
            "service.py": "# Business logic for insights",
            "schemas.py": "# Pydantic models for insights",
            
            "endpoints": {
                "__init__.py": "# Insights endpoints",
                "competition.py": "# Competition analysis endpoints",
                "potential.py": "# Potential analysis endpoints",
                "reports.py": "# Report generation endpoints",
            },
            
            "analyzers": {
                "__init__.py": "# Analysis engines",
                "competition_analyzer.py": "# Competition analysis logic",
                "market_analyzer.py": "# Market analysis logic",
                "traffic_analyzer.py": "# Traffic pattern analysis",
                "revenue_predictor.py": "# ML revenue prediction",
            },
        },
        
        # Domain: Decisions (Decision Support)
        "decisions": {
            "__init__.py": "# Decisions domain",
            "router.py": "# Decisions endpoints router",
            "service.py": "# Business logic for decisions",
            "schemas.py": "# Pydantic models for decisions",
            
            "endpoints": {
                "__init__.py": "# Decision endpoints",
                "recommendations.py": "# Recommendation endpoints",
                "scoring.py": "# Scoring endpoints",
                "scenarios.py": "# Scenario simulation endpoints",
            },
            
            "engines": {
                "__init__.py": "# Decision engines",
                "recommendation_engine.py": "# Recommendation algorithm",
                "scoring_engine.py": "# Batch scoring logic",
                "roi_calculator.py": "# ROI calculation logic",
                "scenario_simulator.py": "# Scenario simulation",
            },
        },
        
        # Domain: Portfolio (Management)
        "portfolio": {
            "__init__.py": "# Portfolio domain",
            "router.py": "# Portfolio endpoints router",
            "service.py": "# Business logic for portfolio",
            "schemas.py": "# Pydantic models for portfolio",
            
            "endpoints": {
                "__init__.py": "# Portfolio endpoints",
                "locations.py": "# Location management endpoints",
                "contracts.py": "# Contract management endpoints",
                "suppliers.py": "# Supplier management endpoints",
                "projects.py": "# Project management endpoints",
            },
        },
        
        # Domain: Admin
        "admin": {
            "__init__.py": "# Admin domain",
            "router.py": "# Admin endpoints router",
            "service.py": "# Admin business logic",
            "schemas.py": "# Admin Pydantic models",
            
            "endpoints": {
                "__init__.py": "# Admin endpoints",
                "users.py": "# User management endpoints",
                "roles.py": "# Role management endpoints",
                "permissions.py": "# Permission management endpoints",
                "audit.py": "# Audit log endpoints",
                "system.py": "# System management endpoints",
            },
        },
        
        # Shared schemas
        "schemas": {
            "__init__.py": "# Shared schemas",
            "common.py": "# Common response schemas",
            "filters.py": "# Filter schemas",
            "pagination.py": "# Pagination schemas",
            "errors.py": "# Error response schemas",
        },
        
        # Shared services
        "services": {
            "__init__.py": "# Shared services",
            "database.py": "# Database service",
            "cache.py": "# Cache service",
            "ml_models.py": "# ML model service",
            "external_api.py": "# External API integrations",
        },
    },
    
    # Tests for v2
    "tests/v2": {
        "__init__.py": "# Tests for API v2",
        "conftest.py": "# Pytest configuration",
        
        "test_territories": {
            "__init__.py": "# Territory tests",
            "test_search.py": "# Search endpoint tests",
            "test_hexagons.py": "# Hexagon endpoint tests",
        },
        
        "test_insights": {
            "__init__.py": "# Insights tests",
            "test_competition.py": "# Competition analysis tests",
            "test_potential.py": "# Potential analysis tests",
        },
        
        "test_decisions": {
            "__init__.py": "# Decision tests",
            "test_recommendations.py": "# Recommendation tests",
            "test_scoring.py": "# Scoring tests",
        },
        
        "test_integration": {
            "__init__.py": "# Integration tests",
            "test_full_workflow.py": "# Full workflow tests",
            "test_permissions.py": "# Permission tests",
        },
    },
    
    # Configuration files
    "config": {
        "v2_config.py": "# Configuration for v2 API",
        "permission_aggregates.py": "# Permission aggregate definitions",
        "rate_limits.py": "# Rate limiting configuration",
    },
    
    # Database migrations for v2
    "migrations/v2": {
        "__init__.py": "# V2 migrations",
        "001_add_v2_tables.sql": "-- Initial v2 tables",
        "002_add_indexes.sql": "-- Performance indexes",
    },
}

def create_structure(base_path: Path, structure: dict, indent: int = 0):
    """Рекурсивно створює структуру папок та файлів"""
    
    for name, content in structure.items():
        path = base_path / name
        
        if isinstance(content, dict):
            # Це папка
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
                print(f"{'  ' * indent}📁 {name}/")
            else:
                print(f"{'  ' * indent}📁 {name}/ (exists)")
            
            # Рекурсивно створюємо вміст папки
            create_structure(path, content, indent + 1)
            
        else:
            # Це файл
            if not path.exists():
                path.write_text(f'"""\n{content}\nCreated for API v2 Domain-Driven Architecture\n"""\n', encoding='utf-8')
                print(f"{'  ' * indent}📄 {name}")
            else:
                print(f"{'  ' * indent}📄 {name} (exists)")

def create_readme():
    """Створює README для v2 API"""
    readme_content = """# API v2 Structure

## Domain-Driven Design Architecture

### Domains:
1. **Territories** - Discovery phase (search, hexagons, clusters)
2. **Insights** - Analysis phase (competition, potential, reports)
3. **Decisions** - Decision support (recommendations, scoring, scenarios)
4. **Portfolio** - Management (locations, contracts, projects)
5. **Admin** - System administration

### Key Directories:
- `core/` - Shared utilities and base classes
- `middleware/` - Request/response middleware
- `schemas/` - Shared Pydantic models
- `services/` - Shared business services

### Usage:
```python
from api.v2.territories.router import router as territories_router
from api.v2.insights.router import router as insights_router

app.include_router(territories_router, prefix="/api/v2/territories")
app.include_router(insights_router, prefix="/api/v2/insights")
```

### Testing:
```bash
pytest tests/v2/
```
"""
    
    readme_path = BASE_DIR / "api/v2/README.md"
    readme_path.write_text(readme_content, encoding='utf-8')
    print(f"\n📚 Created README.md for API v2")

def main():
    """Головна функція"""
    print("="*60)
    print("🚀 Creating API v2 Structure")
    print("="*60)
    
    # Створюємо структуру
    create_structure(BASE_DIR, API_V2_STRUCTURE)
    
    # Створюємо README
    create_readme()
    
    print("\n" + "="*60)
    print("✅ API v2 structure created successfully!")
    print("="*60)
    
    # Статистика
    total_dirs = sum(1 for _ in Path(BASE_DIR / "api/v2").rglob("*/"))
    total_files = sum(1 for _ in Path(BASE_DIR / "api/v2").rglob("*.py"))
    
    print(f"\n📊 Statistics:")
    print(f"   - Directories created: {total_dirs}")
    print(f"   - Python files created: {total_files}")
    print(f"   - Location: {BASE_DIR / 'api/v2'}")
    
    print("\n🎯 Next steps:")
    print("   1. Implement core/dependencies.py (auth dependencies)")
    print("   2. Implement core/responses.py (standard responses)")
    print("   3. Create middleware/permissions.py")
    print("   4. Start with territories/endpoints/search.py")

if __name__ == "__main__":
    main()