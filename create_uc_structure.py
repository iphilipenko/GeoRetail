#!/usr/bin/env python3
"""
Create UC-driven API structure for GeoRetail v2
Generates complete folder structure with all necessary files following Python best practices

Usage:
    python create_uc_structure.py [options]
    
Options:
    --base-dir PATH    Base directory (default: src/api/v2)
    --dry-run         Show what would be created without creating
    --force           Overwrite existing files
    --verbose         Detailed output
"""

import os
import sys
from pathlib import Path
from datetime import datetime
import argparse
import json
import shutil
from typing import Dict, List, Tuple

# ANSI color codes for pretty output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

# UC Structure Definition
UC_STRUCTURE = {
    "uc_explorer": {
        "description": "Explorer Mode - Visual territory analysis",
        "prefix": "explorer",
        "tags": ["Explorer", "Territory Discovery"],
        "modules": {
            "map": {
                "endpoints": ["initial_load", "viewport", "drill_down"],
                "description": "Map data loading and navigation"
            },
            "layers": {
                "endpoints": ["admin_units", "hexagons", "poi", "competitors"],
                "description": "Map layers management"
            },
            "metrics": {
                "endpoints": ["bivariate", "available", "calculate"],
                "description": "Metrics calculation and configuration"
            },
            "details": {
                "endpoints": ["territory", "hexagon", "statistics"],
                "description": "Detailed information retrieval"
            }
        }
    },
    "uc_screening": {
        "description": "Screening Mode - Batch location assessment",
        "prefix": "screening",
        "tags": ["Screening", "Batch Processing"],
        "modules": {
            "setup": {
                "endpoints": ["templates", "criteria", "filters"],
                "description": "Screening configuration"
            },
            "batch": {
                "endpoints": ["score", "progress", "results"],
                "description": "Batch processing operations",
                "extra_files": ["tasks.py"]  # For Celery
            },
            "analysis": {
                "endpoints": ["heatmap", "top_locations", "filter"],
                "description": "Analysis and visualization"
            },
            "export": {
                "endpoints": ["shortlist", "add_to_project"],
                "description": "Export and project management"
            }
        }
    },
    "uc_comparison": {
        "description": "Comparison Mode - Detailed location comparison",
        "prefix": "comparison",
        "tags": ["Comparison", "Decision Support"],
        "modules": {
            "locations": {
                "endpoints": ["add", "remove", "list"],
                "description": "Location management for comparison"
            },
            "analysis": {
                "endpoints": ["spider_chart", "side_by_side", "cannibalization", "roi_forecast"],
                "description": "Comparative analysis"
            },
            "ml": {
                "endpoints": ["predict_revenue", "confidence_scores", "similar_locations"],
                "description": "Machine learning predictions",
                "extra_files": ["models.py", "preprocessors.py"]
            },
            "reports": {
                "endpoints": ["generate", "download", "templates"],
                "description": "Report generation",
                "extra_files": ["generators.py", "templates/"]
            }
        }
    }
}

# File Templates
TEMPLATES = {
    "__init__.py": '''"""
{description}
Created: {date}
Part of GeoRetail v2 UC-driven Architecture
"""

__version__ = "2.0.0"
__all__ = [{exports}]
''',

    "router.py": '''"""
{uc_name} Main Router
Aggregates all sub-routers for the use case
"""

from fastapi import APIRouter
import logging

{imports}

logger = logging.getLogger(__name__)

# Create main router
router = APIRouter(
    prefix="/{prefix}",
    tags={tags}
)

# Include sub-routers
{router_includes}

# Health check endpoint
@router.get("/health")
async def health_check():
    """Check if {uc_name} endpoints are operational"""
    return {{
        "status": "healthy",
        "use_case": "{uc_name}",
        "version": "2.0.0"
    }}
''',

    "endpoints.py": '''"""
{module_name} Endpoints
{description}
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import {schema_imports}
from .services import {service_class}

logger = logging.getLogger(__name__)

router = APIRouter()
service = {service_class}()

{endpoint_functions}
''',

    "services.py": '''"""
{module_name} Business Logic
{description}
"""

from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)


class {service_class}:
    """Service class for {module_name}"""
    
    def __init__(self):
        """Initialize {module_name} service"""
        self.cache_ttl = 300  # 5 minutes default cache
        logger.info(f"Initialized {service_class}")
    
    {service_methods}
''',

    "schemas.py": '''"""
{uc_name} Pydantic Schemas
Data models for request/response validation
"""

from pydantic import BaseModel, Field, ConfigDict, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from enum import Enum

{schema_classes}
''',

    "dependencies.py": '''"""
{uc_name} Dependencies
Dependency injection for {uc_name} use case
"""

from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user
from src.models.rbac_models import RBACUser

logger = logging.getLogger(__name__)

security = HTTPBearer()


async def verify_{prefix}_access(
    current_user: RBACUser = Depends(get_current_user)
) -> RBACUser:
    """Verify user has access to {uc_name} features"""
    required_permissions = [
        "core.view_map",
        "{prefix}.access"
    ]
    
    user_permissions = set(current_user.permissions)
    if not any(perm in user_permissions for perm in required_permissions):
        logger.warning(f"User {{current_user.email}} denied access to {uc_name}")
        raise HTTPException(
            status_code=403,
            detail="Insufficient permissions for {uc_name}"
        )
    
    return current_user


async def get_{prefix}_settings(
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get {uc_name} configuration settings"""
    # TODO: Load from database or config
    return {{
        "enabled": True,
        "max_batch_size": 1000,
        "cache_ttl": 300
    }}
''',

    "utils.py": '''"""
{module_name} Utility Functions
Helper functions for {description}
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
import hashlib
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def calculate_cache_key(*args, **kwargs) -> str:
    """Generate cache key from arguments"""
    key_data = {{
        "args": args,
        "kwargs": kwargs
    }}
    key_string = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.md5(key_string.encode()).hexdigest()


def parse_bounds(bounds_str: str) -> Tuple[float, float, float, float]:
    """Parse boundary string 'minLon,minLat,maxLon,maxLat'"""
    try:
        parts = bounds_str.split(',')
        if len(parts) != 4:
            raise ValueError("Bounds must have 4 coordinates")
        return tuple(map(float, parts))
    except (ValueError, AttributeError) as e:
        logger.error(f"Invalid bounds format: {{bounds_str}}")
        raise ValueError(f"Invalid bounds format: {{e}}")


def format_response(
    data: Any,
    success: bool = True,
    message: Optional[str] = None,
    metadata: Optional[Dict] = None
) -> Dict[str, Any]:
    """Format standardized API response"""
    response = {{
        "success": success,
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
    }}
    
    if message:
        response["message"] = message
    
    if metadata:
        response["metadata"] = metadata
    
    return response


# TODO: Add more utility functions as needed
''',

    "test.py": '''"""
Tests for {module_name}
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import {service_class}
from ...schemas import *


class Test{module_name}:
    """Test suite for {module_name}"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return {service_class}()
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database session"""
        return Mock()
    
    @pytest.fixture
    def mock_user(self):
        """Create mock authenticated user"""
        return Mock(
            id=1,
            email="test@example.com",
            permissions=["core.view_map", "{prefix}.access"]
        )
    
    {test_methods}
    
    # TODO: Add more test cases
''',

    "config.py": '''"""
{uc_name} Configuration
Configuration settings for {uc_name}
"""

from pydantic_settings import BaseSettings
from typing import Optional


class {uc_name}Config(BaseSettings):
    """Configuration for {uc_name}"""
    
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
        env_prefix = "{prefix_upper}_"
        env_file = ".env"


# Singleton instance
config = {uc_name}Config()
''',

    "README.md": '''# {uc_name}

## Overview
{description}

## Structure
```
{structure_tree}
```

## Endpoints

{endpoints_list}

## Usage

### Example Request
```python
import requests

response = requests.get(
    "http://localhost:8000/api/v2/{prefix}/example",
    headers={{"Authorization": "Bearer YOUR_TOKEN"}}
)
```

## Development

### Running Tests
```bash
pytest tests/test_{prefix}/
```

### Adding New Endpoints
1. Define schema in `schemas.py`
2. Add endpoint in `{module}/endpoints.py`
3. Implement logic in `{module}/services.py`
4. Add tests in `tests/test_{module}.py`

## Dependencies
- FastAPI
- SQLAlchemy
- Pydantic
- Redis (for caching)
'''
}


def generate_endpoint_function(endpoint_name: str, module_name: str, uc_prefix: str) -> str:
    """Generate endpoint function code"""
    function_name = endpoint_name.replace('_', ' ').title().replace(' ', '')
    
    return f'''
@router.get("/{endpoint_name}")
async def get_{endpoint_name}(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("{uc_prefix}.{endpoint_name}"))
) -> {function_name}Response:
    """
    Get {endpoint_name.replace('_', ' ')} for {module_name}
    
    Required permission: {uc_prefix}.{endpoint_name}
    """
    try:
        result = await service.get_{endpoint_name}(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return {function_name}Response(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_{endpoint_name}: {{e}}")
        raise HTTPException(status_code=500, detail=str(e))
'''


def generate_service_method(endpoint_name: str, module_name: str) -> str:
    """Generate service method code"""
    return f'''
    async def get_{endpoint_name}(
        self,
        db: Session,
        user_id: int,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get {endpoint_name.replace('_', ' ')} data
        
        Args:
            db: Database session
            user_id: Current user ID
            limit: Maximum items to return
            offset: Number of items to skip
            
        Returns:
            List of {endpoint_name} items
        """
        try:
            # TODO: Implement actual logic
            logger.info(f"Getting {{endpoint_name}} for user {{user_id}}")
            
            # Example query - Note: for async use, need async SQLAlchemy session
            # For now using sync approach which is common in FastAPI
            query = """
                SELECT * FROM example_table
                WHERE user_id = :user_id
                LIMIT :limit OFFSET :offset
            """
            
            # Note: In production, use async session:
            # result = await db.execute(text(query), params)
            # For sync session (common pattern):
            result = db.execute(
                text(query),
                {{"user_id": user_id, "limit": limit, "offset": offset}}
            )
            
            return [dict(row) for row in result]
            
        except Exception as e:
            logger.error(f"Error getting {{endpoint_name}}: {{e}}")
            raise
'''


def generate_schema_class(endpoint_name: str, module_name: str) -> str:
    """Generate Pydantic schema class"""
    class_name = endpoint_name.replace('_', ' ').title().replace(' ', '')
    
    return f'''
class {class_name}Request(BaseModel):
    """Request schema for {endpoint_name}"""
    model_config = ConfigDict(from_attributes=True)
    
    # TODO: Define request fields
    example_field: str = Field(..., description="Example required field")
    optional_field: Optional[int] = Field(None, description="Example optional field")


class {class_name}Response(BaseModel):
    """Response schema for {endpoint_name}"""
    model_config = ConfigDict(from_attributes=True)
    
    success: bool = Field(True, description="Operation success status")
    data: List[Dict[str, Any]] = Field(..., description="Response data")
    total: int = Field(..., description="Total items count")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
'''


def generate_test_method(endpoint_name: str, module_name: str) -> str:
    """Generate test method code"""
    return f'''
    async def test_get_{endpoint_name}(self, service, mock_db, mock_user):
        """Test get_{endpoint_name} endpoint"""
        # Arrange
        expected_data = [{{"id": 1, "name": "Test"}}]
        service.get_{endpoint_name} = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_{endpoint_name}(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_{endpoint_name}.assert_called_once()
'''


class StructureCreator:
    """Main class for creating UC structure"""
    
    def __init__(self, base_dir: Path, dry_run: bool = False, force: bool = False, verbose: bool = False):
        self.base_dir = base_dir
        self.dry_run = dry_run
        self.force = force
        self.verbose = verbose
        self.created_dirs = 0
        self.created_files = 0
        self.skipped_files = 0
        self.date_str = datetime.now().strftime("%Y-%m-%d")
        self.backup_created = False
        
    def check_write_permissions(self) -> bool:
        """Check if we have write permissions to base directory"""
        if self.dry_run:
            return True
            
        # Create parent directories if needed
        self.base_dir.parent.mkdir(parents=True, exist_ok=True)
        
        # Test write permissions
        test_file = self.base_dir.parent / f".write_test_{datetime.now().timestamp()}"
        try:
            test_file.touch()
            test_file.unlink()
            return True
        except PermissionError:
            self.log(f"No write permissions in {self.base_dir.parent}", "error")
            return False
    
    def create_backup(self) -> bool:
        """Create backup of existing structure before modifications"""
        if self.dry_run or not self.base_dir.exists():
            return True
            
        backup_dir = self.base_dir.parent / f"{self.base_dir.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            self.log(f"Creating backup to {backup_dir}...", "info")
            shutil.copytree(self.base_dir, backup_dir)
            self.backup_created = True
            self.log(f"Backup created successfully: {backup_dir}", "success")
            return True
        except Exception as e:
            self.log(f"Failed to create backup: {e}", "error")
            return False
    
    def validate_uc_structure(self) -> bool:
        """Validate UC structure configuration"""
        required_fields = ["description", "prefix", "tags", "modules"]
        
        for uc_name, uc_config in UC_STRUCTURE.items():
            for field in required_fields:
                if field not in uc_config:
                    raise ValueError(f"Missing required field '{field}' in {uc_name} configuration")
            
            # Validate modules
            if not uc_config["modules"]:
                raise ValueError(f"No modules defined for {uc_name}")
                
            for module_name, module_config in uc_config["modules"].items():
                if "endpoints" not in module_config:
                    raise ValueError(f"No endpoints defined for {uc_name}.{module_name}")
                if "description" not in module_config:
                    raise ValueError(f"No description for {uc_name}.{module_name}")
        
        self.log("UC structure configuration validated", "success")
        return True
    
    def log(self, message: str, level: str = "info"):
        """Pretty logging with colors"""
        if level == "success":
            prefix = f"{Colors.GREEN}✅{Colors.RESET}"
        elif level == "warning":
            prefix = f"{Colors.YELLOW}⚠️{Colors.RESET}"
        elif level == "error":
            prefix = f"{Colors.RED}❌{Colors.RESET}"
        elif level == "create":
            prefix = f"{Colors.BLUE}📁{Colors.RESET}"
        elif level == "file":
            prefix = f"{Colors.CYAN}📄{Colors.RESET}"
        else:
            prefix = f"{Colors.WHITE}ℹ️{Colors.RESET}"
        
        print(f"{prefix} {message}")
    
    def create_directory(self, path: Path) -> bool:
        """Create a directory"""
        if self.dry_run:
            self.log(f"[DRY RUN] Would create directory: {path}", "create")
            return True
        
        try:
            path.mkdir(parents=True, exist_ok=True)
            if self.verbose:
                self.log(f"Created directory: {path}", "create")
            self.created_dirs += 1
            return True
        except Exception as e:
            self.log(f"Failed to create directory {path}: {e}", "error")
            return False
    
    def create_file(self, path: Path, content: str) -> bool:
        """Create a file with content"""
        if path.exists() and not self.force:
            if self.verbose:
                self.log(f"Skipped existing file: {path}", "warning")
            self.skipped_files += 1
            return False
        
        if self.dry_run:
            self.log(f"[DRY RUN] Would create file: {path}", "file")
            return True
        
        try:
            path.write_text(content, encoding='utf-8')
            if self.verbose:
                self.log(f"Created file: {path}", "file")
            self.created_files += 1
            return True
        except Exception as e:
            self.log(f"Failed to create file {path}: {e}", "error")
            return False
    
    def create_uc_structure(self):
        """Create the complete UC structure"""
        self.log(f"{Colors.BOLD}🚀 Creating UC-driven API structure...{Colors.RESET}", "info")
        
        # Validate structure configuration
        try:
            self.validate_uc_structure()
        except ValueError as e:
            self.log(f"Invalid configuration: {e}", "error")
            raise
        
        # Check write permissions
        if not self.check_write_permissions():
            raise PermissionError("No write permissions to target directory")
        
        # Create backup if needed
        if not self.force and self.base_dir.exists():
            if not self.create_backup():
                response = input("Continue without backup? (y/n): ")
                if response.lower() != 'y':
                    raise Exception("Aborted: backup failed")
        
        # Create shared components first
        self.create_shared_components()
        
        # Create each UC module
        for uc_name, uc_config in UC_STRUCTURE.items():
            self.create_uc_module(uc_name, uc_config)
        
        # Create tests structure
        self.create_tests_structure()
        
        # Create root documentation
        self.create_root_documentation()
        
        # Print summary
        self.print_summary()
    
    def create_shared_components(self):
        """Create shared components directory"""
        shared_dir = self.base_dir / "shared"
        self.create_directory(shared_dir)
        
        # Create shared files
        shared_files = {
            "__init__.py": TEMPLATES["__init__.py"].format(
                description="Shared components for UC modules",
                date=self.date_str,
                exports=""
            ),
            "cache.py": self.generate_cache_utils(),
            "database.py": self.generate_database_utils(),
            "exceptions.py": self.generate_exceptions(),
            "validators.py": self.generate_validators(),
            "responses.py": self.generate_response_utils()
        }
        
        for filename, content in shared_files.items():
            self.create_file(shared_dir / filename, content)
    
    def create_uc_module(self, uc_name: str, uc_config: Dict):
        """Create a complete UC module"""
        uc_dir = self.base_dir / uc_name
        self.create_directory(uc_dir)
        
        # Create root UC files
        self.create_uc_root_files(uc_dir, uc_name, uc_config)
        
        # Create each sub-module
        for module_name, module_config in uc_config["modules"].items():
            self.create_module(uc_dir, uc_name, module_name, module_config, uc_config)
        
        # Create config file
        config_content = TEMPLATES["config.py"].format(
            uc_name=uc_name.replace('_', ' ').title(),
            prefix=uc_config["prefix"],
            prefix_upper=uc_config["prefix"].upper()
        )
        self.create_file(uc_dir / "config.py", config_content)
    
    def create_uc_root_files(self, uc_dir: Path, uc_name: str, uc_config: Dict):
        """Create root files for UC module"""
        
        # __init__.py
        init_content = TEMPLATES["__init__.py"].format(
            description=uc_config["description"],
            date=self.date_str,
            exports="'router'"
        )
        self.create_file(uc_dir / "__init__.py", init_content)
        
        # router.py
        imports = []
        router_includes = []
        
        for module_name in uc_config["modules"]:
            imports.append(f"from .{module_name} import endpoints as {module_name}_endpoints")
            router_includes.append(
                f'router.include_router({module_name}_endpoints.router, prefix="/{module_name}", tags=["{module_name}"])'
            )
        
        router_content = TEMPLATES["router.py"].format(
            uc_name=uc_name.replace('_', ' ').title(),
            prefix=uc_config["prefix"],
            tags=str(uc_config["tags"]),
            imports='\n'.join(imports),
            router_includes='\n'.join(router_includes)
        )
        self.create_file(uc_dir / "router.py", router_content)
        
        # schemas.py
        schema_classes = []
        for module_name, module_config in uc_config["modules"].items():
            for endpoint in module_config["endpoints"]:
                schema_classes.append(generate_schema_class(endpoint, module_name))
        
        schemas_content = TEMPLATES["schemas.py"].format(
            uc_name=uc_name.replace('_', ' ').title(),
            schema_classes='\n'.join(schema_classes)
        )
        self.create_file(uc_dir / "schemas.py", schemas_content)
        
        # dependencies.py
        dependencies_content = TEMPLATES["dependencies.py"].format(
            uc_name=uc_name.replace('_', ' ').title(),
            prefix=uc_config["prefix"],
            prefix_upper=uc_config["prefix"].upper()
        )
        self.create_file(uc_dir / "dependencies.py", dependencies_content)
    
    def create_module(self, uc_dir: Path, uc_name: str, module_name: str, module_config: Dict, uc_config: Dict):
        """Create a sub-module within UC"""
        module_dir = uc_dir / module_name
        self.create_directory(module_dir)
        
        # __init__.py
        init_content = TEMPLATES["__init__.py"].format(
            description=module_config["description"],
            date=self.date_str,
            exports="'router', 'service'"
        )
        self.create_file(module_dir / "__init__.py", init_content)
        
        # endpoints.py
        endpoint_functions = []
        schema_imports = []
        
        for endpoint in module_config["endpoints"]:
            endpoint_functions.append(generate_endpoint_function(endpoint, module_name, uc_config["prefix"]))
            class_name = endpoint.replace('_', ' ').title().replace(' ', '')
            schema_imports.extend([f"{class_name}Request", f"{class_name}Response"])
        
        endpoints_content = TEMPLATES["endpoints.py"].format(
            module_name=module_name.title(),
            description=module_config["description"],
            schema_imports=', '.join(schema_imports),
            service_class=f"{module_name.title()}Service",
            endpoint_functions=''.join(endpoint_functions)
        )
        self.create_file(module_dir / "endpoints.py", endpoints_content)
        
        # services.py
        service_methods = []
        for endpoint in module_config["endpoints"]:
            service_methods.append(generate_service_method(endpoint, module_name))
        
        services_content = TEMPLATES["services.py"].format(
            module_name=module_name.title(),
            description=module_config["description"],
            service_class=f"{module_name.title()}Service",
            service_methods=''.join(service_methods)
        )
        self.create_file(module_dir / "services.py", services_content)
        
        # utils.py
        utils_content = TEMPLATES["utils.py"].format(
            module_name=module_name.title(),
            description=module_config["description"]
        )
        self.create_file(module_dir / "utils.py", utils_content)
        
        # Special handling for batch module - add Celery config
        if uc_name == "uc_screening" and module_name == "batch":
            celery_config = '''"""
Celery configuration for batch processing
"""

from celery import Celery
from kombu import Queue
import os

# Create Celery app
app = Celery('georetail_screening')

# Configure from environment or defaults
app.conf.update(
    broker_url=os.getenv('CELERY_BROKER_URL', 'redis://localhost:6379/1'),
    result_backend=os.getenv('CELERY_RESULT_BACKEND', 'redis://localhost:6379/2'),
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    timezone='UTC',
    enable_utc=True,
    task_routes={
        'screening.batch.*': {'queue': 'batch_processing'},
        'screening.export.*': {'queue': 'export_tasks'},
    },
    task_queues=(
        Queue('batch_processing', routing_key='batch.#'),
        Queue('export_tasks', routing_key='export.#'),
    ),
    task_default_queue='default',
    task_default_exchange='tasks',
    task_default_routing_key='task.default',
)

# Auto-discover tasks
app.autodiscover_tasks(['src.api.v2.uc_screening.batch'])
'''
            self.create_file(module_dir / "celery_config.py", celery_config)
        
        # Extra files if specified
        if "extra_files" in module_config:
            for extra_file in module_config["extra_files"]:
                if extra_file.endswith('/'):
                    # It's a directory
                    self.create_directory(module_dir / extra_file)
                elif extra_file == "tasks.py" and module_name == "batch":
                    # Special handling for Celery tasks
                    tasks_content = '''"""
Celery tasks for batch processing
"""

from celery import shared_task
from typing import Dict, List, Any
import logging
import time

logger = logging.getLogger(__name__)


@shared_task(bind=True, name='screening.batch.score_locations')
def score_locations_task(self, territory_id: str, criteria: Dict, store_template: Dict) -> Dict:
    """
    Async task for batch scoring locations
    
    Args:
        territory_id: Territory to analyze
        criteria: Scoring criteria
        store_template: Store parameters template
        
    Returns:
        Dict with results and job status
    """
    try:
        # Update task state
        self.update_state(state='PROGRESS', meta={'current': 0, 'total': 100})
        
        # TODO: Implement actual scoring logic
        logger.info(f"Starting batch scoring for territory {territory_id}")
        
        # Simulate processing
        for i in range(100):
            time.sleep(0.1)  # Simulate work
            self.update_state(state='PROGRESS', meta={'current': i, 'total': 100})
        
        return {
            'status': 'completed',
            'results': [],  # TODO: Add actual results
            'total_processed': 100
        }
        
    except Exception as e:
        logger.error(f"Task failed: {e}")
        self.update_state(state='FAILURE', meta={'error': str(e)})
        raise


@shared_task(name='screening.export.generate_report')
def generate_report_task(location_ids: List[str], format: str = 'xlsx') -> str:
    """Generate export report for selected locations"""
    try:
        # TODO: Implement report generation
        logger.info(f"Generating {format} report for {len(location_ids)} locations")
        
        # Return file path or URL
        return f"/exports/report_{int(time.time())}.{format}"
        
    except Exception as e:
        logger.error(f"Export failed: {e}")
        raise
'''
                    self.create_file(module_dir / extra_file, tasks_content)
                else:
                    # Generic extra file
                    self.create_file(
                        module_dir / extra_file,
                        f'"""\n{extra_file} for {module_name}\nTODO: Implement\n"""\n'
                    )
    
    def create_tests_structure(self):
        """Create tests structure for all UC modules"""
        tests_dir = self.base_dir / "tests"
        self.create_directory(tests_dir)
        
        # Root conftest.py
        conftest_content = '''"""
Pytest configuration for UC tests
"""

import pytest
from unittest.mock import Mock
from sqlalchemy.orm import Session


@pytest.fixture
def mock_db():
    """Mock database session"""
    return Mock(spec=Session)


@pytest.fixture
def mock_user():
    """Mock authenticated user"""
    return Mock(
        id=1,
        email="test@georetail.com",
        username="testuser",
        permissions=["core.view_map", "explorer.access", "screening.access", "comparison.access"]
    )
'''
        self.create_file(tests_dir / "conftest.py", conftest_content)
        self.create_file(tests_dir / "__init__.py", '"""UC Tests"""')
        
        # Create test directories for each UC
        for uc_name, uc_config in UC_STRUCTURE.items():
            test_uc_dir = tests_dir / f"test_{uc_name}"
            self.create_directory(test_uc_dir)
            self.create_file(test_uc_dir / "__init__.py", f'"""Tests for {uc_name}"""')
            
            # Create test files for each module
            for module_name, module_config in uc_config["modules"].items():
                test_methods = []
                for endpoint in module_config["endpoints"]:
                    test_methods.append(generate_test_method(endpoint, module_name))
                
                test_content = TEMPLATES["test.py"].format(
                    module_name=module_name.title(),
                    service_class=f"{module_name.title()}Service",
                    prefix=uc_config["prefix"],
                    test_methods=''.join(test_methods)
                )
                self.create_file(test_uc_dir / f"test_{module_name}.py", test_content)
    
    def create_root_documentation(self):
        """Create root documentation files"""
        # Main README
        readme_content = '''# UC-Driven API v2 Architecture

## Overview
This is the new Use Case-driven architecture for GeoRetail v2 API, organized around three main workflows:

1. **Explorer Mode** - Visual territory analysis
2. **Screening Mode** - Batch location assessment  
3. **Comparison Mode** - Detailed location comparison

## Structure
```
src/api/v2/
├── uc_explorer/       # UC1: Visual analysis
├── uc_screening/      # UC2: Batch assessment
├── uc_comparison/     # UC3: Location comparison
├── shared/            # Shared components
└── tests/             # Test suites
```

## Quick Start

### 1. Register routers in main.py
```python
from api.v2.uc_explorer.router import router as explorer_router
from api.v2.uc_screening.router import router as screening_router
from api.v2.uc_comparison.router import router as comparison_router

app.include_router(explorer_router, prefix="/api/v2/explorer")
app.include_router(screening_router, prefix="/api/v2/screening")
app.include_router(comparison_router, prefix="/api/v2/comparison")
```

### 2. Run tests
```bash
pytest src/api/v2/tests/
```

### 3. Access documentation
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Development Guidelines

1. Each UC is independent and self-contained
2. Shared logic goes in `shared/` directory
3. Follow the established patterns in templates
4. Write tests for all new endpoints
5. Document all API changes

## Migration from DDD to UC

See [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md) for details on migrating from the old Domain-Driven Design to the new Use Case architecture.
'''
        self.create_file(self.base_dir / "README.md", readme_content)
        
        # UC Architecture detailed documentation
        uc_architecture_content = '''# UC Architecture Documentation
## Detailed Use Case-Driven Architecture for GeoRetail v2

### Architecture Philosophy

The UC-driven architecture organizes code around user workflows rather than technical domains. 
This approach aligns development with actual user needs and usage patterns.

### Use Case Definitions

#### UC1: Explorer Mode
**Purpose:** Visual exploration and discovery of territories
**Primary Users:** Marketing analysts, expansion managers
**Key Features:**
- Interactive map with multiple data layers
- Drill-down navigation from country to H3-10
- Real-time metric calculations
- Bivariate choropleth visualization

#### UC2: Screening Mode  
**Purpose:** Batch assessment of multiple locations
**Primary Users:** Expansion teams, data analysts
**Key Features:**
- Batch scoring of 100+ locations
- ML-powered predictions
- Heatmap generation
- Automated filtering and ranking

#### UC3: Comparison Mode
**Purpose:** Detailed comparison of finalist locations
**Primary Users:** Decision makers, executives
**Key Features:**
- Side-by-side comparison
- Spider chart visualization
- Cannibalization analysis
- ROI forecasting

### Technical Architecture

#### Layered Structure
```
Presentation Layer (Endpoints)
    ↓
Business Logic Layer (Services)
    ↓
Data Access Layer (Database/Cache)
```

#### Service Communication
- Services are independent and communicate through well-defined interfaces
- Shared components in `shared/` directory provide common functionality
- Each UC has its own schema definitions to maintain independence

#### Caching Strategy
- Redis for real-time data (TTL: 5 minutes)
- Pre-calculated metrics in ClickHouse
- Client-side caching for static data

#### Security Model
- JWT-based authentication
- RBAC permission system
- UC-specific permission checks
- Audit logging for all operations

### Performance Targets

| Use Case | Response Time | Concurrent Users |
|----------|---------------|------------------|
| Explorer | < 200ms | 100 |
| Screening | < 5s (batch) | 20 |
| Comparison | < 500ms | 50 |

### Scalability Considerations

1. **Horizontal Scaling:** Each UC can be deployed independently
2. **Database Sharding:** H3 data partitioned by resolution
3. **Async Processing:** Celery for batch operations
4. **CDN:** Static assets and map tiles

### Monitoring and Observability

- Prometheus metrics for performance
- ELK stack for logging
- Sentry for error tracking
- Custom dashboards for business metrics

### Development Workflow

1. **Feature Development:**
   - Create feature branch from develop
   - Implement in relevant UC module
   - Write tests (minimum 80% coverage)
   - Create/update API documentation

2. **Code Review:**
   - PR requires 2 approvals
   - Automated tests must pass
   - Performance benchmarks checked

3. **Deployment:**
   - Staging environment first
   - Smoke tests
   - Gradual rollout to production

### API Versioning Strategy

- Current version: v2
- Deprecation notice: 3 months
- Backward compatibility: 6 months
- Version sunset: 12 months

### Dependencies

#### External Services
- PostgreSQL with PostGIS
- ClickHouse for analytics
- Redis for caching
- Celery + RabbitMQ for async tasks

#### Python Packages
- FastAPI >= 0.104.0
- SQLAlchemy >= 2.0.0
- Pydantic >= 2.0.0
- Redis >= 5.0.0
- Celery >= 5.3.0

### Future Roadmap

#### Q1 2025
- Complete UC implementation
- Performance optimization
- Enhanced caching

#### Q2 2025
- GraphQL API addition
- Real-time WebSocket updates
- Advanced ML models

#### Q3 2025
- Mobile API optimization
- Offline mode support
- International expansion

### Contact

- Technical Lead: tech-lead@georetail.com
- API Support: api-support@georetail.com
- Documentation: docs@georetail.com
'''
        self.create_file(self.base_dir / "UC_ARCHITECTURE.md", uc_architecture_content)
        
        # Migration Guide
        migration_content = '''# Migration Guide: DDD → UC Architecture

## Overview
This guide helps migrate from the old Domain-Driven Design to the new Use Case-driven architecture.

## Mapping Old to New

| Old DDD Endpoint | New UC Endpoint | Notes |
|-----------------|-----------------|-------|
| /territories/admin/geometries/all | /explorer/map/initial_load | Combined with metrics |
| /territories/h3/grid | /explorer/layers/hexagons | Zoom-based filtering |
| /territories/poi/search | /explorer/layers/poi | Viewport optimized |
| /territories/bivariate/config | /explorer/metrics/bivariate | Same logic |

## Migration Steps

### Phase 1: Aliasing (Day 1)
1. Create aliases for existing endpoints
2. No logic changes
3. Both old and new URLs work

### Phase 2: Refactoring (Week 1)
1. Move logic to UC services
2. Optimize for use cases
3. Add caching

### Phase 3: Deprecation (Month 1)
1. Add deprecation warnings
2. Update documentation
3. Notify frontend team

### Phase 4: Removal (Month 3)
1. Remove old endpoints
2. Clean up code
3. Final testing

## Code Examples

### Old Way (DDD)
```python
@router.get("/territories/admin/{id}")
async def get_territory(id: str):
    return territory_service.get_by_id(id)
```

### New Way (UC)
```python
@router.get("/explorer/details/territory/{id}")
async def get_territory_details(
    id: str,
    include_children: bool = False,
    include_metrics: bool = True
):
    return explorer_service.get_territory_with_context(
        id, include_children, include_metrics
    )
```

## Testing
Run both old and new endpoints in parallel:
```bash
# Test old endpoints
pytest tests/test_territories/

# Test new endpoints  
pytest src/api/v2/tests/
```
'''
        self.create_file(self.base_dir / "MIGRATION_GUIDE.md", migration_content)
    
    def generate_cache_utils(self) -> str:
        """Generate cache utilities"""
        return '''"""
Cache utilities for UC modules
Redis-based caching with TTL support
"""

import redis
import json
import hashlib
from typing import Any, Optional
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.Redis(
    host='localhost',
    port=6379,
    db=0,
    decode_responses=True
)


class CacheManager:
    """Centralized cache management"""
    
    @staticmethod
    def get_key(*args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_data = {"args": args, "kwargs": kwargs}
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return f"georetail:v2:{hashlib.md5(key_string.encode()).hexdigest()}"
    
    @staticmethod
    def get(key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            value = redis_client.get(key)
            if value:
                return json.loads(value)
        except Exception as e:
            logger.error(f"Cache get error: {e}")
        return None
    
    @staticmethod
    def set(key: str, value: Any, ttl: int = 300):
        """Set value in cache with TTL"""
        try:
            redis_client.setex(
                key,
                timedelta(seconds=ttl),
                json.dumps(value, default=str)
            )
        except Exception as e:
            logger.error(f"Cache set error: {e}")
    
    @staticmethod
    def delete(key: str):
        """Delete key from cache"""
        try:
            redis_client.delete(key)
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
    
    @staticmethod
    def clear_pattern(pattern: str):
        """Clear all keys matching pattern"""
        try:
            for key in redis_client.scan_iter(f"georetail:v2:{pattern}*"):
                redis_client.delete(key)
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
'''
    
    def generate_database_utils(self) -> str:
        """Generate database utilities"""
        return '''"""
Database utilities for UC modules
Connection pooling and query helpers
"""

from typing import Dict, List, Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)


class DatabaseHelper:
    """Database query helpers"""
    
    @staticmethod
    def execute_query(
        db: Session,
        query: str,
        params: Dict = None
    ) -> List[Dict]:
        """Execute query and return results as dict list"""
        try:
            result = db.execute(text(query), params or {})
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise
    
    @staticmethod
    def get_admin_units(
        db: Session,
        level: Optional[str] = None,
        parent_id: Optional[int] = None
    ) -> List[Dict]:
        """Get administrative units"""
        query = """
            SELECT 
                id,
                name_uk,
                name_en,
                admin_level,
                parent_id,
                ST_AsGeoJSON(geometry) as geometry
            FROM osm_ukraine.admin_boundaries
            WHERE 1=1
        """
        
        params = {}
        if level:
            query += " AND admin_level = :level"
            params["level"] = level
        
        if parent_id:
            query += " AND parent_id = :parent_id"
            params["parent_id"] = parent_id
        
        return DatabaseHelper.execute_query(db, query, params)
    
    @staticmethod
    def get_hexagons_in_bounds(
        db: Session,
        bounds: tuple,
        resolution: int
    ) -> List[Dict]:
        """Get H3 hexagons within bounds"""
        query = """
            SELECT 
                h3_index,
                resolution,
                ST_X(center_point) as lon,
                ST_Y(center_point) as lat
            FROM h3_data.h3_grid
            WHERE resolution = :resolution
            AND ST_Intersects(
                geom,
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            )
        """
        
        params = {
            "resolution": resolution,
            "min_lon": bounds[0],
            "min_lat": bounds[1],
            "max_lon": bounds[2],
            "max_lat": bounds[3]
        }
        
        return DatabaseHelper.execute_query(db, query, params)
'''
    
    def generate_exceptions(self) -> str:
        """Generate custom exceptions"""
        return '''"""
Custom exceptions for UC modules
"""


class UCException(Exception):
    """Base exception for UC modules"""
    pass


class ValidationException(UCException):
    """Validation error"""
    pass


class PermissionException(UCException):
    """Permission denied"""
    pass


class DataNotFoundException(UCException):
    """Data not found"""
    pass


class ProcessingException(UCException):
    """Processing error"""
    pass


class ExternalServiceException(UCException):
    """External service error"""
    pass
'''
    
    def generate_validators(self) -> str:
        """Generate validators"""
        return '''"""
Common validators for UC modules
"""

from typing import Tuple
import re


def validate_bounds(bounds_str: str) -> Tuple[float, float, float, float]:
    """Validate and parse bounds string"""
    pattern = r'^-?\\d+\\.\\d+,-?\\d+\\.\\d+,-?\\d+\\.\\d+,-?\\d+\\.\\d+$'
    if not re.match(pattern, bounds_str):
        raise ValueError(f"Invalid bounds format: {bounds_str}")
    
    parts = list(map(float, bounds_str.split(',')))
    min_lon, min_lat, max_lon, max_lat = parts
    
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError(f"Longitude out of range: {min_lon}, {max_lon}")
    
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError(f"Latitude out of range: {min_lat}, {max_lat}")
    
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError(f"Invalid bounds: min >= max")
    
    return (min_lon, min_lat, max_lon, max_lat)


def validate_h3_index(h3_index: str) -> bool:
    """Validate H3 index format"""
    pattern = r'^[0-9a-f]{15}$'
    return bool(re.match(pattern, h3_index.lower()))


def validate_resolution(resolution: int) -> bool:
    """Validate H3 resolution"""
    return 4 <= resolution <= 15
'''
    
    def generate_response_utils(self) -> str:
        """Generate response utilities"""
        return '''"""
Response formatting utilities
"""

from typing import Any, Dict, Optional, List
from datetime import datetime


def success_response(
    data: Any,
    message: Optional[str] = None,
    metadata: Optional[Dict] = None,
    total: Optional[int] = None
) -> Dict:
    """Format success response"""
    response = {
        "success": True,
        "data": data,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if message:
        response["message"] = message
    
    if metadata:
        response["metadata"] = metadata
    
    if total is not None:
        response["total"] = total
    
    return response


def error_response(
    error: str,
    code: Optional[str] = None,
    details: Optional[Dict] = None
) -> Dict:
    """Format error response"""
    response = {
        "success": False,
        "error": error,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if code:
        response["code"] = code
    
    if details:
        response["details"] = details
    
    return response


def paginated_response(
    items: List[Any],
    page: int,
    limit: int,
    total: int
) -> Dict:
    """Format paginated response"""
    return {
        "success": True,
        "data": items,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit
        },
        "timestamp": datetime.utcnow().isoformat()
    }
'''
    
    def print_summary(self):
        """Print creation summary"""
        print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
        print(f"{Colors.GREEN}{Colors.BOLD}✅ UC Structure Creation Complete!{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*60}{Colors.RESET}\n")
        
        print(f"{Colors.CYAN}📊 Summary:{Colors.RESET}")
        print(f"   📁 Directories created: {Colors.BOLD}{self.created_dirs}{Colors.RESET}")
        print(f"   📄 Files created: {Colors.BOLD}{self.created_files}{Colors.RESET}")
        print(f"   ⚠️  Files skipped: {Colors.BOLD}{self.skipped_files}{Colors.RESET}")
        
        print(f"\n{Colors.CYAN}📦 Modules created:{Colors.RESET}")
        for uc_name in UC_STRUCTURE:
            print(f"   ✅ {uc_name}")
        
        print(f"\n{Colors.CYAN}🚀 Next steps:{Colors.RESET}")
        print(f"   1. Add routers to main.py")
        print(f"   2. Update .env with configuration")
        print(f"   3. Run tests: pytest src/api/v2/tests/")
        print(f"   4. Check API docs at /docs")
        
        print(f"\n{Colors.GREEN}Happy coding! 🎉{Colors.RESET}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Create UC-driven API structure for GeoRetail v2",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "--base-dir",
        default="src/api/v2",
        help="Base directory for structure creation (default: src/api/v2)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without actually creating files"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing files"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output for each file/directory created"
    )
    
    args = parser.parse_args()
    
    # Create base directory path
    base_dir = Path(args.base_dir)
    
    # Confirm action if not dry run
    if not args.dry_run:
        print(f"{Colors.YELLOW}⚠️  This will create structure in: {base_dir.absolute()}{Colors.RESET}")
        response = input(f"Continue? (y/n): ")
        if response.lower() != 'y':
            print(f"{Colors.RED}Aborted.{Colors.RESET}")
            sys.exit(1)
    
    # Create structure
    creator = StructureCreator(
        base_dir=base_dir,
        dry_run=args.dry_run,
        force=args.force,
        verbose=args.verbose
    )
    
    try:
        creator.create_uc_structure()
    except Exception as e:
        print(f"{Colors.RED}❌ Error: {e}{Colors.RESET}")
        sys.exit(1)


if __name__ == "__main__":
    main()