"""
Uc Comparison Dependencies
Dependency injection for Uc Comparison use case
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


async def verify_comparison_access(
    current_user: RBACUser = Depends(get_current_user)
) -> RBACUser:
    """Verify user has access to Uc Comparison features"""
    required_permissions = [
        "core.view_map",
        "comparison.access"
    ]
    
    user_permissions = set(current_user.permissions)
    if not any(perm in user_permissions for perm in required_permissions):
        logger.warning(f"User {current_user.email} denied access to Uc Comparison")
        raise HTTPException(
            status_code=403,
            detail="Insufficient permissions for Uc Comparison"
        )
    
    return current_user


async def get_comparison_settings(
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """Get Uc Comparison configuration settings"""
    # TODO: Load from database or config
    return {
        "enabled": True,
        "max_batch_size": 1000,
        "cache_ttl": 300
    }
