"""
# Dependency injection (get_current_user, etc.)
Created for API v2 Domain-Driven Architecture
"""
"""
File: src/api/v2/core/dependencies.py
Path: C:\projects\AA AI Assistance\GeoRetail_git\georetail\src\api\v2\core\dependencies.py

Purpose: Базові dependency injection функції для всіх endpoints API v2
- Автентифікація користувачів
- Перевірка permissions
- Отримання database sessions
- Rate limiting
- Pagination helpers
"""

from fastapi import Depends, HTTPException, status, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any, Callable
from functools import wraps
import logging

# Імпорти з існуючої системи
from core.rbac_database import get_db
from models.rbac_models import RBACUser, RBACPermission, RBACUserRole, RBACRolePermission
from api.endpoints.auth_endpoints import decode_token, oauth2_scheme

# Логування
logger = logging.getLogger(__name__)

# Security scheme для Swagger UI
security = HTTPBearer()

# ==========================================
# USER AUTHENTICATION
# ==========================================

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> RBACUser:
    """
    Отримання поточного користувача з JWT токена
    
    Returns:
        RBACUser: Об'єкт користувача з БД
        
    Raises:
        HTTPException: 401 якщо токен невалідний
    """
    token = credentials.credentials
    
    try:
        # Декодуємо токен
        payload = decode_token(token)
        user_id = payload.get("sub")
        
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )
        
        # Отримуємо користувача з БД
        user = db.query(RBACUser).filter(
            RBACUser.id == user_id
        ).first()
        
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found"
            )
        
        if not user.is_active:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is deactivated"
            )
        
        return user
        
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )

async def get_current_active_user(
    current_user: RBACUser = Depends(get_current_user)
) -> RBACUser:
    """
    Перевірка що користувач активний
    Shortcut для використання в endpoints
    """
    if not current_user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user"
        )
    return current_user

async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: Session = Depends(get_db)
) -> Optional[RBACUser]:
    """
    Опціональна автентифікація - для публічних endpoints
    з різним рівнем доступу для авторизованих/неавторизованих
    """
    if not credentials:
        return None
    
    try:
        return await get_current_user(credentials, db)
    except HTTPException:
        return None

# ==========================================
# PERMISSION CHECKING
# ==========================================

def require_permission(permission_code: str):
    """
    Dependency для перевірки одного конкретного permission
    
    Usage:
        @router.get("/", dependencies=[Depends(require_permission("core.view_map"))])
    """
    async def permission_checker(
        current_user: RBACUser = Depends(get_current_active_user),
        db: Session = Depends(get_db)
    ) -> bool:
        # Superuser має всі дозволи
        if current_user.is_superuser:
            return True
        
        # Перевіряємо permissions через ролі
        has_permission = db.query(RBACPermission).join(
            RBACRolePermission
        ).join(
            RBACUserRole,
            RBACUserRole.role_id == RBACRolePermission.role_id
        ).filter(
            RBACUserRole.user_id == current_user.id,
            RBACUserRole.is_active == True,
            RBACPermission.code == permission_code,
            RBACPermission.is_active == True
        ).first()
        
        if not has_permission:
            logger.warning(
                f"Permission denied: User {current_user.username} "
                f"lacks permission '{permission_code}'"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission_code}' required"
            )
        
        return True
    
    return permission_checker

def require_any_permission(permission_codes: List[str]):
    """
    Перевірка що користувач має хоча б один з permissions
    
    Usage:
        @router.get("/", dependencies=[
            Depends(require_any_permission(["core.view_h3_basic", "core.view_h3_detailed"]))
        ])
    """
    async def permission_checker(
        current_user: RBACUser = Depends(get_current_active_user),
        db: Session = Depends(get_db)
    ) -> bool:
        # Superuser має всі дозволи
        if current_user.is_superuser:
            return True
        
        # Перевіряємо чи є хоча б один permission
        has_permission = db.query(RBACPermission).join(
            RBACRolePermission
        ).join(
            RBACUserRole,
            RBACUserRole.role_id == RBACRolePermission.role_id
        ).filter(
            RBACUserRole.user_id == current_user.id,
            RBACUserRole.is_active == True,
            RBACPermission.code.in_(permission_codes),
            RBACPermission.is_active == True
        ).first()
        
        if not has_permission:
            logger.warning(
                f"Permission denied: User {current_user.username} "
                f"lacks any of permissions: {permission_codes}"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"One of permissions required: {', '.join(permission_codes)}"
            )
        
        return True
    
    return permission_checker

def require_all_permissions(permission_codes: List[str]):
    """
    Перевірка що користувач має ВСІ permissions зі списку
    """
    async def permission_checker(
        current_user: RBACUser = Depends(get_current_active_user),
        db: Session = Depends(get_db)
    ) -> bool:
        # Superuser має всі дозволи
        if current_user.is_superuser:
            return True
        
        # Отримуємо всі permissions користувача
        user_permissions = db.query(RBACPermission.code).join(
            RBACRolePermission
        ).join(
            RBACUserRole,
            RBACUserRole.role_id == RBACRolePermission.role_id
        ).filter(
            RBACUserRole.user_id == current_user.id,
            RBACUserRole.is_active == True,
            RBACPermission.is_active == True
        ).all()
        
        user_permission_codes = {p[0] for p in user_permissions}
        required_set = set(permission_codes)
        
        if not required_set.issubset(user_permission_codes):
            missing = required_set - user_permission_codes
            logger.warning(
                f"Permission denied: User {current_user.username} "
                f"lacks permissions: {missing}"
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"All permissions required: {', '.join(missing)}"
            )
        
        return True
    
    return permission_checker

# ==========================================
# USER PERMISSIONS HELPER
# ==========================================

async def get_user_permissions(
    current_user: RBACUser = Depends(get_current_active_user),
    db: Session = Depends(get_db)
) -> List[str]:
    """
    Отримання списку всіх permissions користувача
    Корисно для умовного рендерингу в responses
    """
    if current_user.is_superuser:
        # Superuser має всі permissions
        all_permissions = db.query(RBACPermission.code).filter(
            RBACPermission.is_active == True
        ).all()
        return [p[0] for p in all_permissions]
    
    # Звичайний користувач - permissions через ролі
    permissions = db.query(RBACPermission.code).join(
        RBACRolePermission
    ).join(
        RBACUserRole,
        RBACUserRole.role_id == RBACRolePermission.role_id
    ).filter(
        RBACUserRole.user_id == current_user.id,
        RBACUserRole.is_active == True,
        RBACPermission.is_active == True
    ).distinct().all()
    
    return [p[0] for p in permissions]

def has_permission(user: RBACUser, permission_code: str, db: Session) -> bool:
    """
    Helper функція для перевірки permission без exception
    Використовується для умовної логіки в endpoints
    """
    if user.is_superuser:
        return True
    
    exists = db.query(RBACPermission).join(
        RBACRolePermission
    ).join(
        RBACUserRole,
        RBACUserRole.role_id == RBACRolePermission.role_id
    ).filter(
        RBACUserRole.user_id == user.id,
        RBACUserRole.is_active == True,
        RBACPermission.code == permission_code,
        RBACPermission.is_active == True
    ).first()
    
    return exists is not None

# ==========================================
# PAGINATION
# ==========================================

class PaginationParams:
    """Параметри пагінації для списків"""
    
    def __init__(
        self,
        page: int = Query(1, ge=1, description="Page number"),
        limit: int = Query(20, ge=1, le=100, description="Items per page"),
        sort: Optional[str] = Query(None, description="Sort field (e.g., 'score:desc')")
    ):
        self.page = page
        self.limit = limit
        self.offset = (page - 1) * limit
        self.sort = sort
    
    def get_sort_params(self) -> tuple:
        """Парсинг параметрів сортування"""
        if not self.sort:
            return None, None
        
        parts = self.sort.split(':')
        field = parts[0]
        direction = parts[1] if len(parts) > 1 else 'asc'
        
        return field, direction

# ==========================================
# FILTERS
# ==========================================

class FilterParams:
    """Базовий клас для фільтрів"""
    
    def __init__(
        self,
        search: Optional[str] = Query(None, description="Search query"),
        date_from: Optional[str] = Query(None, description="Date from (ISO format)"),
        date_to: Optional[str] = Query(None, description="Date to (ISO format)")
    ):
        self.search = search
        self.date_from = date_from
        self.date_to = date_to
    
    def to_dict(self) -> Dict[str, Any]:
        """Конвертація в словник для ORM queries"""
        filters = {}
        if self.search:
            filters['search'] = self.search
        if self.date_from:
            filters['date_from'] = self.date_from
        if self.date_to:
            filters['date_to'] = self.date_to
        return filters

# ==========================================
# RATE LIMITING (placeholder for future)
# ==========================================

class RateLimitParams:
    """Rate limiting parameters based on user role"""
    
    def __init__(
        self,
        current_user: Optional[RBACUser] = Depends(get_optional_user)
    ):
        if not current_user:
            self.limit = 10  # Anonymous users
        elif current_user.is_superuser:
            self.limit = 10000  # No real limit for superusers
        else:
            # Based on role (simplified for now)
            self.limit = 100  # Authenticated users
    
    def check_limit(self, current_count: int) -> bool:
        """Check if rate limit exceeded"""
        return current_count < self.limit

# ==========================================
# EXPORTS
# ==========================================

__all__ = [
    "get_current_user",
    "get_current_active_user",
    "get_optional_user",
    "require_permission",
    "require_any_permission",
    "require_all_permissions",
    "get_user_permissions",
    "has_permission",
    "PaginationParams",
    "FilterParams",
    "RateLimitParams",
    "get_db",
]