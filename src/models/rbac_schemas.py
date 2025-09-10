#!/usr/bin/env python3
"""
Pydantic schemas для RBAC системи GeoRetail (FIXED VERSION)
Файл: GeoRetail\src\models\rbac_schemas.py
Опис: Схеми валідації та серіалізації для API з виправленим полем metadata
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import UUID
from enum import Enum

from pydantic import BaseModel, EmailStr, Field, validator, ConfigDict, field_validator

# =====================================================
# ENUMS
# =====================================================

class RiskLevel(str, Enum):
    """Рівні ризику для permissions"""
    LOW = "low"
    MEDIUM = "medium" 
    HIGH = "high"
    CRITICAL = "critical"

class ResourceType(str, Enum):
    """Типи ресурсів"""
    API = "api"
    UI = "ui"
    DATA = "data"

class OverrideAction(str, Enum):
    """Дії для permission overrides"""
    GRANT = "grant"
    REVOKE = "revoke"

# =====================================================
# MODULE SCHEMAS
# =====================================================

class ModuleBase(BaseModel):
    """Базова схема для модулів"""
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    icon: Optional[str] = Field(None, max_length=20)
    display_order: int = Field(default=0)
    is_active: bool = Field(default=True)

class ModuleCreate(ModuleBase):
    """Схема для створення модуля"""
    pass

class ModuleUpdate(BaseModel):
    """Схема для оновлення модуля"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    icon: Optional[str] = Field(None, max_length=20)
    display_order: Optional[int] = None
    is_active: Optional[bool] = None

class ModuleResponse(ModuleBase):
    """Схема відповіді для модуля"""
    id: int
    created_at: datetime
    updated_at: datetime
    permissions_count: Optional[int] = 0
    
    model_config = ConfigDict(from_attributes=True)

# =====================================================
# PERMISSION SCHEMAS
# =====================================================

class PermissionBase(BaseModel):
    """Базова схема для permissions"""
    code: str = Field(..., min_length=1, max_length=100)
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    resource_type: Optional[ResourceType] = None
    risk_level: RiskLevel = Field(default=RiskLevel.LOW)
    is_active: bool = Field(default=True)
    meta_data: Dict[str, Any] = Field(default_factory=dict, alias="metadata")
    
    model_config = ConfigDict(populate_by_name=True)

class PermissionCreate(PermissionBase):
    """Схема для створення permission"""
    module_id: int

class PermissionUpdate(BaseModel):
    """Схема для оновлення permission"""
    name: Optional[str] = Field(None, min_length=1, max_length=200)
    description: Optional[str] = None
    resource_type: Optional[ResourceType] = None
    risk_level: Optional[RiskLevel] = None
    is_active: Optional[bool] = None
    meta_data: Optional[Dict[str, Any]] = Field(None, alias="metadata")
    
    model_config = ConfigDict(populate_by_name=True)

class PermissionResponse(PermissionBase):
    """Схема відповіді для permission"""
    id: int
    module_id: int
    module_code: Optional[str] = None
    module_name: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

# =====================================================
# ROLE SCHEMAS
# =====================================================

class RoleBase(BaseModel):
    """Базова схема для ролей"""
    code: str = Field(..., min_length=1, max_length=50)
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    is_system: bool = Field(default=False)
    is_active: bool = Field(default=True)
    max_sessions: int = Field(default=1, ge=1, le=10)
    session_duration_hours: int = Field(default=8, ge=1, le=24)
    meta_data: Dict[str, Any] = Field(default_factory=dict, alias="metadata")
    
    model_config = ConfigDict(populate_by_name=True)

class RoleCreate(RoleBase):
    """Схема для створення ролі"""
    permission_ids: List[int] = Field(default_factory=list)

class RoleUpdate(BaseModel):
    """Схема для оновлення ролі"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None
    max_sessions: Optional[int] = Field(None, ge=1, le=10)
    session_duration_hours: Optional[int] = Field(None, ge=1, le=24)
    meta_data: Optional[Dict[str, Any]] = Field(None, alias="metadata")
    
    model_config = ConfigDict(populate_by_name=True)

class RoleResponse(RoleBase):
    """Схема відповіді для ролі"""
    id: int
    created_at: datetime
    updated_at: datetime
    permissions: Optional[List[PermissionResponse]] = Field(default_factory=list)
    permissions_count: Optional[int] = 0
    users_count: Optional[int] = 0
    
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

class RoleWithPermissions(RoleResponse):
    """Роль з повним списком permissions"""
    permissions: List[PermissionResponse]

# =====================================================
# USER SCHEMAS (FIXED)
# =====================================================

class UserBase(BaseModel):
    """Базова схема для користувачів"""
    email: EmailStr
    username: str = Field(..., min_length=3, max_length=100)
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    department: Optional[str] = Field(None, max_length=100)
    position: Optional[str] = Field(None, max_length=100)
    is_active: bool = Field(default=True)
    meta_data: Dict[str, Any] = Field(default_factory=dict, alias="metadata")
    
    model_config = ConfigDict(populate_by_name=True)
    
    @field_validator('meta_data', mode='before')
    @classmethod
    def validate_metadata(cls, v):
        """Перетворення metadata в словник"""
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        # Якщо це SQLAlchemy MetaData об'єкт, повертаємо порожній словник
        if hasattr(v, '__class__') and v.__class__.__name__ == 'MetaData':
            return {}
        return v

class UserCreate(UserBase):
    """Схема для створення користувача"""
    password: str = Field(..., min_length=8)
    role_ids: List[int] = Field(default_factory=list)
    is_superuser: bool = Field(default=False)
    
    @validator('password')
    def validate_password(cls, v):
        """Валідація складності пароля"""
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain digit')
        return v

class UserUpdate(BaseModel):
    """Схема для оновлення користувача"""
    email: Optional[EmailStr] = None
    first_name: Optional[str] = Field(None, max_length=100)
    last_name: Optional[str] = Field(None, max_length=100)
    phone: Optional[str] = Field(None, max_length=20)
    department: Optional[str] = Field(None, max_length=100)
    position: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None
    meta_data: Optional[Dict[str, Any]] = Field(None, alias="metadata")
    
    model_config = ConfigDict(populate_by_name=True)

class UserResponse(UserBase):
    """Схема відповіді для користувача (FIXED)"""
    id: int
    is_superuser: bool
    email_verified: bool
    phone_verified: bool
    last_login: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime
    full_name: Optional[str] = None
    roles: Optional[List[RoleResponse]] = Field(default_factory=list)
    
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True
    )
    
    @field_validator('meta_data', mode='before')
    @classmethod
    def convert_metadata(cls, v):
        """Конвертація metadata в словник"""
        if v is None:
            return {}
        if isinstance(v, dict):
            return v
        # Якщо це SQLAlchemy MetaData або JSONB, повертаємо порожній словник
        if hasattr(v, '__class__'):
            class_name = v.__class__.__name__
            if class_name in ['MetaData', 'JSONB']:
                return {}
        # Спробуємо отримати значення через атрибути
        if hasattr(v, 'meta_data'):
            return v.meta_data if isinstance(v.meta_data, dict) else {}
        return {}

class UserWithPermissions(UserResponse):
    """Користувач з повним списком permissions"""
    effective_permissions: List[str] = Field(default_factory=list)
    permission_sources: Dict[str, str] = Field(default_factory=dict)

# =====================================================
# AUTH SCHEMAS
# =====================================================

class LoginRequest(BaseModel):
    """Запит на логін"""
    username: str  # може бути email або username
    password: str

class LoginResponse(BaseModel):
    """Відповідь після логіну"""
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    user: UserResponse

class TokenRefreshRequest(BaseModel):
    """Запит на оновлення токена"""
    refresh_token: str

class TokenResponse(BaseModel):
    """Відповідь з новим токеном"""
    access_token: str
    token_type: str = "bearer"
    expires_in: int

class ChangePasswordRequest(BaseModel):
    """Запит на зміну пароля"""
    old_password: str
    new_password: str = Field(..., min_length=8)
    
    @validator('new_password')
    def validate_password(cls, v, values):
        """Валідація нового пароля"""
        if 'old_password' in values and v == values['old_password']:
            raise ValueError('New password must be different from old password')
        # Повторюємо валідацію складності
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain uppercase letter')
        if not any(c.islower() for c in v):
            raise ValueError('Password must contain lowercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain digit')
        return v

# =====================================================
# SESSION SCHEMAS
# =====================================================

class SessionResponse(BaseModel):
    """Схема відповіді для сесії"""
    id: UUID
    user_id: int
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    created_at: datetime
    expires_at: datetime
    last_activity: datetime
    is_active: bool
    
    model_config = ConfigDict(from_attributes=True)

# =====================================================
# AUDIT LOG SCHEMAS
# =====================================================

class AuditLogFilter(BaseModel):
    """Фільтри для audit log"""
    user_id: Optional[int] = None
    action: Optional[str] = None
    module_code: Optional[str] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)

class AuditLogResponse(BaseModel):
    """Схема відповіді для audit log"""
    id: int
    user_id: Optional[int] = None
    username: Optional[str] = None
    action: str
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    module_code: Optional[str] = None
    permission_code: Optional[str] = None
    ip_address: Optional[str] = None
    request_method: Optional[str] = None
    request_path: Optional[str] = None
    response_status: Optional[int] = None
    response_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    created_at: datetime
    
    model_config = ConfigDict(from_attributes=True)

# =====================================================
# PERMISSION OVERRIDE SCHEMAS
# =====================================================

class PermissionOverrideCreate(BaseModel):
    """Схема для створення override"""
    user_id: int
    permission_id: int
    action: OverrideAction
    reason: Optional[str] = None
    expires_at: Optional[datetime] = None

class PermissionOverrideResponse(BaseModel):
    """Схема відповіді для override"""
    user_id: int
    permission_id: int
    permission_code: str
    action: OverrideAction
    reason: Optional[str] = None
    expires_at: Optional[datetime] = None
    created_at: datetime
    created_by: Optional[int] = None
    
    model_config = ConfigDict(from_attributes=True)

# =====================================================
# UTILITY SCHEMAS
# =====================================================

class HealthCheckResponse(BaseModel):
    """Відповідь health check"""
    status: str = "healthy"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    version: str = "1.0.0"
    rbac_enabled: bool = True

class ErrorResponse(BaseModel):
    """Схема для помилок"""
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class SuccessResponse(BaseModel):
    """Загальна схема успішної відповіді"""
    success: bool = True
    message: str
    data: Optional[Dict[str, Any]] = None

class PaginatedResponse(BaseModel):
    """Схема для пагінації"""
    items: List[Any]
    total: int
    page: int
    pages: int
    per_page: int