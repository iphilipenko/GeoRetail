#!/usr/bin/env python3
"""
SQLAlchemy моделі для RBAC системи GeoRetail (ВИПРАВЛЕНА ВЕРСІЯ 2.0)
Файл: GeoRetail\src\models\rbac_models.py
Шлях від кореня: GeoRetail\src\models\rbac_models.py
Опис: ORM моделі для всіх RBAC таблиць з виправленими relationships
"""

from datetime import datetime
from typing import Optional, List
from uuid import UUID
import uuid

from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, 
    ForeignKey, Text, JSON, TIMESTAMP, Enum,
    UniqueConstraint, CheckConstraint, Index
)
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID, INET, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql import func

# Базовий клас для всіх моделей
Base = declarative_base()

# =====================================================
# RBAC MODULES - Модулі системи
# =====================================================
class RBACModule(Base):
    """Модулі системи (core, competition, expansion, etc.)"""
    __tablename__ = 'rbac_modules'
    __table_args__ = {'schema': 'public'}
    
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    is_active = Column(Boolean, default=True)
    icon = Column(String(20))  # Emoji або CSS class
    display_order = Column(Integer, default=0)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), 
                       onupdate=func.current_timestamp())
    
    # Relationships
    permissions = relationship("RBACPermission", back_populates="module", 
                              cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<RBACModule(code='{self.code}', name='{self.name}')>"

# =====================================================
# RBAC PERMISSIONS - Дозволи
# =====================================================
class RBACPermission(Base):
    """Атомарні дозволи в системі"""
    __tablename__ = 'rbac_permissions'
    __table_args__ = (
        CheckConstraint("risk_level IN ('low', 'medium', 'high', 'critical')", 
                       name='valid_risk_level'),
        {'schema': 'public'}
    )
    
    id = Column(Integer, primary_key=True)
    module_id = Column(Integer, ForeignKey('public.rbac_modules.id', ondelete='CASCADE'))
    code = Column(String(100), unique=True, nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(Text)
    resource_type = Column(String(50))  # api, ui, data
    risk_level = Column(String(20), default='low')
    is_active = Column(Boolean, default=True)
    # FIX: Змінено metadata на meta_data для уникнення конфлікту з SQLAlchemy
    meta_data = Column('metadata', JSONB, default={})
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), 
                       onupdate=func.current_timestamp())
    
    # Relationships
    module = relationship("RBACModule", back_populates="permissions")
    role_permissions = relationship("RBACRolePermission", back_populates="permission",
                                   cascade="all, delete-orphan")
    user_overrides = relationship("RBACUserPermissionOverride", back_populates="permission",
                                 cascade="all, delete-orphan",
                                 foreign_keys="RBACUserPermissionOverride.permission_id")
    
    def __repr__(self):
        return f"<RBACPermission(code='{self.code}', risk='{self.risk_level}')>"

# =====================================================
# RBAC ROLES - Ролі
# =====================================================
class RBACRole(Base):
    """Ролі користувачів"""
    __tablename__ = 'rbac_roles'
    __table_args__ = {'schema': 'public'}
    
    id = Column(Integer, primary_key=True)
    code = Column(String(50), unique=True, nullable=False)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    is_system = Column(Boolean, default=False)  # Системні ролі не можна видалити
    is_active = Column(Boolean, default=True)
    max_sessions = Column(Integer, default=1)
    session_duration_hours = Column(Integer, default=8)
    # FIX: Змінено metadata на meta_data
    meta_data = Column('metadata', JSONB, default={})
    created_by = Column(Integer, ForeignKey('public.rbac_users.id'))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), 
                       onupdate=func.current_timestamp())
    
    # Relationships
    role_permissions = relationship("RBACRolePermission", back_populates="role",
                                   cascade="all, delete-orphan")
    user_roles = relationship("RBACUserRole", back_populates="role",
                             cascade="all, delete-orphan",
                             foreign_keys="RBACUserRole.role_id")
    
    def __repr__(self):
        return f"<RBACRole(code='{self.code}', name='{self.name}')>"

# =====================================================
# RBAC USERS - Користувачі
# =====================================================
class RBACUser(Base):
    """Користувачі системи"""
    __tablename__ = 'rbac_users'
    __table_args__ = {'schema': 'public'}
    
    id = Column(Integer, primary_key=True)
    email = Column(String(255), unique=True, nullable=False)
    username = Column(String(100), unique=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    phone = Column(String(20))
    department = Column(String(100))
    position = Column(String(100))
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    email_verified = Column(Boolean, default=False)
    phone_verified = Column(Boolean, default=False)
    last_login = Column(TIMESTAMP)
    failed_login_attempts = Column(Integer, default=0)
    locked_until = Column(TIMESTAMP)
    password_changed_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    must_change_password = Column(Boolean, default=False)
    # FIX: Змінено metadata на meta_data
    meta_data = Column('metadata', JSONB, default={})
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    updated_at = Column(TIMESTAMP, server_default=func.current_timestamp(), 
                       onupdate=func.current_timestamp())
    
    # Relationships - FIX: Явно вказуємо foreign_keys для уникнення конфліктів
    user_roles = relationship("RBACUserRole", 
                             back_populates="user",
                             cascade="all, delete-orphan",
                             foreign_keys="RBACUserRole.user_id")
    
    permission_overrides = relationship("RBACUserPermissionOverride", 
                                       back_populates="user",
                                       cascade="all, delete-orphan",
                                       foreign_keys="RBACUserPermissionOverride.user_id")
    
    sessions = relationship("RBACUserSession", 
                           back_populates="user",
                           cascade="all, delete-orphan",
                           foreign_keys="RBACUserSession.user_id")
    
    audit_logs = relationship("RBACAuditLog", 
                            back_populates="user",
                            foreign_keys="RBACAuditLog.user_id")
    
    # Додаткові relationships для foreign keys де user виступає як "assigned_by" або "created_by"
    assigned_roles = relationship("RBACUserRole",
                                 foreign_keys="RBACUserRole.assigned_by",
                                 backref="assigner")
    
    created_overrides = relationship("RBACUserPermissionOverride",
                                    foreign_keys="RBACUserPermissionOverride.created_by",
                                    backref="creator")
    
    granted_permissions = relationship("RBACRolePermission",
                                      foreign_keys="RBACRolePermission.granted_by",
                                      backref="granter")
    
    @property
    def full_name(self):
        """Повне ім'я користувача"""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.username
    
    @property
    def is_locked(self):
        """Чи заблокований користувач"""
        if self.locked_until:
            return datetime.utcnow() < self.locked_until
        return False
    
    def __repr__(self):
        return f"<RBACUser(username='{self.username}', email='{self.email}')>"

# =====================================================
# RBAC ROLE_PERMISSIONS - Зв'язок Ролі-Дозволи
# =====================================================
class RBACRolePermission(Base):
    """Зв'язок між ролями та дозволами"""
    __tablename__ = 'rbac_role_permissions'
    __table_args__ = {'schema': 'public'}
    
    role_id = Column(Integer, ForeignKey('public.rbac_roles.id', ondelete='CASCADE'), 
                    primary_key=True)
    permission_id = Column(Integer, ForeignKey('public.rbac_permissions.id', ondelete='CASCADE'), 
                         primary_key=True)
    granted_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    granted_by = Column(Integer, ForeignKey('public.rbac_users.id'))
    
    # Relationships
    role = relationship("RBACRole", back_populates="role_permissions")
    permission = relationship("RBACPermission", back_populates="role_permissions")
    # granted_by relationship визначено в RBACUser.granted_permissions
    
    def __repr__(self):
        return f"<RBACRolePermission(role_id={self.role_id}, permission_id={self.permission_id})>"

# =====================================================
# RBAC USER_ROLES - Зв'язок Користувачі-Ролі (FIXED)
# =====================================================
class RBACUserRole(Base):
    """Ролі призначені користувачам"""
    __tablename__ = 'rbac_user_roles'
    __table_args__ = {'schema': 'public'}
    
    user_id = Column(Integer, ForeignKey('public.rbac_users.id', ondelete='CASCADE'), 
                    primary_key=True)
    role_id = Column(Integer, ForeignKey('public.rbac_roles.id', ondelete='CASCADE'), 
                    primary_key=True)
    assigned_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    assigned_by = Column(Integer, ForeignKey('public.rbac_users.id'))
    expires_at = Column(TIMESTAMP)  # Для тимчасових ролей
    is_active = Column(Boolean, default=True)
    
    # FIX: Вказуємо явно foreign_keys для кожного relationship
    user = relationship("RBACUser", 
                       back_populates="user_roles",
                       foreign_keys=[user_id])  # Явно вказуємо який FK використовувати
    
    role = relationship("RBACRole", 
                       back_populates="user_roles",
                       foreign_keys=[role_id])  # Явно вказуємо який FK використовувати
    
    # assigned_by relationship визначено в RBACUser.assigned_roles
    
    @property
    def is_expired(self):
        """Чи закінчився термін дії ролі"""
        if self.expires_at:
            return datetime.utcnow() > self.expires_at
        return False
    
    def __repr__(self):
        return f"<RBACUserRole(user_id={self.user_id}, role_id={self.role_id})>"

# =====================================================
# RBAC USER_PERMISSION_OVERRIDES - Персональні налаштування (FIXED)
# =====================================================
class RBACUserPermissionOverride(Base):
    """Індивідуальні налаштування дозволів"""
    __tablename__ = 'rbac_user_permission_overrides'
    __table_args__ = (
        CheckConstraint("action IN ('grant', 'revoke')", name='valid_action'),
        {'schema': 'public'}
    )
    
    user_id = Column(Integer, ForeignKey('public.rbac_users.id', ondelete='CASCADE'), 
                    primary_key=True)
    permission_id = Column(Integer, ForeignKey('public.rbac_permissions.id', ondelete='CASCADE'), 
                         primary_key=True)
    action = Column(String(10), nullable=False)  # grant або revoke
    reason = Column(Text)
    expires_at = Column(TIMESTAMP)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    created_by = Column(Integer, ForeignKey('public.rbac_users.id'))
    
    # FIX: Вказуємо явно foreign_keys
    user = relationship("RBACUser", 
                       back_populates="permission_overrides",
                       foreign_keys=[user_id])  # Явно вказуємо який FK використовувати
    
    permission = relationship("RBACPermission", 
                            back_populates="user_overrides",
                            foreign_keys=[permission_id])
    
    # created_by relationship визначено в RBACUser.created_overrides
    
    def __repr__(self):
        return f"<RBACUserPermissionOverride(user_id={self.user_id}, action='{self.action}')>"

# =====================================================
# RBAC USER_SESSIONS - Сесії користувачів
# =====================================================
class RBACUserSession(Base):
    """Активні сесії користувачів"""
    __tablename__ = 'rbac_user_sessions'
    __table_args__ = {'schema': 'public'}
    
    id = Column(PostgresUUID(as_uuid=True), primary_key=True, 
               default=uuid.uuid4)
    user_id = Column(Integer, ForeignKey('public.rbac_users.id', ondelete='CASCADE'))
    token_hash = Column(String(255), unique=True, nullable=False)
    ip_address = Column(INET)
    user_agent = Column(Text)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    expires_at = Column(TIMESTAMP, nullable=False)
    last_activity = Column(TIMESTAMP, server_default=func.current_timestamp())
    is_active = Column(Boolean, default=True)
    # FIX: Змінено metadata на meta_data
    meta_data = Column('metadata', JSONB, default={})
    
    # Relationships
    user = relationship("RBACUser", back_populates="sessions")
    audit_logs = relationship("RBACAuditLog", back_populates="session")
    
    @property
    def is_expired(self):
        """Чи закінчився термін дії сесії"""
        return datetime.utcnow() > self.expires_at
    
    def __repr__(self):
        return f"<RBACUserSession(id='{self.id}', user_id={self.user_id})>"

# =====================================================
# RBAC AUDIT_LOG - Аудит лог
# =====================================================
class RBACAuditLog(Base):
    """Логування всіх дій користувачів"""
    __tablename__ = 'rbac_audit_log'
    __table_args__ = {'schema': 'public'}
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('public.rbac_users.id'))
    session_id = Column(PostgresUUID(as_uuid=True), 
                       ForeignKey('public.rbac_user_sessions.id'))
    action = Column(String(100), nullable=False)
    resource_type = Column(String(50))
    resource_id = Column(String(100))
    module_code = Column(String(50))
    permission_code = Column(String(100))
    ip_address = Column(INET)
    user_agent = Column(Text)
    request_method = Column(String(10))
    request_path = Column(Text)
    request_body = Column(JSONB)
    response_status = Column(Integer)
    response_time_ms = Column(Integer)
    error_message = Column(Text)
    # FIX: Змінено metadata на meta_data
    meta_data = Column('metadata', JSONB, default={})
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    
    # Relationships
    user = relationship("RBACUser", back_populates="audit_logs")
    session = relationship("RBACUserSession", back_populates="audit_logs")
    
    def __repr__(self):
        return f"<RBACAuditLog(id={self.id}, action='{self.action}', user_id={self.user_id})>"