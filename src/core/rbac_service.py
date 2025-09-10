#!/usr/bin/env python3
"""
RBAC сервіс для управління дозволами
Файл: GeoRetail\src\core\rbac_service.py
"""

from typing import List, Optional, Set
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, text

from models.rbac_models import (
    RBACUser, RBACRole, RBACModule, RBACPermission,
    RBACUserRole, RBACRolePermission, RBACUserPermissionOverride
)

class RBACService:
    """Сервіс управління RBAC"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_user_permissions(self, user_id: int) -> List[str]:
        """Отримати всі permissions користувача"""
        result = self.session.execute(
            text("SELECT permission_code FROM get_user_permissions(:user_id)"),
            {"user_id": user_id}
        ).fetchall()
        return [row[0] for row in result]
    
    def has_permission(self, user_id: int, permission_code: str) -> bool:
        """Перевірити чи має користувач дозвіл"""
        permissions = self.get_user_permissions(user_id)
        return permission_code in permissions
    
    def get_user_roles(self, user_id: int) -> List[RBACRole]:
        """Отримати ролі користувача"""
        user_roles = self.session.query(RBACUserRole).filter(
            and_(
                RBACUserRole.user_id == user_id,
                RBACUserRole.is_active == True
            )
        ).all()
        
        role_ids = [ur.role_id for ur in user_roles]
        return self.session.query(RBACRole).filter(
            RBACRole.id.in_(role_ids)
        ).all()
    
    def get_user_modules(self, user_id: int) -> List[RBACModule]:
        """Отримати доступні модулі для користувача"""
        permissions = self.get_user_permissions(user_id)
        
        # Отримуємо permissions з БД
        permission_objs = self.session.query(RBACPermission).filter(
            RBACPermission.code.in_(permissions)
        ).all()
        
        # Унікальні module_ids
        module_ids = list(set(p.module_id for p in permission_objs if p.module_id))
        
        return self.session.query(RBACModule).filter(
            RBACModule.id.in_(module_ids)
        ).order_by(RBACModule.display_order).all()
    
    def get_max_risk_level(self, user_id: int) -> str:
        """Отримати максимальний рівень ризику дозволів користувача"""
        permissions = self.get_user_permissions(user_id)
        
        if not permissions:
            return "none"
        
        permission_objs = self.session.query(RBACPermission).filter(
            RBACPermission.code.in_(permissions)
        ).all()
        
        risk_levels = ["low", "medium", "high", "critical"]
        max_risk = "low"
        
        for perm in permission_objs:
            if perm.risk_level in risk_levels:
                if risk_levels.index(perm.risk_level) > risk_levels.index(max_risk):
                    max_risk = perm.risk_level
        
        return max_risk