"""
# Permission checking utilities
Created for API v2 Domain-Driven Architecture
"""
"""
File: src/api/v2/core/permissions.py
Path: C:\projects\AA AI Assistance\GeoRetail_git\georetail\src\api\v2\core\permissions.py

Purpose: Утиліти для роботи з permissions та permission aggregates
- Permission aggregates для спрощення управління
- Permission checking utilities
- Role-based feature flags
- Dynamic permission loading
"""

from typing import List, Dict, Set, Optional, Any
from sqlalchemy.orm import Session
from functools import lru_cache
import logging

from models.rbac_models import (
    RBACUser, 
    RBACPermission, 
    RBACUserRole, 
    RBACRolePermission,
    RBACRole
)

logger = logging.getLogger(__name__)

# ==========================================
# PERMISSION AGGREGATES
# ==========================================

PERMISSION_AGGREGATES = {
    # Territory viewing - базовий доступ
    "territory.view": [
        "core.view_map",
        "core.view_admin_units",
        "core.view_h3_basic"
    ],
    
    # Territory analysis - розширений доступ
    "territory.analyze": [
        "core.view_h3_detailed",
        "core.export_pdf",
        "core.export_data"
    ],
    
    # Competition analysis
    "competition.basic": [
        "competition.view_competitors",
        "competition.export_reports"
    ],
    
    "competition.advanced": [
        "competition.view_competitors",
        "competition.analyze_competitors",
        "competition.cannibalization_analysis",
        "competition.export_reports"
    ],
    
    # Expansion workflow
    "expansion.screening": [
        "expansion.run_screening",
        "expansion.create_field_report",
        "expansion.upload_media"
    ],
    
    "expansion.ml": [
        "expansion.ml_prediction",
        "expansion.run_screening"
    ],
    
    "expansion.full": [
        "expansion.run_screening",
        "expansion.ml_prediction",
        "expansion.create_field_report",
        "expansion.upload_media",
        "expansion.manage_projects"
    ],
    
    # Legal operations
    "legal.view": [
        "legal.view_rental_listings"
    ],
    
    "legal.manage": [
        "legal.view_rental_listings",
        "legal.manage_contracts",
        "legal.contact_landlords",
        "legal.approve_contracts"
    ],
    
    # Partner management
    "partners.all": [
        "partners.view_suppliers",
        "partners.monitor_quality",
        "partners.manage_partner_contracts"
    ],
    
    # Admin operations
    "admin.users": [
        "admin.view_users",
        "admin.manage_users"
    ],
    
    "admin.system": [
        "admin.manage_roles",
        "admin.view_audit_log",
        "admin.system_settings"
    ],
    
    # Export capabilities
    "export.all": [
        "core.export_pdf",
        "core.export_data",
        "competition.export_reports"
    ],
    
    # Read-only access
    "readonly": [
        "core.view_map",
        "core.view_admin_units",
        "core.view_h3_basic",
        "competition.view_competitors",
        "legal.view_rental_listings",
        "partners.view_suppliers"
    ]
}

# ==========================================
# PERMISSION CHECKER CLASS
# ==========================================

class PermissionChecker:
    """
    Клас для ефективної перевірки permissions
    з кешуванням та aggregate support
    """
    
    def __init__(self, user: RBACUser, db: Session):
        self.user = user
        self.db = db
        self._permissions_cache = None
        self._roles_cache = None
    
    @property
    def permissions(self) -> Set[str]:
        """Lazy loading permissions користувача"""
        if self._permissions_cache is None:
            self._load_permissions()
        return self._permissions_cache
    
    @property
    def roles(self) -> Set[str]:
        """Lazy loading ролей користувача"""
        if self._roles_cache is None:
            self._load_roles()
        return self._roles_cache
    
    def _load_permissions(self):
        """Завантаження всіх permissions користувача"""
        if self.user.is_superuser:
            # Superuser має всі permissions
            all_perms = self.db.query(RBACPermission.code).filter(
                RBACPermission.is_active == True
            ).all()
            self._permissions_cache = {p[0] for p in all_perms}
        else:
            # Звичайний користувач - permissions через ролі
            perms = self.db.query(RBACPermission.code).join(
                RBACRolePermission
            ).join(
                RBACUserRole,
                RBACUserRole.role_id == RBACRolePermission.role_id
            ).filter(
                RBACUserRole.user_id == self.user.id,
                RBACUserRole.is_active == True,
                RBACPermission.is_active == True
            ).distinct().all()
            
            self._permissions_cache = {p[0] for p in perms}
            
            # TODO: Додати user permission overrides тут
            # (grant/revoke індивідуальних permissions)
    
    def _load_roles(self):
        """Завантаження ролей користувача"""
        roles = self.db.query(RBACRole.code).join(
            RBACUserRole
        ).filter(
            RBACUserRole.user_id == self.user.id,
            RBACUserRole.is_active == True
        ).all()
        
        self._roles_cache = {r[0] for r in roles}
        
        if self.user.is_superuser:
            self._roles_cache.add("superuser")
    
    def has_permission(self, permission_code: str) -> bool:
        """Перевірка одного permission"""
        return permission_code in self.permissions
    
    def has_any_permission(self, permission_codes: List[str]) -> bool:
        """Перевірка хоча б одного permission зі списку"""
        return bool(set(permission_codes) & self.permissions)
    
    def has_all_permissions(self, permission_codes: List[str]) -> bool:
        """Перевірка всіх permissions зі списку"""
        return set(permission_codes).issubset(self.permissions)
    
    def has_aggregate(self, aggregate_name: str) -> bool:
        """Перевірка permission aggregate"""
        if aggregate_name not in PERMISSION_AGGREGATES:
            logger.warning(f"Unknown permission aggregate: {aggregate_name}")
            return False
        
        required_perms = PERMISSION_AGGREGATES[aggregate_name]
        return self.has_all_permissions(required_perms)
    
    def has_role(self, role_code: str) -> bool:
        """Перевірка чи користувач має роль"""
        return role_code in self.roles
    
    def get_missing_permissions(self, required: List[str]) -> List[str]:
        """Отримати список відсутніх permissions"""
        return list(set(required) - self.permissions)
    
    def get_available_aggregates(self) -> List[str]:
        """Отримати список доступних aggregates"""
        available = []
        for name, perms in PERMISSION_AGGREGATES.items():
            if self.has_all_permissions(perms):
                available.append(name)
        return available

# ==========================================
# FEATURE FLAGS BASED ON PERMISSIONS
# ==========================================

class FeatureFlags:
    """
    Feature flags на основі permissions
    для умовного включення функціональності
    """
    
    def __init__(self, checker: PermissionChecker):
        self.checker = checker
    
    @property
    def can_view_detailed_h3(self) -> bool:
        """Чи може переглядати детальні H3 метрики"""
        return self.checker.has_permission("core.view_h3_detailed")
    
    @property
    def can_export_data(self) -> bool:
        """Чи може експортувати дані"""
        return self.checker.has_any_permission([
            "core.export_pdf",
            "core.export_data"
        ])
    
    @property
    def can_run_ml_predictions(self) -> bool:
        """Чи може запускати ML прогнози"""
        return self.checker.has_permission("expansion.ml_prediction")
    
    @property
    def can_manage_projects(self) -> bool:
        """Чи може управляти проектами"""
        return self.checker.has_permission("expansion.manage_projects")
    
    @property
    def can_analyze_competition(self) -> bool:
        """Чи може аналізувати конкурентів"""
        return self.checker.has_aggregate("competition.basic")
    
    @property
    def can_manage_contracts(self) -> bool:
        """Чи може управляти контрактами"""
        return self.checker.has_permission("legal.manage_contracts")
    
    @property
    def is_admin(self) -> bool:
        """Чи є адміністратором"""
        return self.checker.has_role("admin") or self.checker.user.is_superuser
    
    @property
    def is_readonly_user(self) -> bool:
        """Чи є readonly користувачем"""
        return (
            self.checker.has_aggregate("readonly") and 
            not self.checker.has_aggregate("territory.analyze")
        )
    
    def to_dict(self) -> Dict[str, bool]:
        """Експорт всіх feature flags як dict"""
        return {
            "can_view_detailed_h3": self.can_view_detailed_h3,
            "can_export_data": self.can_export_data,
            "can_run_ml_predictions": self.can_run_ml_predictions,
            "can_manage_projects": self.can_manage_projects,
            "can_analyze_competition": self.can_analyze_competition,
            "can_manage_contracts": self.can_manage_contracts,
            "is_admin": self.is_admin,
            "is_readonly_user": self.is_readonly_user
        }

# ==========================================
# UTILITY FUNCTIONS
# ==========================================

def get_user_permission_checker(user: RBACUser, db: Session) -> PermissionChecker:
    """Factory функція для створення PermissionChecker"""
    return PermissionChecker(user, db)

def get_user_feature_flags(user: RBACUser, db: Session) -> FeatureFlags:
    """Factory функція для створення FeatureFlags"""
    checker = PermissionChecker(user, db)
    return FeatureFlags(checker)

def check_permission_for_response(
    user: RBACUser,
    permission_code: str,
    db: Session,
    include_details: bool = False
) -> Dict[str, Any]:
    """
    Перевірка permission з детальною інформацією для response
    
    Returns:
        Dict з результатом перевірки та опціонально деталями
    """
    checker = PermissionChecker(user, db)
    has_permission = checker.has_permission(permission_code)
    
    result = {"has_permission": has_permission}
    
    if include_details:
        result["details"] = {
            "requested_permission": permission_code,
            "user_permissions": list(checker.permissions),
            "user_roles": list(checker.roles),
            "is_superuser": user.is_superuser
        }
        
        if not has_permission:
            # Знайдемо які aggregates могли б дати цей permission
            possible_aggregates = []
            for agg_name, agg_perms in PERMISSION_AGGREGATES.items():
                if permission_code in agg_perms:
                    possible_aggregates.append(agg_name)
            
            result["details"]["possible_aggregates"] = possible_aggregates
    
    return result

@lru_cache(maxsize=128)
def get_permission_hierarchy() -> Dict[str, List[str]]:
    """
    Отримати ієрархію permissions по модулях
    (кешується для продуктивності)
    """
    return {
        "core": [
            "core.view_map",
            "core.view_admin_units",
            "core.view_h3_basic",
            "core.view_h3_detailed",
            "core.export_pdf",
            "core.export_data"
        ],
        "competition": [
            "competition.view_competitors",
            "competition.analyze_competitors",
            "competition.cannibalization_analysis",
            "competition.export_reports"
        ],
        "expansion": [
            "expansion.run_screening",
            "expansion.ml_prediction",
            "expansion.create_field_report",
            "expansion.upload_media",
            "expansion.manage_projects"
        ],
        "legal": [
            "legal.view_rental_listings",
            "legal.manage_contracts",
            "legal.contact_landlords",
            "legal.approve_contracts"
        ],
        "partners": [
            "partners.view_suppliers",
            "partners.monitor_quality",
            "partners.manage_partner_contracts"
        ],
        "admin": [
            "admin.view_users",
            "admin.manage_users",
            "admin.manage_roles",
            "admin.view_audit_log",
            "admin.system_settings"
        ]
    }

def get_module_permissions(module_name: str) -> List[str]:
    """Отримати всі permissions конкретного модуля"""
    hierarchy = get_permission_hierarchy()
    return hierarchy.get(module_name, [])

# ==========================================
# EXPORTS
# ==========================================

__all__ = [
    # Main classes
    "PermissionChecker",
    "FeatureFlags",
    
    # Factory functions
    "get_user_permission_checker",
    "get_user_feature_flags",
    
    # Utility functions
    "check_permission_for_response",
    "get_permission_hierarchy",
    "get_module_permissions",
    
    # Constants
    "PERMISSION_AGGREGATES",
]