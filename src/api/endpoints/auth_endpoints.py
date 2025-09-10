"""
🔐 AUTH ENDPOINTS FOR RBAC SYSTEM
Team #2 Implementation - Backend Integration
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import jwt
import bcrypt
import logging

# Імпорти з проекту
from core.rbac_database import get_db
from models.rbac_models import (
    RBACUser, 
    RBACUserRole, 
    RBACRole,
    RBACUserSession,
    RBACPermission,
    RBACRolePermission
)

# Налаштування
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v2/auth", tags=["Authentication"])

# JWT Configuration
JWT_SECRET_KEY = "your-very-long-secret-key-min-32-chars-for-mvp"  # TODO: перенести в .env
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 60
JWT_REFRESH_TOKEN_EXPIRE_DAYS = 30

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v2/auth/login")

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Перевірка пароля"""
    return bcrypt.checkpw(
        plain_password.encode('utf-8'),
        hashed_password.encode('utf-8')
    )

def hash_password(password: str) -> str:
    """Хешування пароля"""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Створення JWT access token"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({"exp": expire, "iat": datetime.utcnow()})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict):
    """Створення JWT refresh token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=JWT_REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "iat": datetime.utcnow(), "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt

def decode_token(token: str):
    """Декодування JWT token"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )

# ==========================================
# AUTHENTICATION ENDPOINTS
# ==========================================

@router.post("/login")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db)
):
    """
    Логін користувача
    
    Returns:
        - access_token: JWT токен для API запитів
        - refresh_token: токен для оновлення access_token
        - token_type: "bearer"
        - expires_in: час життя токена в секундах
    """
    # Пошук користувача
    user = db.query(RBACUser).filter(
        (RBACUser.username == form_data.username) | 
        (RBACUser.email == form_data.username)
    ).first()
    
    if not user:
        logger.warning(f"Login attempt failed: user not found - {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    # Перевірка пароля
    if not verify_password(form_data.password, user.password_hash):
        # Збільшуємо лічильник невдалих спроб
        user.failed_login_attempts += 1
        
        # Блокування після 5 спроб
        if user.failed_login_attempts >= 5:
            user.locked_until = datetime.utcnow() + timedelta(minutes=15)
            user.is_active = False
            logger.warning(f"User {user.username} locked due to failed attempts")
        
        db.commit()
        
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    
    # Перевірка чи користувач не заблокований
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is locked"
        )
    
    if user.locked_until and user.locked_until > datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Account locked until {user.locked_until}"
        )
    
    # Скидання лічильника при успішному логіні
    user.failed_login_attempts = 0
    user.last_login = datetime.utcnow()
    user.locked_until = None
    
    # Отримання ролей користувача
    user_roles = db.query(RBACRole).join(RBACUserRole).filter(
        RBACUserRole.user_id == user.id,
        RBACUserRole.is_active == True
    ).all()
    
    role_codes = [role.code for role in user_roles]
    
    # Створення токенів
    access_token_data = {
        "sub": str(user.id),
        "username": user.username,
        "email": user.email,
        "roles": role_codes,
        "is_superuser": user.is_superuser
    }
    
    access_token = create_access_token(access_token_data)
    refresh_token = create_refresh_token({"sub": str(user.id)})
    
    # Створення сесії
    session = RBACUserSession(
        user_id=user.id,
        token_hash=access_token[:50],  # Зберігаємо частину токена для ідентифікації
        expires_at=datetime.utcnow() + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES),
        is_active=True
    )
    db.add(session)
    db.commit()
    
    logger.info(f"User {user.username} logged in successfully")
    
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "roles": role_codes
        }
    }

@router.post("/refresh")
async def refresh_token(
    refresh_token: str,
    db: Session = Depends(get_db)
):
    """Оновлення access token за допомогою refresh token"""
    
    payload = decode_token(refresh_token)
    
    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type"
        )
    
    user_id = payload.get("sub")
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    # Отримання ролей
    user_roles = db.query(RBACRole).join(RBACUserRole).filter(
        RBACUserRole.user_id == user.id,
        RBACUserRole.is_active == True
    ).all()
    
    role_codes = [role.code for role in user_roles]
    
    # Новий access token
    access_token_data = {
        "sub": str(user.id),
        "username": user.username,
        "email": user.email,
        "roles": role_codes,
        "is_superuser": user.is_superuser
    }
    
    new_access_token = create_access_token(access_token_data)
    
    return {
        "access_token": new_access_token,
        "token_type": "bearer",
        "expires_in": JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

@router.get("/me")
async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """Отримання інформації про поточного користувача"""
    
    payload = decode_token(token)
    user_id = payload.get("sub")
    
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Отримання ролей та permissions
    user_roles = db.query(RBACRole).join(RBACUserRole).filter(
        RBACUserRole.user_id == user.id,
        RBACUserRole.is_active == True
    ).all()
    
    # Отримання всіх permissions через ролі
    permissions = set()
    for role in user_roles:
        role_permissions = db.query(RBACPermission).join(RBACRolePermission).filter(
            RBACRolePermission.role_id == role.id
        ).all()
        permissions.update([p.code for p in role_permissions])
    
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_active": user.is_active,
        "is_superuser": user.is_superuser,
        "roles": [{"id": r.id, "code": r.code, "name": r.name} for r in user_roles],
        "permissions": list(permissions)
    }

@router.post("/logout")
async def logout(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """Вихід користувача (деактивація сесії)"""
    
    payload = decode_token(token)
    user_id = payload.get("sub")
    
    # Деактивація всіх сесій користувача
    db.query(RBACUserSession).filter(
        RBACUserSession.user_id == user_id,
        RBACUserSession.is_active == True
    ).update({"is_active": False})
    
    db.commit()
    
    return {"message": "Successfully logged out"}

@router.post("/change-password")
async def change_password(
    current_password: str,
    new_password: str,
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """Зміна пароля користувача"""
    
    payload = decode_token(token)
    user_id = payload.get("sub")
    
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Перевірка поточного пароля
    if not verify_password(current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Current password is incorrect"
        )
    
    # Валідація нового пароля
    if len(new_password) < 8:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Password must be at least 8 characters long"
        )
    
    # Оновлення пароля
    user.password_hash = hash_password(new_password)
    user.password_changed_at = datetime.utcnow()
    user.must_change_password = False
    
    db.commit()
    
    return {"message": "Password changed successfully"}

# ==========================================
# DEPENDENCY FOR OTHER ENDPOINTS
# ==========================================

async def get_current_active_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> RBACUser:
    """
    Dependency для отримання поточного активного користувача
    Використовується в інших endpoints для перевірки автентифікації
    """
    payload = decode_token(token)
    user_id = payload.get("sub")
    
    user = db.query(RBACUser).filter(
        RBACUser.id == user_id,
        RBACUser.is_active == True
    ).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    return user

def require_permission(permission_code: str):
    """
    Dependency для перевірки наявності конкретного дозволу
    
    Usage:
        @router.get("/protected", dependencies=[Depends(require_permission("core.view_map"))])
    """
    async def permission_checker(
        current_user: RBACUser = Depends(get_current_active_user),
        db: Session = Depends(get_db)
    ):
        # Superuser має всі дозволи
        if current_user.is_superuser:
            return True
        
        # Отримання permissions користувача через ролі
        permissions = db.query(RBACPermission.code).join(
            RBACRolePermission
        ).join(
            RBACUserRole,
            RBACUserRole.role_id == RBACRolePermission.role_id
        ).filter(
            RBACUserRole.user_id == current_user.id,
            RBACUserRole.is_active == True
        ).all()
        
        permission_codes = [p[0] for p in permissions]
        
        if permission_code not in permission_codes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission_code}' required"
            )
        
        return True
    
    return permission_checker

# Export для використання в інших модулях
__all__ = [
    "router",
    "get_current_active_user", 
    "require_permission",
    "oauth2_scheme"
]