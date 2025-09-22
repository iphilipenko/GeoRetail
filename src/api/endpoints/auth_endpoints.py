"""
🔐 AUTH ENDPOINTS FOR RBAC SYSTEM - FIXED VERSION
Team #2 Implementation - Backend Integration
Виправлено проблеми з токенами та сесіями
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import jwt
import bcrypt
import hashlib
import logging
import uuid
import json

# Імпорти з проекту
from core.rbac_database import get_db
from models.rbac_models import (
    RBACUser, 
    RBACUserRole, 
    RBACRole,
    RBACUserSession,
    RBACPermission,
    RBACRolePermission,
    RBACModule,  # ДОДАНО для JOIN з permissions
    RBACAuditLog
)

# Налаштування
logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v2/auth", tags=["Authentication"])

# JWT Configuration
JWT_SECRET_KEY = "your-very-long-secret-key-min-32-chars-for-mvp-georetail-2025"  # TODO: перенести в .env
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

def hash_token(token: str) -> str:
    """
    ВАЖЛИВО: Хешування токена для збереження в БД
    Використовуємо SHA256 для створення фіксованого розміру хешу
    """
    return hashlib.sha256(token.encode()).hexdigest()

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """Створення JWT access token"""
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)
    
    to_encode.update({
        "exp": expire, 
        "iat": datetime.utcnow(),
        "type": "access",
        "jti": str(uuid.uuid4())  # Унікальний ID токена
    })
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: dict):
    """Створення JWT refresh token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=JWT_REFRESH_TOKEN_EXPIRE_DAYS)
    
    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow(), 
        "type": "refresh",
        "jti": str(uuid.uuid4())  # Унікальний ID токена
    })
    
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return encoded_jwt

def decode_token(token: str) -> Optional[dict]:
    """Декодування JWT токена"""
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {e}")
        return None

# ==========================================
# ENDPOINTS
# ==========================================

@router.post("/login")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    db: Session = Depends(get_db),
    request: Request = None
):
    """
    Логін користувача
    Приймає username (або email) та password
    """
    logger.info(f"Login attempt for: {form_data.username}")
    
    # Пошук користувача по username АБО email
    user = db.query(RBACUser).filter(
        (RBACUser.username == form_data.username) | 
        (RBACUser.email == form_data.username)
    ).first()
    
    if not user:
        logger.warning(f"User not found: {form_data.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Перевірка активності користувача
    if not user.is_active:
        logger.warning(f"Inactive user attempted login: {user.email}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Account is deactivated"
        )
    
    # Перевірка блокування (після 5 невдалих спроб)
    if user.failed_login_attempts >= 5:
        lockout_time = timedelta(minutes=15)
        if user.last_failed_login and \
           datetime.utcnow() - user.last_failed_login < lockout_time:
            logger.warning(f"Account locked: {user.email}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account temporarily locked due to multiple failed login attempts"
            )
        else:
            # Скидаємо лічильник після закінчення lockout
            user.failed_login_attempts = 0
    
    # Перевірка пароля
    if not verify_password(form_data.password, user.password_hash):
        # Збільшуємо лічильник невдалих спроб
        user.failed_login_attempts += 1
        user.last_failed_login = datetime.utcnow()
        db.commit()
        
        logger.warning(f"Invalid password for user: {user.email}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Успішний логін - скидаємо лічильники
    user.failed_login_attempts = 0
    user.last_login = datetime.utcnow()
    
    # Створюємо токени
    access_token = create_access_token(
        data={"sub": str(user.id), "email": user.email}
    )
    refresh_token = create_refresh_token(
        data={"sub": str(user.id), "email": user.email}
    )
    
    # Деактивуємо старі сесії
    db.query(RBACUserSession).filter(
        and_(
            RBACUserSession.user_id == user.id,
            RBACUserSession.is_active == True
        )
    ).update({"is_active": False})
    
    # Зберігаємо нову сесію з ХЕШОВАНИМ токеном
    session = RBACUserSession(
        id=uuid.uuid4(),
        user_id=user.id,
        token_hash=hash_token(access_token),  # ХЕШУЄМО ТОКЕН!
        expires_at=datetime.utcnow() + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES),
        is_active=True,
        ip_address=request.client.host if request else None,
        user_agent=request.headers.get("User-Agent") if request else None,
        metadata=json.dumps({
            "login_method": "password",
            "refresh_token_hash": hash_token(refresh_token)
        })
    )
    db.add(session)
    
    # Аудит лог - НЕ передаємо id, нехай БД генерує сама (SERIAL/BIGSERIAL)
    audit_log = RBACAuditLog(
        user_id=user.id,
        session_id=session.id,  # UUID сесії
        action="login_success",
        resource_type="auth",
        resource_id=str(session.id),
        ip_address=request.client.host if request else None,
        user_agent=request.headers.get("User-Agent") if request else None,
        request_path="/api/v2/auth/login",
        response_status=200,
        metadata=json.dumps({"method": "password"})
    )
    db.add(audit_log)
    
    db.commit()
    
    # Отримуємо ролі користувача
    user_roles = db.query(RBACRole).join(RBACUserRole).filter(
        RBACUserRole.user_id == user.id
    ).all()
    
    logger.info(f"Successful login: {user.email}")
    
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer",
        "expires_in": JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "first_name": user.first_name,
            "last_name": user.last_name,
            "roles": [role.code for role in user_roles],
            "is_superuser": user.is_superuser
        }
    }

@router.get("/me")
async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """Отримати інформацію про поточного користувача"""
    
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Перевірка типу токена
    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Перевірка активної сесії з хешованим токеном
    token_hash = hash_token(token)
    session = db.query(RBACUserSession).filter(
        and_(
            RBACUserSession.token_hash == token_hash,
            RBACUserSession.is_active == True,
            RBACUserSession.expires_at > datetime.utcnow()
        )
    ).first()
    
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session not found or expired"
        )
    
    # Оновлюємо last_activity
    session.last_activity = datetime.utcnow()
    db.commit()
    
    # Отримуємо користувача
    user_id = int(payload.get("sub"))
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    # Отримуємо ролі та permissions
    user_roles = db.query(RBACRole).join(RBACUserRole).filter(
        RBACUserRole.user_id == user.id
    ).all()
    
    # Збираємо всі permissions через ролі з JOIN на modules
    permissions = set()
    for role in user_roles:
        # ВИПРАВЛЕНО: JOIN з RBACModule щоб отримати module_code
        role_permissions = db.query(
            RBACPermission.code.label('perm_code'),
            RBACModule.code.label('module_code')
        ).join(
            RBACModule,
            RBACPermission.module_id == RBACModule.id
        ).join(
            RBACRolePermission,
            RBACRolePermission.permission_id == RBACPermission.id
        ).filter(
            RBACRolePermission.role_id == role.id
        ).all()
        
        for perm in role_permissions:
            # Тепер використовуємо правильні поля
            permissions.add(f"{perm.module_code}.{perm.perm_code}")
    
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "first_name": user.first_name,
        "last_name": user.last_name,
        "is_active": user.is_active,
        "is_superuser": user.is_superuser,
        "roles": [
            {
                "id": role.id,
                "code": role.code,
                "name": role.name
            } for role in user_roles
        ],
        "permissions": list(permissions)
    }

@router.post("/refresh")
async def refresh_token(
    refresh_token: str,
    db: Session = Depends(get_db)
):
    """Оновити access token використовуючи refresh token"""
    
    payload = decode_token(refresh_token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token"
        )
    
    # Перевірка типу токена
    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token type"
        )
    
    # Отримуємо користувача
    user_id = int(payload.get("sub"))
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    # Створюємо новий access token
    new_access_token = create_access_token(
        data={"sub": str(user.id), "email": user.email}
    )
    
    return {
        "access_token": new_access_token,
        "token_type": "bearer",
        "expires_in": JWT_ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }

@router.post("/logout")
async def logout(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
):
    """Вихід користувача - деактивація сесій"""
    
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    # Деактивуємо всі активні сесії користувача
    user_id = int(payload.get("sub"))
    db.query(RBACUserSession).filter(
        and_(
            RBACUserSession.user_id == user_id,
            RBACUserSession.is_active == True
        )
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
    
    # Перевірка токена
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )
    
    # Отримуємо користувача
    user_id = int(payload.get("sub"))
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    # Перевірка поточного пароля
    if not verify_password(current_password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect"
        )
    
    # Валідація нового пароля
    if len(new_password) < 8:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="New password must be at least 8 characters long"
        )
    
    # Оновлюємо пароль
    user.password_hash = hash_password(new_password)
    user.password_changed_at = datetime.utcnow()
    
    # Деактивуємо всі сесії (для безпеки)
    db.query(RBACUserSession).filter(
        RBACUserSession.user_id == user.id
    ).update({"is_active": False})
    
    db.commit()
    
    return {"message": "Password successfully changed. Please login again."}

# ==========================================
# DEPENDENCY FUNCTIONS
# ==========================================

async def get_current_active_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db)
) -> RBACUser:
    """Dependency для отримання поточного користувача"""
    
    payload = decode_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token"
        )
    
    # Перевірка активної сесії
    token_hash = hash_token(token)
    session = db.query(RBACUserSession).filter(
        and_(
            RBACUserSession.token_hash == token_hash,
            RBACUserSession.is_active == True,
            RBACUserSession.expires_at > datetime.utcnow()
        )
    ).first()
    
    if not session:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired or invalid"
        )
    
    user_id = int(payload.get("sub"))
    user = db.query(RBACUser).filter(RBACUser.id == user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found or inactive"
        )
    
    return user

def require_permission(permission_code: str):
    """Decorator для перевірки permissions"""
    
    async def permission_checker(
        current_user: RBACUser = Depends(get_current_active_user),
        db: Session = Depends(get_db)
    ):
        # Superuser має всі права
        if current_user.is_superuser:
            return True
        
        # Перевіряємо permissions через ролі з JOIN на modules
        # ВИПРАВЛЕНО: Перевіряємо що повний код permission існує
        user_permissions = db.query(
            RBACPermission.code.label('perm_code'),
            RBACModule.code.label('module_code')
        ).join(
            RBACModule,
            RBACPermission.module_id == RBACModule.id
        ).join(
            RBACRolePermission
        ).join(
            RBACRole
        ).join(
            RBACUserRole
        ).filter(
            RBACUserRole.user_id == current_user.id
        ).all()
        
        # Формуємо список повних кодів permissions
        permission_codes = [
            f"{p.module_code}.{p.perm_code}" for p in user_permissions
        ]
        
        if permission_code not in permission_codes:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"You don't have permission: {permission_code}"
            )
        
        return True
    
    return permission_checker

# ==========================================
# EXPORTS FOR OTHER MODULES
# ==========================================

# Експортуємо функції для використання в dependencies
__all__ = [
    'router',
    'decode_token',
    'hash_token',
    'create_access_token',
    'create_refresh_token',
    'get_current_active_user',
    'require_permission',
    'oauth2_scheme',
    'verify_password',
    'hash_password'
]