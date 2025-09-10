#!/usr/bin/env python3
"""
Сервіс автентифікації для RBAC системи (FIXED VERSION)
Файл: GeoRetail\src\core\auth_service.py
"""

import os
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import hashlib
import secrets

from passlib.context import CryptContext
from jose import jwt, JWTError
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_

from models.rbac_models import RBACUser, RBACUserSession
from models.rbac_schemas import LoginRequest, LoginResponse, UserResponse

# =====================================================
# PASSWORD MANAGER
# =====================================================

class PasswordManager:
    """Управління паролями"""
    
    def __init__(self):
        self.pwd_context = CryptContext(
            schemes=["bcrypt"],
            deprecated="auto",
            bcrypt__rounds=12
        )
    
    def hash_password(self, password: str) -> str:
        """Хешування пароля"""
        return self.pwd_context.hash(password)
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """Перевірка пароля"""
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def validate_password_strength(self, password: str) -> tuple[bool, str]:
        """Валідація складності пароля"""
        if len(password) < 8:
            return False, "Password must be at least 8 characters"
        if not any(c.isupper() for c in password):
            return False, "Password must contain at least one uppercase letter"
        if not any(c.islower() for c in password):
            return False, "Password must contain at least one lowercase letter"
        if not any(c.isdigit() for c in password):
            return False, "Password must contain at least one digit"
        return True, "Password is strong"

# Singleton instance
password_manager = PasswordManager()

# =====================================================
# TOKEN MANAGER
# =====================================================

class TokenManager:
    """Управління JWT токенами"""
    
    def __init__(self):
        self.secret_key = os.getenv("JWT_SECRET_KEY", "your-very-long-secret-key-min-32-chars-for-mvp")
        self.algorithm = os.getenv("JWT_ALGORITHM", "HS256")
        self.access_token_expire_minutes = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "60"))
        self.refresh_token_expire_days = int(os.getenv("JWT_REFRESH_TOKEN_EXPIRE_DAYS", "30"))
    
    def create_access_token(self, data: Dict[str, Any]) -> str:
        """Створити access token"""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(minutes=self.access_token_expire_minutes)
        to_encode.update({
            "exp": expire,
            "type": "access"
        })
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    def create_refresh_token(self, data: Dict[str, Any]) -> str:
        """Створити refresh token"""
        to_encode = data.copy()
        expire = datetime.utcnow() + timedelta(days=self.refresh_token_expire_days)
        to_encode.update({
            "exp": expire,
            "type": "refresh"
        })
        return jwt.encode(to_encode, self.secret_key, algorithm=self.algorithm)
    
    def decode_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Декодувати токен"""
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except JWTError:
            return None
    
    def verify_token(self, token: str, token_type: str = "access") -> bool:
        """Перевірити токен"""
        payload = self.decode_token(token)
        if not payload:
            return False
        return payload.get("type") == token_type

# =====================================================
# AUTH SERVICE
# =====================================================

class AuthService:
    """Сервіс автентифікації"""
    
    def __init__(self, session: Session):
        self.session = session
        self.token_manager = TokenManager()
    
    def _prepare_user_data(self, user: RBACUser) -> dict:
        """Підготовка даних користувача для UserResponse"""
        # Перетворюємо SQLAlchemy об'єкт в словник
        user_data = {
            'id': user.id,
            'email': user.email,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'phone': user.phone,
            'department': user.department,
            'position': user.position,
            'is_active': user.is_active,
            'is_superuser': user.is_superuser,
            'email_verified': user.email_verified,
            'phone_verified': user.phone_verified,
            'last_login': user.last_login,
            'created_at': user.created_at,
            'updated_at': user.updated_at,
            'full_name': user.full_name,
            'roles': []
        }
        
        # Обробка metadata
        if hasattr(user, 'meta_data'):
            if isinstance(user.meta_data, dict):
                user_data['metadata'] = user.meta_data
            else:
                user_data['metadata'] = {}
        else:
            user_data['metadata'] = {}
        
        return user_data
    
    def login(self, login_data: LoginRequest, 
              ip_address: Optional[str] = None,
              user_agent: Optional[str] = None) -> Optional[LoginResponse]:
        """Логін користувача"""
        
        # Пошук користувача
        user = self.session.query(RBACUser).filter(
            or_(
                RBACUser.email == login_data.username,
                RBACUser.username == login_data.username
            )
        ).first()
        
        if not user:
            return None
        
        # Перевірка пароля
        if not password_manager.verify_password(login_data.password, user.password_hash):
            # Збільшуємо лічильник невдалих спроб
            user.failed_login_attempts += 1
            if user.failed_login_attempts >= 5:
                user.locked_until = datetime.utcnow() + timedelta(minutes=15)
            self.session.commit()
            return None
        
        # Перевірка блокування
        if user.locked_until and user.locked_until > datetime.utcnow():
            return None
        
        # Перевірка активності
        if not user.is_active:
            return None
        
        # Скидаємо лічильник
        user.failed_login_attempts = 0
        user.locked_until = None
        user.last_login = datetime.utcnow()
        
        # Створюємо токени
        token_data = {
            "sub": str(user.id),
            "username": user.username,
            "email": user.email
        }
        
        access_token = self.token_manager.create_access_token(token_data)
        refresh_token = self.token_manager.create_refresh_token(token_data)
        
        # Створюємо сесію
        session_id = secrets.token_urlsafe(32)
        new_session = RBACUserSession(
            user_id=user.id,
            token_hash=hashlib.sha256(access_token.encode()).hexdigest(),
            ip_address=ip_address,
            user_agent=user_agent,
            expires_at=datetime.utcnow() + timedelta(minutes=self.token_manager.access_token_expire_minutes)
        )
        self.session.add(new_session)
        self.session.commit()
        
        # Підготовка даних для UserResponse
        user_data = self._prepare_user_data(user)
        
        # Формуємо відповідь
        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            token_type="bearer",
            expires_in=self.token_manager.access_token_expire_minutes * 60,
            user=UserResponse(**user_data)
        )
    
    def logout(self, token: str) -> bool:
        """Вихід користувача"""
        try:
            # Знаходимо сесію по хешу токена
            token_hash = hashlib.sha256(token.encode()).hexdigest()
            session = self.session.query(RBACUserSession).filter(
                RBACUserSession.token_hash == token_hash
            ).first()
            
            if session:
                session.is_active = False
                self.session.commit()
                return True
            return False
        except Exception:
            return False
    
    def refresh_token(self, refresh_token: str) -> Optional[Dict[str, str]]:
        """Оновлення access токена"""
        payload = self.token_manager.decode_token(refresh_token)
        
        if not payload or payload.get("type") != "refresh":
            return None
        
        # Створюємо новий access token
        token_data = {
            "sub": payload.get("sub"),
            "username": payload.get("username"),
            "email": payload.get("email")
        }
        
        new_access_token = self.token_manager.create_access_token(token_data)
        
        return {
            "access_token": new_access_token,
            "token_type": "bearer",
            "expires_in": self.token_manager.access_token_expire_minutes * 60
        }
    
    def get_current_user(self, token: str) -> Optional[RBACUser]:
        """Отримати поточного користувача по токену"""
        payload = self.token_manager.decode_token(token)
        
        if not payload:
            return None
        
        user_id = payload.get("sub")
        if not user_id:
            return None
        
        return self.session.query(RBACUser).filter(
            RBACUser.id == int(user_id)
        ).first()
    
    def change_password(self, user_id: int, old_password: str, new_password: str) -> bool:
        """Зміна пароля користувача"""
        user = self.session.query(RBACUser).filter(
            RBACUser.id == user_id
        ).first()
        
        if not user:
            return False
        
        # Перевірка старого пароля
        if not password_manager.verify_password(old_password, user.password_hash):
            return False
        
        # Валідація нового пароля
        is_valid, msg = password_manager.validate_password_strength(new_password)
        if not is_valid:
            return False
        
        # Оновлення пароля
        user.password_hash = password_manager.hash_password(new_password)
        user.password_changed_at = datetime.utcnow()
        user.must_change_password = False
        
        # Деактивація всіх сесій користувача
        self.session.query(RBACUserSession).filter(
            RBACUserSession.user_id == user_id
        ).update({"is_active": False})
        
        self.session.commit()
        return True

# =====================================================
# UTILITY FUNCTIONS
# =====================================================

def cleanup_expired_sessions(session: Session):
    """Очистка застарілих сесій"""
    expired = session.query(RBACUserSession).filter(
        RBACUserSession.expires_at < datetime.utcnow()
    ).update({"is_active": False})
    session.commit()
    return expired

def verify_session(session: Session, token: str) -> Optional[RBACUserSession]:
    """Перевірка активної сесії"""
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    
    user_session = session.query(RBACUserSession).filter(
        and_(
            RBACUserSession.token_hash == token_hash,
            RBACUserSession.is_active == True,
            RBACUserSession.expires_at > datetime.utcnow()
        )
    ).first()
    
    if user_session:
        # Оновлюємо час останньої активності
        user_session.last_activity = datetime.utcnow()
        session.commit()
    
    return user_session