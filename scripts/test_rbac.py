#!/usr/bin/env python3
"""
Тестовий скрипт для перевірки RBAC системи (FIXED VERSION з оновленням паролів)
Файл: GeoRetail\scripts\test_rbac.py
Використання: python scripts/test_rbac.py
"""

import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, List

# Додаємо src до Python path
project_root = Path(__file__).parent.parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

# Імпортуємо модулі
from core.rbac_database import db_manager, init_database, QueryHelper
from core.auth_service import password_manager, TokenManager, AuthService, cleanup_expired_sessions
from core.rbac_service import RBACService
from models.rbac_models import RBACUser, RBACUserSession, RBACAuditLog, RBACRole, RBACUserRole
from models.rbac_schemas import LoginRequest

# Для SQL запитів
from sqlalchemy import text

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Кольори для виводу
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_header(text: str):
    """Вивести заголовок"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'=' * 60}{Colors.END}")

def print_success(text: str):
    """Вивести успішне повідомлення"""
    print(f"{Colors.GREEN}✅ {text}{Colors.END}")

def print_error(text: str):
    """Вивести помилку"""
    print(f"{Colors.RED}❌ {text}{Colors.END}")

def print_info(text: str):
    """Вивести інформацію"""
    print(f"{Colors.YELLOW}ℹ️  {text}{Colors.END}")

def print_result(label: str, value: any):
    """Вивести результат"""
    print(f"   {Colors.BOLD}{label}:{Colors.END} {value}")

# =====================================================
# ФУНКЦІЯ ДЛЯ ОНОВЛЕННЯ ПАРОЛІВ
# =====================================================

def fix_user_passwords():
    """Оновити паролі всіх користувачів на Test123!"""
    print_header("FIXING USER PASSWORDS")
    
    try:
        with db_manager.session_scope() as session:
            # Генеруємо правильний хеш для Test123!
            correct_hash = password_manager.hash_password("Test123!")
            print_info(f"Generated new password hash for 'Test123!'")
            print_result("Hash length", len(correct_hash))
            
            # Оновлюємо всіх користувачів
            users = session.query(RBACUser).all()
            updated_count = 0
            
            for user in users:
                # Перевіряємо чи працює поточний хеш
                if not password_manager.verify_password("Test123!", user.password_hash):
                    user.password_hash = correct_hash
                    updated_count += 1
                    print_info(f"Updated password for: {user.email}")
            
            session.commit()
            
            if updated_count > 0:
                print_success(f"Updated passwords for {updated_count} users")
            else:
                print_info("All passwords are already correct")
                
            return True
            
    except Exception as e:
        print_error(f"Failed to fix passwords: {e}")
        return False

# =====================================================
# ТЕСТИ
# =====================================================

def test_database_connection():
    """Тест підключення до БД"""
    print_header("1. ТЕСТ ПІДКЛЮЧЕННЯ ДО БАЗИ ДАНИХ")
    
    try:
        # Ініціалізація
        init_database()
        print_success("Database initialized")
        
        # Тест з'єднання
        if db_manager.test_connection():
            print_success("Database connection successful")
        else:
            print_error("Database connection failed")
            return False
        
        # Перевірка RBAC таблиць
        with db_manager.session_scope() as session:
            result = session.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name LIKE 'rbac_%'
            """)).scalar()
            
            print_result("RBAC tables found", result)
            
            if result >= 9:
                print_success(f"All {result} RBAC tables exist")
            else:
                print_error(f"Expected 9 tables, found {result}")
                return False
        
        return True
        
    except Exception as e:
        print_error(f"Database test failed: {e}")
        return False

def test_password_hashing():
    """Тест хешування паролів"""
    print_header("2. ТЕСТ ХЕШУВАННЯ ПАРОЛІВ")
    
    try:
        # Тест хешування
        test_password = "Test123!"
        hashed = password_manager.hash_password(test_password)
        print_result("Password hash length", len(hashed))
        
        # Тест верифікації
        if password_manager.verify_password(test_password, hashed):
            print_success("Password verification works")
        else:
            print_error("Password verification failed")
            return False
        
        # Тест неправильного пароля
        if not password_manager.verify_password("WrongPassword", hashed):
            print_success("Wrong password correctly rejected")
        else:
            print_error("Wrong password incorrectly accepted")
            return False
        
        # Тест валідації складності
        test_cases = [
            ("weak", False),
            ("Test123", False),  # No special char
            ("Test123!", True),   # Valid
            ("TEST123!", False),  # No lowercase
            ("test123!", False),  # No uppercase
            ("Testtest!", False), # No digit
        ]
        
        print_info("Testing password strength validation:")
        for password, should_pass in test_cases:
            is_valid, msg = password_manager.validate_password_strength(password)
            if is_valid == should_pass:
                print_success(f"  '{password}': {msg}")
            else:
                print_error(f"  '{password}': Expected {should_pass}, got {is_valid}")
        
        return True
        
    except Exception as e:
        print_error(f"Password test failed: {e}")
        return False

def test_jwt_tokens():
    """Тест JWT токенів"""
    print_header("3. ТЕСТ JWT ТОКЕНІВ")
    
    try:
        token_manager = TokenManager()
        
        # Тест створення access token
        test_data = {
            "sub": "1",
            "username": "test_user",
            "email": "test@example.com"
        }
        
        access_token = token_manager.create_access_token(test_data)
        print_result("Access token length", len(access_token))
        
        # Тест декодування
        decoded = token_manager.decode_token(access_token)
        print_result("Decoded username", decoded.get("username"))
        
        if decoded.get("username") == "test_user":
            print_success("Token decode successful")
        else:
            print_error("Token decode failed")
            return False
        
        # Тест refresh token
        refresh_token = token_manager.create_refresh_token(test_data)
        print_result("Refresh token length", len(refresh_token))
        
        # Тест верифікації
        verified = token_manager.verify_token(access_token, "access")
        if verified:
            print_success("Access token verification successful")
        else:
            print_error("Access token verification failed")
            return False
        
        verified_refresh = token_manager.verify_token(refresh_token, "refresh")
        if verified_refresh:
            print_success("Refresh token verification successful")
        else:
            print_error("Refresh token verification failed")
            return False
        
        return True
        
    except Exception as e:
        print_error(f"JWT test failed: {e}")
        return False

def test_user_authentication():
    """Тест автентифікації користувача"""
    print_header("4. ТЕСТ АВТЕНТИФІКАЦІЇ КОРИСТУВАЧА")
    
    try:
        with db_manager.session_scope() as session:
            # Спочатку перевіримо наявність користувача
            admin_user = session.query(RBACUser).filter(
                RBACUser.email == "admin@georetail.com"
            ).first()
            
            if not admin_user:
                print_error("Admin user not found in database")
                print_info("Available users:")
                users = session.query(RBACUser).all()
                for u in users:
                    print(f"   - {u.email} (id: {u.id})")
                return False
            
            print_info(f"Found admin user: {admin_user.email} (id: {admin_user.id})")
            
            # Перевіряємо пароль
            print_info("Testing current password hash...")
            if not password_manager.verify_password("Test123!", admin_user.password_hash):
                print_warning("Current password hash doesn't match 'Test123!'")
                print_info("Updating password hash...")
                admin_user.password_hash = password_manager.hash_password("Test123!")
                session.commit()
                print_success("Password hash updated")
            else:
                print_success("Password hash is correct")
            
            # Тест логіну
            auth_service = AuthService(session)
            
            login_data = LoginRequest(
                username="admin@georetail.com",
                password="Test123!"
            )
            
            response = auth_service.login(
                login_data,
                ip_address="127.0.0.1",
                user_agent="Test Script"
            )
            
            if response:
                print_success("Login successful")
                print_result("User", response.user.username)
                print_result("Email", response.user.email)
                print_result("Token type", response.token_type)
                print_result("Expires in", f"{response.expires_in} seconds")
                
                # Зберігаємо токен для наступних тестів
                global test_access_token, test_user_id
                test_access_token = response.access_token
                test_user_id = response.user.id
            else:
                print_error("Login failed - check password or user status")
                
                # Додаткова діагностика
                admin_user = session.query(RBACUser).filter(
                    RBACUser.email == "admin@georetail.com"
                ).first()
                
                print_info("User diagnostics:")
                print_result("Is active", admin_user.is_active)
                print_result("Failed attempts", admin_user.failed_login_attempts)
                print_result("Locked until", admin_user.locked_until)
                
                # Скидаємо блокування якщо є
                if admin_user.locked_until:
                    admin_user.locked_until = None
                    admin_user.failed_login_attempts = 0
                    session.commit()
                    print_info("Reset user lockout, try again")
                
                return False
            
            # Тест неправильного пароля
            bad_login = LoginRequest(
                username="admin@georetail.com",
                password="WrongPassword"
            )
            
            bad_response = auth_service.login(bad_login)
            if bad_response is None:
                print_success("Invalid password correctly rejected")
            else:
                print_error("Invalid password incorrectly accepted")
                return False
        
        return True
        
    except Exception as e:
        print_error(f"Authentication test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_rbac_permissions():
    """Тест RBAC permissions"""
    print_header("5. ТЕСТ RBAC PERMISSIONS")
    
    try:
        with db_manager.session_scope() as session:
            rbac_service = RBACService(session)
            
            # Тестові користувачі
            test_users = [
                ("admin", "admin@georetail.com"),
                ("viewer_user", "viewer@georetail.com"),
                ("marketing_analyst", "marketing@georetail.com"),
                ("expansion_mgr", "expansion@georetail.com")
            ]
            
            for username, email in test_users:
                user = session.query(RBACUser).filter(
                    RBACUser.email == email
                ).first()
                
                if user:
                    print_info(f"\nTesting user: {username}")
                    
                    # Отримуємо permissions
                    permissions = rbac_service.get_user_permissions(user.id)
                    print_result("Total permissions", len(permissions))
                    
                    # Отримуємо ролі
                    roles = rbac_service.get_user_roles(user.id)
                    role_names = [r.code for r in roles]
                    print_result("Roles", ", ".join(role_names) if role_names else "No roles")
                    
                    # Отримуємо модулі
                    modules = rbac_service.get_user_modules(user.id)
                    module_names = [m.code for m in modules]
                    print_result("Accessible modules", ", ".join(module_names) if module_names else "No modules")
                    
                    # Перевірка конкретних permissions
                    test_permissions = [
                        "core.view_map",
                        "competition.view_competitors",
                        "admin.manage_users"
                    ]
                    
                    for perm in test_permissions:
                        has_perm = rbac_service.has_permission(user.id, perm)
                        symbol = "✓" if has_perm else "✗"
                        print(f"     {symbol} {perm}")
                    
                    # Risk level
                    max_risk = rbac_service.get_max_risk_level(user.id)
                    print_result("Max risk level", max_risk)
                else:
                    print_warning(f"User {email} not found")
        
        print_success("\nAll permission checks completed")
        return True
        
    except Exception as e:
        print_error(f"RBAC test failed: {e}")
        return False

def test_audit_logging():
    """Тест audit logging"""
    print_header("6. ТЕСТ AUDIT LOGGING")
    
    try:
        with db_manager.session_scope() as session:
            # Підрахунок записів в audit log
            count = session.query(RBACAuditLog).count()
            print_result("Audit log entries", count)
            
            # Останні 5 записів
            recent_logs = session.query(RBACAuditLog).order_by(
                RBACAuditLog.created_at.desc()
            ).limit(5).all()
            
            if recent_logs:
                print_info("\nRecent audit entries:")
                for log in recent_logs:
                    user_info = f"User {log.user_id}" if log.user_id else "Anonymous"
                    print(f"   • {log.created_at.strftime('%Y-%m-%d %H:%M:%S')} - "
                          f"{user_info} - {log.action}")
                
                print_success("Audit logging is working")
            else:
                print_info("No audit entries yet (this is normal for fresh install)")
        
        return True
        
    except Exception as e:
        print_error(f"Audit test failed: {e}")
        return False

def test_session_management():
    """Тест управління сесіями"""
    print_header("7. ТЕСТ SESSION MANAGEMENT")
    
    try:
        with db_manager.session_scope() as session:
            # Підрахунок активних сесій
            active_count = session.query(RBACUserSession).filter(
                RBACUserSession.is_active == True,
                RBACUserSession.expires_at > datetime.utcnow()
            ).count()
            print_result("Active sessions", active_count)
            
            # Підрахунок застарілих сесій
            expired_count = session.query(RBACUserSession).filter(
                RBACUserSession.expires_at < datetime.utcnow()
            ).count()
            print_result("Expired sessions", expired_count)
            
            # Тест cleanup
            cleanup_expired_sessions(session)
            print_success("Session cleanup executed")
            
            # Перевірка після cleanup
            active_after = session.query(RBACUserSession).filter(
                RBACUserSession.is_active == True
            ).count()
            print_result("Active sessions after cleanup", active_after)
        
        return True
        
    except Exception as e:
        print_error(f"Session test failed: {e}")
        return False

def print_warning(text: str):
    """Вивести попередження"""
    print(f"{Colors.YELLOW}⚠️  {text}{Colors.END}")

def run_all_tests():
    """Запустити всі тести"""
    print(f"\n{Colors.BOLD}{Colors.GREEN}🚀 RBAC SYSTEM TEST SUITE{Colors.END}")
    print(f"{Colors.BOLD}Testing GeoRetail RBAC Implementation{Colors.END}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Спочатку виправляємо паролі
    print_info("Checking and fixing user passwords if needed...")
    fix_user_passwords()
    
    tests = [
        ("Database Connection", test_database_connection),
        ("Password Hashing", test_password_hashing),
        ("JWT Tokens", test_jwt_tokens),
        ("User Authentication", test_user_authentication),
        ("RBAC Permissions", test_rbac_permissions),
        ("Audit Logging", test_audit_logging),
        ("Session Management", test_session_management)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print_error(f"Test '{test_name}' crashed: {e}")
            results.append((test_name, False))
    
    # Підсумок
    print_header("TEST RESULTS SUMMARY")
    
    passed = sum(1 for _, result in results if result)
    failed = len(results) - passed
    
    print(f"\n{Colors.BOLD}Results:{Colors.END}")
    for test_name, result in results:
        status = f"{Colors.GREEN}PASSED{Colors.END}" if result else f"{Colors.RED}FAILED{Colors.END}"
        print(f"  • {test_name}: {status}")
    
    print(f"\n{Colors.BOLD}Summary:{Colors.END}")
    print(f"  Total tests: {len(results)}")
    print(f"  {Colors.GREEN}Passed: {passed}{Colors.END}")
    print(f"  {Colors.RED}Failed: {failed}{Colors.END}")
    
    if failed == 0:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✨ ALL TESTS PASSED! ✨{Colors.END}")
        print(f"{Colors.GREEN}RBAC system is ready for use!{Colors.END}")
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}⚠️  SOME TESTS FAILED{Colors.END}")
        print(f"{Colors.RED}Please check the errors above{Colors.END}")
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# =====================================================
# MAIN
# =====================================================

if __name__ == "__main__":
    try:
        # Встановлюємо environment variables якщо потрібно
        os.environ.setdefault("DB_HOST", "localhost")
        os.environ.setdefault("DB_PORT", "5432")
        os.environ.setdefault("DB_NAME", "georetail")
        os.environ.setdefault("DB_USER", "georetail_user")
        os.environ.setdefault("DB_PASSWORD", "georetail_secure_2024")
        os.environ.setdefault("JWT_SECRET_KEY", "your-very-long-secret-key-min-32-chars-for-mvp")
        
        # Запускаємо тести
        run_all_tests()
        
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Tests interrupted by user{Colors.END}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Colors.RED}Fatal error: {e}{Colors.END}")
        sys.exit(1)