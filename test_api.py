"""
Тестовий скрипт для перевірки всіх endpoints GeoRetail API
Версія: 3.0.0 - Оновлено для реальних territories endpoints
"""

import requests
import json
import time
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
from tabulate import tabulate

# Налаштування логування
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GeoRetailAPITester:
    """Клас для тестування GeoRetail API endpoints"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.token = None
        self.headers = {"Content-Type": "application/json"}
        self.test_results = []
        
    def test_endpoint(self, method: str, endpoint: str, data: Optional[Dict] = None, 
                     params: Optional[Dict] = None, requires_auth: bool = False, 
                     use_form_data: bool = False) -> Dict:
        """Тестує один endpoint"""
        url = f"{self.base_url}{endpoint}"
        headers = self.headers.copy()
        
        if requires_auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        
        start_time = time.time()
        
        try:
            if method == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method == "POST":
                if use_form_data:
                    # Для auth endpoints використовуємо form-data
                    response = requests.post(
                        url, 
                        data=data,
                        headers={"Authorization": headers.get("Authorization")} if "Authorization" in headers else {}
                    )
                else:
                    # Для інших - JSON
                    response = requests.post(url, headers=headers, json=data)
            else:
                response = requests.request(method, url, headers=headers, json=data, params=params)
                
            elapsed_time = round((time.time() - start_time) * 1000, 2)  # мс
            
            result = {
                "endpoint": endpoint,
                "method": method,
                "status": response.status_code,
                "time_ms": elapsed_time,
                "success": 200 <= response.status_code < 300,
                "error": None,
                "data_sample": None
            }
            
            if result["success"]:
                try:
                    data = response.json()
                    # Зберігаємо зразок даних
                    if isinstance(data, list):
                        result["data_sample"] = f"List with {len(data)} items"
                        if len(data) > 0:
                            result["data_sample"] += f", first item keys: {list(data[0].keys()) if isinstance(data[0], dict) else 'not dict'}"
                    elif isinstance(data, dict):
                        result["data_sample"] = f"Dict with keys: {list(data.keys())[:5]}"
                    else:
                        result["data_sample"] = str(type(data))
                except:
                    result["data_sample"] = "Non-JSON response"
            else:
                result["error"] = response.text[:200]
                
        except Exception as e:
            result = {
                "endpoint": endpoint,
                "method": method,
                "status": "ERROR",
                "time_ms": 0,
                "success": False,
                "error": str(e)[:200],
                "data_sample": None
            }
            
        self.test_results.append(result)
        return result
    
    def attempt_login(self, username_or_email: str, password: str) -> bool:
        """Спроба авторизації"""
        logger.info(f"🔐 Спроба авторизації як {username_or_email}...")
        
        endpoint = "/api/v2/auth/login"
        
        # OAuth2PasswordRequestForm очікує username і password як FORM-DATA
        credentials = {
            "username": username_or_email,
            "password": password
        }
        
        result = self.test_endpoint("POST", endpoint, credentials, use_form_data=True)
        
        if result["success"]:
            try:
                response = requests.post(
                    f"{self.base_url}{endpoint}", 
                    data=credentials
                )
                response_data = response.json()
                
                # Отримуємо токен
                self.token = response_data.get("access_token")
                
                if self.token:
                    logger.info(f"   ✅ Авторизація успішна!")
                    logger.info(f"   ✅ Отримано токен: {self.token[:30]}...")
                    
                    # Виводимо інформацію про користувача
                    if "user" in response_data:
                        user_info = response_data["user"]
                        logger.info(f"   👤 Користувач: {user_info.get('email')}")
                        logger.info(f"   🎭 Ролі: {user_info.get('roles', [])}")
                    
                    return True
                else:
                    logger.warning(f"   ⚠️ Токен не знайдено в відповіді")
                    return False
                    
            except Exception as e:
                logger.error(f"   ❌ Помилка парсингу відповіді: {e}")
                return False
        else:
            if result["status"] == 401:
                logger.warning(f"   ❌ Неправильний логін або пароль")
            elif result["status"] == 403:
                logger.warning(f"   ⚠️ Акаунт заблоковано або деактивовано")
            elif result["status"] == 500:
                logger.error(f"   ❌ Внутрішня помилка серверу")
            else:
                logger.warning(f"   ⚠️ Помилка авторизації: {result['error']}")
            return False
    
    def test_territories_endpoints(self, requires_auth: bool = True):
        """Тестування РЕАЛЬНИХ territories endpoints"""
        logger.info("\n📍 ТЕСТУВАННЯ TERRITORIES ENDPOINTS")
        logger.info("=" * 60)
        
        endpoints = [
            # Реальні endpoints з router.py
            ("GET", "/api/v2/territories/admin/geometries/all", None, 
             {"level": "oblast", "simplified": True}, "Всі геометрії областей"),
            
            ("GET", "/api/v2/territories/admin/geometries/all", None,
             {"level": "raion", "simplified": True}, "Всі геометрії районів"),
            
            ("GET", "/api/v2/territories/admin/metrics/all", None,
             {"metric_x": "population", "metric_y": "income_index"}, "Метрики адмінодиниць"),
            
            ("GET", "/api/v2/territories/admin/32000000000", None, None, 
             "Деталі по Києву (KOATUU)"),
            
            ("GET", "/api/v2/territories/admin/14000000000", None, None,
             "Деталі по Київській області"),
            
            ("GET", "/api/v2/territories/h3/grid", None,
             {"bounds": "30.2,50.3,30.7,50.6", "resolution": 7}, "H3 сітка (resolution 7)"),
            
            ("GET", "/api/v2/territories/h3/881f1d4b9ffffff", None, None,
             "Деталі H3 гексагону"),
            
            ("GET", "/api/v2/territories/poi/search", None,
             {"bounds": "30.2,50.3,30.7,50.6", "limit": 10}, "Пошук POI"),
            
            ("GET", "/api/v2/territories/competition/nearby", None,
             {"lat": 50.45, "lon": 30.52, "radius_km": 5}, "Пошук конкурентів"),
            
            ("GET", "/api/v2/territories/bivariate/config", None,
             {"metric_x": "population", "metric_y": "income_index", "bins": 3}, 
             "Конфігурація bivariate"),
            
            ("GET", "/api/v2/territories/metrics/available", None, None,
             "Доступні метрики"),
            
            ("GET", "/api/v2/territories/statistics/32000000000", None, 
             {"period": "month"}, "Статистика території"),
            
            ("POST", "/api/v2/territories/search", 
             {"query": "Київ", "limit": 10}, None, "Пошук територій"),
            
            ("GET", "/api/v2/territories/export/geojson", None,
             {"koatuu_code": "32000000000"}, "Експорт в GeoJSON"),
            
            ("GET", "/api/v2/territories/health", None, None, 
             "Territory service health"),
        ]
        
        for method, endpoint, data, params, description in endpoints:
            logger.info(f"\n🔍 Тестуємо: {description}")
            logger.info(f"   Endpoint: {method} {endpoint}")
            if params:
                logger.info(f"   Params: {params}")
            if data:
                logger.info(f"   Data: {data}")
                
            result = self.test_endpoint(method, endpoint, data, params, requires_auth=requires_auth)
            
            if result["success"]:
                logger.info(f"   ✅ Успішно! Код: {result['status']}, Час: {result['time_ms']}ms")
                logger.info(f"   📊 Дані: {result['data_sample']}")
            elif result["status"] == 403:
                logger.warning(f"   🔒 Потрібна авторизація або недостатньо прав (403)")
            elif result["status"] == 404:
                logger.warning(f"   ⚠️ Endpoint не знайдено (404)")
            elif result["status"] == 500:
                logger.error(f"   ❌ Внутрішня помилка сервера (500)")
                logger.error(f"   Деталі: {result['error'][:100]}")
            else:
                logger.error(f"   ❌ Помилка! Код: {result['status']}")
                logger.error(f"   Деталі: {result['error']}")
    
    def test_auth_endpoints(self):
        """Тестування auth endpoints"""
        logger.info("\n🔑 ТЕСТУВАННЯ AUTH ENDPOINTS")
        logger.info("=" * 60)
        
        # Тестуємо інші auth endpoints
        if self.token:
            # Тест /me endpoint
            logger.info("\n🔍 Тестуємо: /api/v2/auth/me")
            result = self.test_endpoint("GET", "/api/v2/auth/me", requires_auth=True)
            if result["success"]:
                logger.info(f"   ✅ Успішно! Отримано інформацію користувача")
                try:
                    response = requests.get(
                        f"{self.base_url}/api/v2/auth/me",
                        headers={"Authorization": f"Bearer {self.token}"}
                    )
                    user_data = response.json()
                    logger.info(f"   👤 ID: {user_data.get('id')}")
                    logger.info(f"   📧 Email: {user_data.get('email')}")
                    logger.info(f"   🎭 Ролі: {[r['code'] for r in user_data.get('roles', [])]}")
                    logger.info(f"   🔐 Permissions: {len(user_data.get('permissions', []))} дозволів")
                except:
                    pass
            else:
                logger.error(f"   ❌ Помилка: {result['error']}")
            
            # Тест /refresh endpoint
            logger.info("\n🔍 Тестуємо: /api/v2/auth/refresh")
            # Спочатку отримаємо refresh token з логіну
            # ... (якщо потрібно)
            
            # Тест /logout endpoint
            logger.info("\n🔍 Тестуємо: /api/v2/auth/logout")
            result = self.test_endpoint("POST", "/api/v2/auth/logout", requires_auth=True)
            if result["success"]:
                logger.info(f"   ✅ Успішно! Користувач вийшов")
            else:
                logger.error(f"   ❌ Помилка: {result['error']}")
    
    def test_with_different_users(self):
        """Тестує API з різними користувачами"""
        logger.info("\n👥 ТЕСТУВАННЯ З РІЗНИМИ КОРИСТУВАЧАМИ")
        logger.info("=" * 60)
        
        test_users = [
            {
                "login": "admin@georetail.com",
                "password": "Test123!",
                "role": "Admin",
                "expected_access": ["all"]
            },
            {
                "login": "viewer@georetail.com",
                "password": "Test123!",
                "role": "Viewer",
                "expected_access": ["view_map", "view_admin_units"]
            },
            {
                "login": "marketing@georetail.com",
                "password": "Test123!",
                "role": "Marketing Analyst",
                "expected_access": ["view_map", "view_competitors"]
            },
            {
                "login": "expansion@georetail.com",
                "password": "Test123!",
                "role": "Expansion Manager",
                "expected_access": ["view_map", "ml_prediction"]
            }
        ]
        
        for user in test_users:
            logger.info(f"\n🧑 Тестуємо користувача: {user['role']}")
            logger.info(f"   Email: {user['login']}")
            
            # Спроба логіну
            auth_success = self.attempt_login(user['login'], user['password'])
            
            if auth_success:
                logger.info(f"   ✅ Авторизовано як {user['role']}")
                
                # Тест доступу до базових endpoints
                test_endpoints = [
                    ("/api/v2/territories/admin/geometries/all?level=oblast", "Геометрії"),
                    ("/api/v2/territories/h3/grid?bounds=30.2,50.3,30.7,50.6&resolution=7", "H3 Grid"),
                    ("/api/v2/territories/bivariate/config", "Bivariate Config")
                ]
                
                for endpoint, name in test_endpoints:
                    url = endpoint.split("?")[0]
                    params_str = endpoint.split("?")[1] if "?" in endpoint else ""
                    params = {}
                    if params_str:
                        for param in params_str.split("&"):
                            key, value = param.split("=")
                            params[key] = value
                    
                    result = self.test_endpoint("GET", url, params=params, requires_auth=True)
                    
                    if result["success"]:
                        logger.info(f"      ✓ Доступ до {name}")
                    elif result["status"] == 403:
                        logger.info(f"      ✗ Немає доступу до {name} (недостатньо прав)")
                    else:
                        logger.info(f"      ✗ Помилка доступу до {name} (код: {result['status']})")
                
                # Очищаємо токен для наступного користувача
                self.token = None
            else:
                logger.warning(f"   ⚠️ Не вдалося авторизуватися")
    
    def test_health_endpoints(self):
        """Тестування service endpoints"""
        logger.info("\n💓 ТЕСТУВАННЯ SERVICE ENDPOINTS")
        logger.info("=" * 60)
        
        endpoints = [
            ("GET", "/", None, None, "Root"),
            ("GET", "/api", None, None, "API Info"),
            ("GET", "/health", None, None, "Health Check"),
            ("GET", "/metrics", None, None, "Metrics"),
            ("GET", "/docs", None, None, "Swagger UI"),
            ("GET", "/openapi.json", None, None, "OpenAPI Schema"),
        ]
        
        for method, endpoint, data, params, description in endpoints:
            logger.info(f"\n🔍 Тестуємо: {description}")
            result = self.test_endpoint(method, endpoint, data, params)
            
            if result["success"]:
                logger.info(f"   ✅ Код: {result['status']}, Час: {result['time_ms']}ms")
            else:
                logger.error(f"   ❌ Код: {result['status']}")
    
    def generate_report(self):
        """Генерує звіт по тестуванню"""
        logger.info("\n" + "=" * 60)
        logger.info("📊 ЗВІТ ТЕСТУВАННЯ")
        logger.info("=" * 60)
        
        # Підрахунок статистики
        total = len(self.test_results)
        successful = len([r for r in self.test_results if r["success"]])
        failed = total - successful
        avg_time = sum(r["time_ms"] for r in self.test_results if r["time_ms"] > 0) / max(total, 1)
        
        logger.info(f"\n📈 Статистика:")
        logger.info(f"   Всього тестів: {total}")
        logger.info(f"   ✅ Успішних: {successful} ({successful/max(total, 1)*100:.1f}%)")
        logger.info(f"   ❌ Помилок: {failed} ({failed/max(total, 1)*100:.1f}%)")
        logger.info(f"   ⏱️ Середній час відповіді: {avg_time:.2f}ms")
        
        # Групування по статусу
        status_codes = {}
        for r in self.test_results:
            status = r["status"]
            if status not in status_codes:
                status_codes[status] = 0
            status_codes[status] += 1
        
        logger.info(f"\n📊 Розподіл по кодах відповіді:")
        for status, count in sorted(status_codes.items()):
            logger.info(f"   {status}: {count} запитів")
        
        # Таблиця результатів (скорочена)
        logger.info("\n📋 Детальні результати (топ проблемних):")
        
        # Показуємо тільки помилкові
        error_results = [r for r in self.test_results if not r["success"]][:10]
        
        if error_results:
            table_data = []
            for r in error_results:
                table_data.append([
                    "❌",
                    r["method"],
                    r["endpoint"][:50],
                    r["status"],
                    f"{r['time_ms']}ms" if r["time_ms"] > 0 else "N/A",
                    (r["error"] or "")[:50]
                ])
            
            print(tabulate(
                table_data,
                headers=["", "Method", "Endpoint", "Status", "Time", "Error"],
                tablefmt="grid"
            ))
        else:
            logger.info("   🎉 Всі тести пройшли успішно!")
        
        # Проблемні endpoints
        if failed > 0:
            logger.warning("\n⚠️ Проблемні endpoints:")
            seen = set()
            for r in self.test_results:
                if not r["success"]:
                    key = f"{r['method']} {r['endpoint']}"
                    if key not in seen:
                        logger.warning(f"   - {key}: {r['status']}")
                        seen.add(key)
    
    def run_all_tests(self):
        """Запускає всі тести"""
        logger.info("🚀 ЗАПУСК ТЕСТУВАННЯ GEORETAIL API v3.0")
        logger.info(f"📍 Base URL: {self.base_url}")
        logger.info(f"🕐 Час запуску: {datetime.now()}")
        logger.info("=" * 60)
        
        # 1. Тестуємо service endpoints
        self.test_health_endpoints()
        
        # 2. Основний тест з admin користувачем
        logger.info("\n🔑 ОСНОВНИЙ ТЕСТ З ADMIN")
        if self.attempt_login("admin@georetail.com", "Test123!"):
            self.test_auth_endpoints()
            self.test_territories_endpoints(requires_auth=True)
        else:
            logger.error("❌ Не вдалося авторизуватися як admin!")
        
        # 3. Тестуємо з різними користувачами (опційно)
        # self.test_with_different_users()
        
        # 4. Генеруємо звіт
        self.generate_report()
        
        logger.info("\n✅ ТЕСТУВАННЯ ЗАВЕРШЕНО!")


def main():
    """Головна функція"""
    import sys
    
    # Парсимо аргументи командного рядка
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        # Швидкий тест тільки з одним користувачем
        logger.info("⚡ ШВИДКИЙ РЕЖИМ ТЕСТУВАННЯ")
        tester = GeoRetailAPITester(base_url="http://localhost:8000")
        
        # Тільки admin логін і основні endpoints
        if tester.attempt_login("admin@georetail.com", "Test123!"):
            tester.test_health_endpoints()
            tester.test_auth_endpoints()
            tester.test_territories_endpoints(requires_auth=True)
            tester.generate_report()
    elif len(sys.argv) > 1 and sys.argv[1] == "--full":
        # Повний тест з усіма користувачами
        logger.info("🔥 ПОВНИЙ РЕЖИМ ТЕСТУВАННЯ")
        tester = GeoRetailAPITester(base_url="http://localhost:8000")
        tester.test_health_endpoints()
        tester.test_with_different_users()
        tester.generate_report()
    else:
        # Стандартний тест
        tester = GeoRetailAPITester(base_url="http://localhost:8000")
        tester.run_all_tests()


if __name__ == "__main__":
    main()