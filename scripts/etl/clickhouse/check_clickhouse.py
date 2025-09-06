"""
scripts/etl/clickhouse/check_clickhouse.py
Діагностика проблем з ClickHouse підключенням
Перевіряє різні способи підключення та налаштування
"""

import subprocess
import sys
import time
import logging
import requests
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def check_docker_container():
    """
    Перевіряє чи запущений Docker контейнер ClickHouse
    """
    logger.info("=" * 60)
    logger.info("🐳 ПЕРЕВІРКА DOCKER КОНТЕЙНЕРА")
    logger.info("=" * 60)
    
    try:
        # Перевіряємо чи є docker
        result = subprocess.run(['docker', 'ps'], capture_output=True, text=True)
        
        if 'clickhouse' in result.stdout.lower() or 'georetail' in result.stdout.lower():
            logger.info("✅ ClickHouse контейнер знайдено і запущено")
            
            # Показуємо деталі контейнера
            result = subprocess.run(['docker', 'ps', '--filter', 'name=GeoRetail', '--format', 'table {{.Names}}\t{{.Ports}}\t{{.Status}}'], 
                                  capture_output=True, text=True)
            if result.stdout:
                logger.info(f"\nДеталі контейнера:\n{result.stdout}")
            
            # Альтернативний пошук
            result = subprocess.run(['docker', 'ps', '--filter', 'ancestor=clickhouse/clickhouse-server', '--format', 'table {{.Names}}\t{{.Ports}}\t{{.Status}}'], 
                                  capture_output=True, text=True)
            if result.stdout:
                logger.info(f"\nClickHouse контейнери:\n{result.stdout}")
                
        else:
            logger.warning("⚠️ ClickHouse контейнер НЕ знайдено або не запущено")
            logger.info("\n🔧 Спробуйте запустити контейнер:")
            logger.info("docker run -d --name GeoRetail \\")
            logger.info("  -p 32768:8123 -p 32769:9000 \\")
            logger.info("  clickhouse/clickhouse-server:23.12")
            return False
            
    except FileNotFoundError:
        logger.error("❌ Docker не встановлено або не доступний в PATH")
        return False
    except Exception as e:
        logger.error(f"❌ Помилка при перевірці Docker: {e}")
        return False
    
    return True

def check_http_interface():
    """
    Перевіряє HTTP інтерфейс ClickHouse
    """
    logger.info("\n" + "=" * 60)
    logger.info("🌐 ПЕРЕВІРКА HTTP ІНТЕРФЕЙСУ")
    logger.info("=" * 60)
    
    urls_to_check = [
        ("http://localhost:32768/", None, None),
        ("http://localhost:32768/", "webuser", "password123"),
        ("http://localhost:8123/", None, None),  # Стандартний порт
        ("http://localhost:8123/", "default", "")
    ]
    
    for url, user, password in urls_to_check:
        try:
            auth = (user, password) if user else None
            response = requests.get(f"{url}?query=SELECT version()", auth=auth, timeout=5)
            
            if response.status_code == 200:
                logger.info(f"✅ HTTP працює на {url}")
                if user:
                    logger.info(f"   Користувач: {user}")
                logger.info(f"   Версія: {response.text.strip()}")
                return url, user, password
            else:
                logger.warning(f"❌ {url} - код {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            logger.warning(f"❌ {url} - немає з'єднання")
        except Exception as e:
            logger.warning(f"❌ {url} - помилка: {e}")
    
    return None, None, None

def check_native_connections():
    """
    Перевіряє різні варіанти підключення через Native протокол
    """
    logger.info("\n" + "=" * 60)
    logger.info("🔌 ПЕРЕВІРКА NATIVE ПІДКЛЮЧЕНЬ")
    logger.info("=" * 60)
    
    connections_to_try = [
        {"host": "localhost", "port": 32769, "user": "webuser", "password": "password123"},
        {"host": "localhost", "port": 32769, "user": "default", "password": ""},
        {"host": "localhost", "port": 9000, "user": "default", "password": ""},
        {"host": "localhost", "port": 9000, "user": "webuser", "password": "password123"},
    ]
    
    for conn_params in connections_to_try:
        try:
            logger.info(f"\nСпроба: {conn_params['user']}@{conn_params['host']}:{conn_params['port']}")
            
            client = Client(**conn_params)
            version = client.execute("SELECT version()")[0][0]
            
            logger.info(f"✅ УСПІШНО! Версія: {version}")
            
            # Перевіряємо бази даних
            databases = client.execute("SHOW DATABASES")
            logger.info(f"   Бази даних: {[db[0] for db in databases]}")
            
            # Перевіряємо geo_analytics
            if 'geo_analytics' in [db[0] for db in databases]:
                logger.info("   ✅ База geo_analytics існує")
            else:
                logger.info("   ⚠️ База geo_analytics НЕ існує, створюємо...")
                client.execute("CREATE DATABASE IF NOT EXISTS geo_analytics")
                logger.info("   ✅ База створена")
            
            client.disconnect()
            
            # Повертаємо робочі параметри
            return conn_params
            
        except Exception as e:
            error_msg = str(e)
            if "Authentication failed" in error_msg:
                logger.warning(f"   ❌ Невірний пароль або користувач")
            elif "Connection refused" in error_msg:
                logger.warning(f"   ❌ Порт {conn_params['port']} закритий")
            else:
                logger.warning(f"   ❌ {error_msg[:100]}")
    
    return None

def suggest_fix(working_params):
    """
    Пропонує виправлення для config.py
    """
    logger.info("\n" + "=" * 60)
    logger.info("💡 РЕКОМЕНДАЦІЇ")
    logger.info("=" * 60)
    
    if working_params:
        logger.info("✅ Знайдено робочі параметри підключення!")
        logger.info("\nОновіть config.py наступним чином:")
        logger.info("-" * 40)
        logger.info("CH_CONFIG = {")
        logger.info(f"    'host': '{working_params['host']}',")
        logger.info(f"    'port': {working_params['port']},")
        logger.info(f"    'database': 'geo_analytics',")
        logger.info(f"    'user': '{working_params['user']}',")
        logger.info(f"    'password': '{working_params['password']}'")
        logger.info("}")
        logger.info("-" * 40)
    else:
        logger.error("❌ Не вдалося знайти робочі параметри!")
        logger.info("\nМожливі рішення:")
        logger.info("1. Запустіть ClickHouse контейнер:")
        logger.info("   docker run -d --name GeoRetail \\")
        logger.info("     -p 32768:8123 -p 32769:9000 \\")
        logger.info("     clickhouse/clickhouse-server:23.12")
        logger.info("\n2. Або використайте існуючий контейнер:")
        logger.info("   docker start GeoRetail")
        logger.info("\n3. Створіть користувача в ClickHouse:")
        logger.info("   docker exec -it GeoRetail clickhouse-client")
        logger.info("   CREATE USER webuser IDENTIFIED BY 'password123';")
        logger.info("   GRANT ALL ON *.* TO webuser;")

def main():
    """
    Головна функція діагностики
    """
    logger.info("🔍 ДІАГНОСТИКА CLICKHOUSE ПІДКЛЮЧЕННЯ")
    logger.info("=" * 60)
    
    # 1. Перевіряємо Docker
    docker_ok = check_docker_container()
    
    # 2. Перевіряємо HTTP
    http_url, http_user, http_pass = check_http_interface()
    
    # 3. Перевіряємо Native підключення
    working_params = check_native_connections()
    
    # 4. Даємо рекомендації
    suggest_fix(working_params)
    
    if working_params:
        logger.info("\n✅ Діагностика завершена успішно!")
        return 0
    else:
        logger.error("\n❌ Потрібно налаштувати ClickHouse!")
        return 1

if __name__ == "__main__":
    sys.exit(main())