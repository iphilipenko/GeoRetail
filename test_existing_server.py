# test_existing_server.py
"""
🚀 Тест інтегрованого H3 Modal API в існуючому сервері
Використовує main_safe.py замість нового сервера
"""

import requests
import json
from datetime import datetime
import time

# Конфігурація
BASE_URL = "http://localhost:8000"
TEST_H3_INDICES = {
    7: "871fb4662ffffff",   # Район
    8: "881fb46622fffff",   # Частина району  
    9: "891fb466227ffff",   # Квартал
    10: "8a1fb46622d7fff"   # Вулиця
}

def colored_print(text, color="white"):
    """Кольоровий вивід"""
    colors = {
        "red": "\033[91m",
        "green": "\033[92m", 
        "yellow": "\033[93m",
        "blue": "\033[94m",
        "purple": "\033[95m",
        "cyan": "\033[96m",
        "white": "\033[97m",
        "reset": "\033[0m"
    }
    print(f"{colors.get(color, '')}{text}{colors['reset']}")

def test_endpoint(method, url, params=None, expected_status=200, description=""):
    """Тестування endpoint"""
    colored_print(f"\n🧪 {description}", "cyan")
    colored_print(f"📡 {method.upper()} {url}", "blue")
    if params:
        colored_print(f"📋 Параметри: {params}", "yellow")
    
    try:
        start_time = time.time()
        response = requests.get(url, params=params, timeout=10)
        duration = (time.time() - start_time) * 1000
        
        colored_print(f"⏱️ {duration:.0f}ms | 📊 Статус: {response.status_code}", "white")
        
        if response.status_code == expected_status:
            colored_print(f"✅ SUCCESS", "green")
            try:
                data = response.json()
                
                # Спеціальний вивід для різних endpoints
                if 'available_features' in data:
                    features = data['available_features']
                    colored_print(f"🎯 H3 Modal API: {features.get('h3_modal_api', False)}", "green" if features.get('h3_modal_api') else "red")
                    colored_print(f"🎯 Database Integration: {features.get('database_integration', False)}", "green" if features.get('database_integration') else "red")
                
                elif 'location_info' in data:
                    loc = data['location_info']
                    colored_print(f"📍 H3-{loc.get('resolution')}: {loc.get('h3_index', '')[:12]}...", "cyan")
                    colored_print(f"📐 {loc.get('center_lat'):.6f}, {loc.get('center_lon'):.6f}", "cyan")
                    
                elif 'resolution' in data and 'rings' in data:
                    colored_print(f"🧮 H3-{data['resolution']}, {data['rings']} кілець = {data.get('total_area_km2', 'N/A')} км²", "purple")
                    
                elif 'status' in data:
                    status_color = "green" if data['status'] == 'success' else "yellow"
                    colored_print(f"📈 Статус: {data['status']}", status_color)
                    
                return True, data
                
            except json.JSONDecodeError:
                colored_print(f"⚠️ Не JSON відповідь", "yellow")
                return response.status_code == expected_status, response.text
        else:
            colored_print(f"❌ FAILED: {response.status_code}", "red")
            return False, response.text
            
    except requests.exceptions.ConnectionError:
        colored_print(f"❌ Сервер недоступний на {BASE_URL}", "red")
        colored_print(f"💡 Запустіть: cd src && python main_safe.py", "yellow")
        return False, "Connection failed"
        
    except Exception as e:
        colored_print(f"❌ Помилка: {str(e)}", "red")
        return False, str(e)

def main():
    """Основна функція тестування"""
    colored_print(f"🚀 ТЕСТ ІНТЕГРОВАНОГО H3 MODAL API", "cyan")
    colored_print(f"🌐 Існуючий сервер: {BASE_URL}", "blue")
    colored_print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "white")
    print("="*60)
    
    tests = []
    
    # 1. Корінь API - перевірка інтеграції
    success, data = test_endpoint(
        "GET", f"{BASE_URL}/",
        description="Root endpoint - перевірка інтеграції H3 Modal API"
    )
    tests.append(("Root Integration Check", success))
    
    # 2. Health Check
    success, _ = test_endpoint(
        "GET", f"{BASE_URL}/health",
        description="Health Check - стан всіх компонентів"
    )
    tests.append(("Health Check", success))
    
    # 3. Database Connection (якщо доступний)
    success, db_data = test_endpoint(
        "GET", f"{BASE_URL}/api/v1/database/test-connection",
        description="Database Test - PostgreSQL підключення"
    )
    tests.append(("Database Connection", success))
    
    if not success or (isinstance(db_data, dict) and db_data.get('status') == 'error'):
        colored_print("\n⚠️ База даних недоступна - працюємо з mock даними", "yellow")
    
    # 4. Coverage Calculator
    success, _ = test_endpoint(
        "GET", f"{BASE_URL}/api/v1/hexagon-details/coverage-calculator",
        params={"resolution": 10, "rings": 2},
        description="Coverage Calculator - H3 математичні розрахунки"
    )
    tests.append(("Coverage Calculator", success))
    
    # 5. Analysis Preview
    h3_index = TEST_H3_INDICES[10]
    success, _ = test_endpoint(
        "GET", f"{BASE_URL}/api/v1/hexagon-details/analysis-preview/{h3_index}",
        params={"resolution": 10},
        description="Analysis Preview - доступні типи аналізу"
    )
    tests.append(("Analysis Preview", success))
    
    # 6. Hexagon Details
    success, _ = test_endpoint(
        "GET", f"{BASE_URL}/api/v1/hexagon-details/details/{h3_index}",
        params={"resolution": 10, "analysis_type": "pedestrian_competition"},
        description="Hexagon Details - повний аналіз локації"
    )
    tests.append(("Hexagon Details", success))
    
    # 7. POI в гексагоні
    success, _ = test_endpoint(
        "GET", f"{BASE_URL}/api/v1/hexagon-details/poi-in-hexagon/{h3_index}",
        params={"resolution": 10, "include_neighbors": 1},
        description="POI in Hexagon - пошук точок інтересу"
    )
    tests.append(("POI in Hexagon", success))
    
    # 8. Конкурентний аналіз
    success, _ = test_endpoint(
        "GET", f"{BASE_URL}/api/v1/hexagon-details/competitive-analysis/{h3_index}",
        params={"resolution": 10, "radius_rings": 2},
        description="Competitive Analysis - аналіз конкуренції"
    )
    tests.append(("Competitive Analysis", success))
    
    # 9. Перевірка існуючих endpoints
    if data and isinstance(data, dict) and data.get('available_features', {}).get('osm_extractor'):
        success, _ = test_endpoint(
            "GET", f"{BASE_URL}/osm/info",
            description="OSM Integration - перевірка існуючих можливостей"
        )
        tests.append(("OSM Integration", success))
    
    if data and isinstance(data, dict) and data.get('available_features', {}).get('neo4j'):
        success, _ = test_endpoint(
            "GET", f"{BASE_URL}/neo4j/info",
            description="Neo4j Integration - перевірка графової БД"
        )
        tests.append(("Neo4j Integration", success))
    
    # Підсумок
    print("\n" + "="*80)
    colored_print(f"🏁 ПІДСУМОК ІНТЕГРАЦІЇ", "cyan")
    print("="*80)
    
    passed = sum(1 for _, success in tests if success)
    total = len(tests)
    
    for test_name, success in tests:
        status = "✅" if success else "❌"
        color = "green" if success else "red"
        colored_print(f"{status} {test_name}", color)
    
    print(f"\n📊 Результат: {passed}/{total} тестів пройдено")
    
    if passed >= total * 0.8:  # 80%+ success
        colored_print(f"🎉 ІНТЕГРАЦІЯ УСПІШНА!", "green")
        colored_print(f"✅ H3 Modal API готовий до використання", "green")
        print(f"\n🌐 Доступні endpoints:")
        print(f"   📚 Swagger UI: {BASE_URL}/docs")
        print(f"   🔧 Health: {BASE_URL}/health")
        print(f"   🗂️ H3 Modal: {BASE_URL}/api/v1/hexagon-details/")
        print(f"   💾 Database: {BASE_URL}/api/v1/database/test-connection")
    elif passed >= total * 0.6:  # 60%+ success
        colored_print(f"🟡 ЧАСТКОВА ІНТЕГРАЦІЯ", "yellow")
        colored_print(f"⚠️ Деякі компоненти потребують налаштування", "yellow")
    else:
        colored_print(f"🔴 ПРОБЛЕМИ З ІНТЕГРАЦІЄЮ", "red")
        colored_print(f"🔧 Перевірте налаштування сервера", "red")
    
    success_rate = (passed / total) * 100
    colored_print(f"📈 Відсоток успішності: {success_rate:.1f}%", "white")
    
    print(f"\n💡 Наступні кроки:")
    print(f"   1. Відкрийте {BASE_URL}/docs для повної документації")
    print(f"   2. Перевірте {BASE_URL}/health для стану системи")
    print(f"   3. Налаштуйте PostgreSQL для реальних даних")
    print(f"   4. Інтегруйте з frontend додатком")
    
if __name__ == "__main__":
    main()
