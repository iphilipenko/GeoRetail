"""
Test minimal conda environment setup
"""
import sys

def check_python_version():
    """Check Python version"""
    print("🐍 Перевірка Python...")
    version = sys.version
    print(f"  ✅ Python: {version.split()[0]}")
    
    if "3.11" in version:
        print("  ✅ Версія Python правильна")
        return True
    else:
        print("  ⚠️  Очікувалась Python 3.11")
        return False

def test_conda_packages():
    """Test conda-installed packages"""
    print("\n📦 Тестування conda пакетів...")
    
    packages = {
        'pandas': 'Pandas',
        'numpy': 'NumPy', 
        'networkx': 'NetworkX'
    }
    
    success = 0
    for package, name in packages.items():
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'unknown')
            print(f"  ✅ {name}: {version}")
            success += 1
        except ImportError:
            print(f"  ❌ {name}: не встановлено")
    
    return success

def test_pip_packages():
    """Test pip-installed packages"""
    print("\n📦 Тестування pip пакетів...")
    
    packages = {
        'neo4j': 'Neo4j Driver',
        'pydantic': 'Pydantic'
    }
    
    success = 0
    for package, name in packages.items():
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'unknown')
            print(f"  ✅ {name}: {version}")
            success += 1
        except ImportError:
            print(f"  ❌ {name}: не встановлено")
    
    return success

def test_neo4j_connection():
    """Test Neo4j connection capability"""
    print("\n🗃️  Тестування Neo4j підключення...")
    
    try:
        from neo4j import GraphDatabase
        print("  ✅ Neo4j GraphDatabase імпорт успішний")
        
        # Спроба підключення до локального Neo4j
        try:
            driver = GraphDatabase.driver(
                "neo4j://127.0.0.1:7687",
                auth=("neo4j", "Nopassword")
            )
            
            # Тест підключення
            with driver.session(database="neo4j") as session:
                result = session.run("RETURN 'Hello from conda!' as message")
                message = result.single()["message"]
                print(f"  ✅ Neo4j підключення: {message}")
                driver.close()
                return True
                
        except Exception as e:
            print(f"  ⚠️  Neo4j підключення: {e}")
            print("  💡 Перевір чи запущений Neo4j Desktop")
            if 'driver' in locals():
                driver.close()
            return False
            
    except ImportError as e:
        print(f"  ❌ Neo4j імпорт помилка: {e}")
        return False

def main():
    print("🚀 Тест GeoRetail Conda Environment")
    print("=" * 50)
    
    tests = [
        ("Python Version", check_python_version),
        ("Conda Packages", test_conda_packages),
        ("Pip Packages", test_pip_packages),
        ("Neo4j Connection", test_neo4j_connection)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        result = test_func()
        results.append(result)
    
    print("\n" + "=" * 50)
    print("📊 Результати:")
    
    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    passed = sum(results)
    total = len(results)
    
    print(f"\n📈 Результат: {passed}/{total} тестів пройдено")
    
    if passed >= 3:
        print("\n🎉 Базове середовище готове!")
        print("🚀 Можна починати розробку!")
        print("💡 Наступний крок: встановлення геопакетів")
    else:
        print("\n🔧 Потрібно виправити проблеми")
    
    return passed >= 3

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)