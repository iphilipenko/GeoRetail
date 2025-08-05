#!/usr/bin/env python3
"""
Test script для перевірки роботи компонентів Module 2
"""

import sys
from pathlib import Path

# Додаємо поточну директорію до Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Імпортуємо компоненти напряму з локальних модулів
try:
    from normalization.tag_parser import TagParser
    from normalization.brand_dictionary import BrandDictionary
    from normalization.brand_matcher import BrandMatcher
    print("✅ Імпорти успішні!")
except ImportError as e:
    print(f"❌ Помилка імпорту: {e}")
    print(f"Поточна директорія: {current_dir}")
    print(f"Python path: {sys.path[:3]}")
    sys.exit(1)

def test_tag_parser():
    """Тест Tag Parser"""
    print("\n🧪 Тестування Tag Parser...")
    
    parser = TagParser()
    
    # Тестові дані
    test_cases = [
        {"tags": '{"shop": "supermarket", "name": "АТБ", "brand": "АТБ"}'},
        {"name": "Сільпо", "shop": "supermarket"},
        None
    ]
    
    for tags in test_cases:
        result = parser.parse_tags(tags)
        print(f"  Вхід: {tags}")
        print(f"  Результат: name={result.name}, shop={result.shop_type}")
        print()

def test_brand_dictionary():
    """Тест Brand Dictionary"""
    print("\n🧪 Тестування Brand Dictionary...")
    
    brand_dict = BrandDictionary()
    stats = brand_dict.get_brand_statistics()
    
    print(f"  Всього брендів: {stats['total_brands']}")
    print(f"  Всього синонімів: {stats['total_synonyms']}")
    print(f"  За групами: {stats['by_functional_group']}")
    
    # Тест пошуку
    test_names = ["АТБ", "Епіцентр", "Pizza Day"]
    print("\n  Тест пошуку:")
    for name in test_names:
        result = brand_dict.find_brand_by_name(name)
        if result:
            brand_id, info = result
            print(f"    '{name}' → {info.canonical_name} (ID: {brand_id})")
        else:
            print(f"    '{name}' → Не знайдено")

def test_brand_matcher():
    """Тест Brand Matcher"""
    print("\n🧪 Тестування Brand Matcher...")
    
    matcher = BrandMatcher()
    
    # Тестові випадки
    test_cases = [
        ("АТБ-маркет", None),
        ("силпо", {"shop": "supermarket"}),
        ("McDonald's", {"amenity": "fast_food", "brand": "McDonald's"}),
        ("Невідомий магазин", None)
    ]
    
    for name, tags in test_cases:
        result = matcher.match_brand(name, tags)
        if result:
            print(f"  '{name}' → {result.canonical_name}")
            print(f"    Довіра: {result.confidence:.2f}, Тип: {result.match_type}")
            print(f"    Вплив: {result.influence_weight}, Група: {result.functional_group}")
        else:
            print(f"  '{name}' → Не знайдено")
        print()
    
    # Статистика
    stats = matcher.get_statistics()
    print(f"\n  Статистика matcher:")
    print(f"    Всього запитів: {stats['total_requests']}")
    print(f"    Успішних: {stats['successful_matches']}")
    print(f"    Типи: {stats['match_types']}")

def test_database_connection():
    """Тест підключення до БД"""
    print("\n🧪 Тестування підключення до БД...")
    
    import psycopg2
    import os
    
    db_string = os.getenv(
        'DB_CONNECTION_STRING',
        "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"
    )
    
    try:
        conn = psycopg2.connect(db_string)
        cur = conn.cursor()
        
        # Перевірка таблиць
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'osm_ukraine' 
            AND table_name IN ('poi_processed', 'h3_analytics_current', 'h3_analytics_changes')
            ORDER BY table_name;
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        print(f"  Знайдено таблиці: {tables}")
        
        # Перевірка даних в osm_raw
        cur.execute("""
            SELECT COUNT(*) 
            FROM osm_ukraine.osm_raw 
            WHERE tags IS NOT NULL
        """)
        count = cur.fetchone()[0]
        print(f"  Записів в osm_raw з тегами: {count:,}")
        
        cur.close()
        conn.close()
        print("  ✅ Підключення успішне!")
        
    except Exception as e:
        print(f"  ❌ Помилка: {e}")

def main():
    """Основна функція"""
    print("=" * 60)
    print("🚀 Тестування компонентів Module 2")
    print("=" * 60)
    
    # Запускаємо тести
    test_tag_parser()
    test_brand_dictionary()
    test_brand_matcher()
    test_database_connection()
    
    print("\n✅ Тестування завершено!")
    print("\nНаступні кроки:")
    print("1. Запустити обробку даних з osm_raw")
    print("2. Перевірити результати в poi_processed")
    print("3. Запустити розрахунок H3 метрик")

if __name__ == "__main__":
    main()