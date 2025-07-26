#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test OSM Extractor з реальними координатами магазину у Львові
Магазин: РК ЛЬВІВ ВУЛ.ТРАКТ ГЛИНЯНСЬКИЙ.163
"""
import sys
import os
from pathlib import Path

# Додаємо project root до Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

def test_lviv_store_osm_extraction():
    """Тестування витягу OSM даних для магазину у Львові"""
    print("GeoRetail OSM Extractor - Lviv Store Test")
    print("=" * 60)
    
    # ПРАВИЛЬНІ дані магазину у Львові
    store_data = {
        "shop_number": 1649,
        "name": "РК ЛЬВІВ ВУЛ.ТРАКТ ГЛИНЯНСЬКИЙ.163",
        "lat": 49.84248,
        "lon": 24.10269,
        "city": "м.Львів",
        "region": "Львівська",
        "format": "Біля дому",
        "square_trade": 204,
        "square_total": 266.7,
        "opening_date": "2019-09-20",
        "revenue": 2292695.913,
        "avg_check": 150.69,
        "monthly_checks": 15215,
        "bakery_full_cycle": True,
        "meat_department": True,
        "pizza": True,
        "food_to_go": True
    }
    
    print(f"Тестуємо магазин: {store_data['name']}")
    print(f"Координати: {store_data['lat']}, {store_data['lon']}")
    print(f"Локація: {store_data['city']}, {store_data['region']}")
    print(f"Площа торгова: {store_data['square_trade']} м²")
    print(f"Фактичний виторг: {store_data['revenue']:,.0f} грн")
    print(f"Середній чек: {store_data['avg_check']:.2f} грн")
    
    try:
        # Імпортуємо OSM extractor
        from src.data.osm_extractor import geo_osm_extractor
        print("\nOSM Extractor імпортовано успішно")
        
        # Витягуємо OSM дані для Львова
        print("\nПочинаємо витяг OSM даних для Львова...")
        print(f"Радіус пошуку: {geo_osm_extractor.radius_meters}м")
        
        location_data = geo_osm_extractor.extract_location_data(
            store_data["lat"], 
            store_data["lon"]
        )
        
        print("\nВитяг OSM даних завершено!")
        
        # Аналізуємо результати
        pois = location_data.get("pois", [])
        buildings = location_data.get("buildings", {})
        road_network = location_data.get("road_network", {})
        spatial_metrics = location_data.get("spatial_metrics", {})
        
        print(f"\nЗібрані дані навколо магазину у Львові:")
        print(f"   POIs знайдено: {len(pois)}")
        print(f"   Будівель: {buildings.get('count', 0)}")
        print(f"   Дорожніх вузлів: {road_network.get('nodes_count', 0)}")
        print(f"   Загальна довжина доріг: {road_network.get('total_length_km', 0):.1f} км")
        
        # Категорії POI у Львові
        if pois:
            categories = {}
            for poi in pois:
                cat = poi.get("poi_type", "unknown")
                categories[cat] = categories.get(cat, 0) + 1
            
            print(f"\nТоп-5 категорій POI у районі:")
            for cat, count in sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   - {cat}: {count}")
                
            # Аналіз конкурентів
            competitors = [poi for poi in pois if poi.get("poi_type") in ["shop", "supermarket"]]
            restaurants = [poi for poi in pois if poi.get("poi_type") in ["restaurant", "cafe"]]
            
            print(f"\nКонкурентний аналіз:")
            print(f"   Магазини-конкуренти: {len(competitors)}")
            print(f"   Ресторани/кафе: {len(restaurants)}")
        
        # Просторові метрики
        if spatial_metrics:
            print(f"\nПросторові метрики району:")
            print(f"   Щільність POI: {spatial_metrics.get('poi_density', 0):.1f} на км²")
            print(f"   Щільність retail: {spatial_metrics.get('retail_density', 0):.1f} на км²") 
            print(f"   Транспортна доступність: {spatial_metrics.get('transport_accessibility', 0):.1f}")
            print(f"   Щільність забудови: {spatial_metrics.get('building_density', 0):.1f}")
            print(f"   Різноманітність POI: {spatial_metrics.get('poi_diversity', 0)} категорій")
        
        # Оцінка локації
        retail_score = 0
        if spatial_metrics:
            poi_score = min(spatial_metrics.get("poi_density", 0) / 100, 1.0)
            retail_score_raw = min(spatial_metrics.get("retail_density", 0) / 50, 1.0) 
            transport_score = min(spatial_metrics.get("transport_accessibility", 0) / 20, 1.0)
            
            retail_score = (poi_score * 0.3 + retail_score_raw * 0.4 + transport_score * 0.3)
        
        print(f"\nАналіз локації:")
        print(f"   Оцінка за геоданими: {retail_score:.2f}/1.0")
        print(f"   Фактичний виторг: {store_data['revenue']:,.0f} грн")
        
        # Прогноз vs реальність
        estimated_revenue = 1500000 * (0.5 + retail_score * 0.5)
        accuracy = 1 - abs(estimated_revenue - store_data["revenue"]) / store_data["revenue"]
        
        print(f"   Прогнозний виторг: {estimated_revenue:,.0f} грн")
        print(f"   Точність прогнозу: {accuracy:.1%}")
        
        if retail_score > 0.7:
            print("   Відмінна локація для роздрібної торгівлі")
        elif retail_score > 0.5:
            print("   Хороша локація з потенціалом")
        else:
            print("   Потребує додаткового аналізу")
            
        print("\nТест OSM Extractor для Львова завершено успішно!")
        return location_data
        
    except ImportError as e:
        print(f"Помилка імпорту: {e}")
        print("Переконайтеся що встановлені залежності: pip install -r requirements.txt")
        return None
        
    except Exception as e:
        print(f"Помилка витягу OSM даних: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    result = test_lviv_store_osm_extraction()
    if result:
        print("\nГотово до наступного кроку: створення графу в Neo4j")
        print("Дані про Львів зібрано, можемо переходити до Graph Builder")
    else:
        print("\nВиправте помилки перед продовженням")