"""
Test graph builder for Lviv store location
Тестування побудови графу для локації магазину у Львові
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data.osm_extractor import geo_osm_extractor
from src.graph.neo4j_client import geo_neo4j_client

def test_lviv_store_graph_building():
    """Test building graph for a potential store location in Lviv"""
    
    print("🚀 Тестування побудови графу для потенційної локації магазину у Львові")
    print("=" * 80)
    
    # Lviv coordinates - центр міста
    lat, lon = 49.8424, 24.0316
    store_id = "lviv_test_001"
    
    # Store parameters for revenue prediction
    store_params = {
        "store_id": store_id,
        "area_sqm": 1200,
        "format": "supermarket", 
        "assortment_categories": 18,
        "parking_spaces": 50,
        "planned_revenue": 2500000  # 2.5M UAH annually
    }
    
    try:
        # Step 1: Extract OSM data
        print(f"1. 📍 Витягування даних OSM для координат: {lat:.6f}, {lon:.6f}")
        location_data = geo_osm_extractor.extract_location_data(lat, lon)
        
        print(f"   Знайдено: {len(location_data['pois'])} POIs, {location_data['buildings']['count']} будівель")
        
        # Step 2: Create location node
        print("2. 🎯 Створення вузла локації...")
        location_id = geo_neo4j_client.create_location_node(
            lat=lat,
            lon=lon,
            **store_params,
            **location_data["spatial_metrics"]
        )
        
        # Step 3: Create POI nodes
        print(f"3. 🏪 Створення {len(location_data['pois'])} POI вузлів...")
        poi_count = 0
        
        for poi_data in location_data['pois']:
            try:
                geo_neo4j_client.create_poi_node(poi_data)
                poi_count += 1
            except Exception as e:
                print(f"   ⚠️  Помилка створення POI {poi_data.get('osm_id')}: {e}")
                continue
        
        # Step 4: Create spatial relationships
        print("4. 🔗 Створення просторових зв'язків...")
        relationship_count = geo_neo4j_client.create_spatial_relationships(
            location_id, 
            radius_km=0.5
        )
        
        print(f"   Граф створено успішно!")
        
        # Step 5: Analyze created graph
        print("5. Аналіз створеного графу:")
        print(f"   Location ID: {location_id}")
        print(f"   POIs створено: {poi_count}")
        print(f"   Зв'язків створено: {relationship_count}")
        
        # Step 6: Verify data in Neo4j
        print("6. Перевірка даних у Neo4j...")
        
        # Get location summary
        summary = geo_neo4j_client.get_location_summary(location_id)
        
        if summary:
            coords = summary.get('coordinates', {})
            print(f"   Локація у графі: {summary.get('location_id', 'N/A')}")
            print(f"   Записаний виторг: {store_params['planned_revenue']:,} грн")
            print(f"   Підключені POI: {summary.get('total_pois', 0)}")
            print(f"   Приклади категорій: {summary.get('categories', [])}")
        
        # Step 7: Competition analysis
        print("7. Аналіз конкурентів у графі...")
        
        # Query for nearby competitors (shopping POIs)
        competition_query = """
        MATCH (l:Location {location_id: $location_id})-[:NEAR_POI]->(p:POI)
        WHERE p.category = 'shopping'
        WITH l, count(p) as competitor_count, avg(p.distance_km) as avg_distance
        RETURN competitor_count, avg_distance
        """
        
        result = geo_neo4j_client.execute_query(competition_query, {"location_id": location_id})
        
        if result:
            comp_data = result[0]
            competitor_count = comp_data.get('competitor_count', 0)
            avg_distance = comp_data.get('avg_distance')
            
            print(f"   Конкуренти поруч: {competitor_count}")
            
            # Handle None values safely
            if avg_distance is not None:
                print(f"   Середня відстань: {avg_distance:.3f} км")
            else:
                print(f"   Середня відстань: немає даних")
        
        # Step 8: Revenue prediction factors
        print("8. 📊 Фактори для прогнозування виторгу:")
        
        factors_query = """
        MATCH (l:Location {location_id: $location_id})
        OPTIONAL MATCH (l)-[:NEAR_POI]->(food:POI {category: 'food'})
        OPTIONAL MATCH (l)-[:NEAR_POI]->(transport:POI {category: 'transport'})
        OPTIONAL MATCH (l)-[:NEAR_POI]->(services:POI {category: 'services'})
        WITH l, 
             count(DISTINCT food) as food_nearby,
             count(DISTINCT transport) as transport_nearby,
             count(DISTINCT services) as services_nearby
        RETURN 
            l.poi_density as poi_density,
            l.retail_density as retail_density,
            l.transport_accessibility as transport_access,
            food_nearby,
            transport_nearby, 
            services_nearby
        """
        
        factors_result = geo_neo4j_client.execute_query(factors_query, {"location_id": location_id})
        
        if factors_result:
            factors = factors_result[0]
            
            # Handle None values safely for all factors
            poi_density = factors.get('poi_density') or 0
            retail_density = factors.get('retail_density') or 0
            transport_access = factors.get('transport_access') or 0
            food_nearby = factors.get('food_nearby') or 0
            transport_nearby = factors.get('transport_nearby') or 0
            services_nearby = factors.get('services_nearby') or 0
            
            print(f"   POI щільність: {poi_density:.2f} на км²")
            print(f"   Торгівельна щільність: {retail_density:.2f} на км²")
            print(f"   Транспортна доступність: {transport_access:.2f}")
            print(f"   Заклади харчування поруч: {food_nearby}")
            print(f"   Транспортні вузли поруч: {transport_nearby}")
            print(f"   Сервіси поруч: {services_nearby}")
        
        # Step 9: Simple revenue prediction
        print("9. 🎯 Простий прогноз виторгу:")
        
        if factors_result:
            factors = factors_result[0]
            
            # Simple scoring model
            base_score = 1.0
            
            # POI density factor (more POIs = more foot traffic)
            poi_density = factors.get('poi_density') or 0
            poi_factor = min(poi_density / 100.0, 1.5)  # Cap at 1.5x
            
            # Transport accessibility factor
            transport_access = factors.get('transport_access') or 0
            transport_factor = 1.0 + (transport_access / 10.0)  # Up to 1.1x
            
            # Competition factor (some competition is good, too much is bad)
            competition_factor = 1.0
            if competitor_count == 0:
                competition_factor = 0.8  # No competition might mean poor location
            elif competitor_count <= 3:
                competition_factor = 1.2  # Healthy competition
            else:
                competition_factor = 0.9  # Too much competition
            
            # Calculate adjusted revenue
            total_factor = base_score * poi_factor * transport_factor * competition_factor
            predicted_revenue = store_params['planned_revenue'] * total_factor
            
            print(f"   Базовий план: {store_params['planned_revenue']:,} грн")
            print(f"   Фактор POI: {poi_factor:.2f}")
            print(f"   Фактор транспорту: {transport_factor:.2f}")
            print(f"   Фактор конкуренції: {competition_factor:.2f}")
            print(f"   Загальний множник: {total_factor:.2f}")
            print(f"   📈 Прогнозований виторг: {predicted_revenue:,.0f} грн")
            
            # Risk assessment
            if total_factor >= 1.2:
                risk_level = "🟢 Низький ризик - відмінна локація"
            elif total_factor >= 1.0:
                risk_level = "🟡 Середній ризик - хороша локація"
            elif total_factor >= 0.8:
                risk_level = "🟠 Підвищений ризик - потребує уваги"
            else:
                risk_level = "🔴 Високий ризик - проблемна локація"
            
            print(f"   Оцінка ризику: {risk_level}")
        
        print("\n" + "=" * 80)
        print("✅ Тестування завершено успішно!")
        print(f"📊 Результат: Локація {location_id} проаналізована та збережена в графі")
        
        return True
        
    except Exception as e:
        print(f"❌ Помилка створення графу: {e}")
        import traceback
        traceback.print_exc()
        return False

def cleanup_test_data():
    """Clean up test data from Neo4j"""
    print("\n🧹 Очищення тестових даних...")
    
    try:
        # Delete test location and related data
        cleanup_query = """
        MATCH (l:Location {store_id: 'lviv_test_001'})
        OPTIONAL MATCH (l)-[r]-()
        DELETE r, l
        """
        
        geo_neo4j_client.execute_query(cleanup_query)
        print("✅ Тестові дані очищено")
        
    except Exception as e:
        print(f"⚠️  Помилка очищення: {e}")

def main():
    """Main test function"""
    print("🏪 GeoRetail: Тест побудови графу для Львова")
    print("=" * 60)
    
    try:
        # Test graph building
        success = test_lviv_store_graph_building()
        
        if success:
            print("\n🎉 Усі тести пройшли успішно!")
            
            # Ask if user wants to keep the data
            response = input("\nЗберегти тестові дані в графі? (y/N): ")
            if response.lower() != 'y':
                cleanup_test_data()
        else:
            print("\n❌ Тести завершились з помилками")
            
    except KeyboardInterrupt:
        print("\n⏹️  Тестування перервано користувачем")
    except Exception as e:
        print(f"\n💥 Критична помилка: {e}")
    finally:
        # Always close connections
        try:
            geo_neo4j_client.close()
        except:
            pass

if __name__ == "__main__":
    main()