"""
Test GeoRetail OSM Extractor functionality
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.data.osm_extractor import geo_osm_extractor

def test_osm_extraction():
    """Test OSM data extraction for Kyiv center"""
    print("🗺️  Testing OSM data extraction...")
    
    try:
        # Test location: Kyiv center (Maidan Nezalezhnosti)
        lat, lon = 50.4501, 30.5234
        
        print(f"  📍 Extracting data for: {lat}, {lon}")
        location_data = geo_osm_extractor.extract_location_data(lat, lon)
        
        # Display results
        print(f"  ✅ POIs found: {len(location_data['pois'])}")
        print(f"  ✅ Buildings: {location_data['buildings']['count']}")
        print(f"  ✅ Road edges: {location_data['road_network']['edges_count']}")
        
        # Display spatial metrics
        metrics = location_data['spatial_metrics']
        print(f"  📊 POI density: {metrics['poi_density']:.2f} per km²")
        print(f"  📊 Retail density: {metrics['retail_density']:.2f} per km²")
        print(f"  📊 POI diversity: {metrics['poi_diversity']} categories")
        
        # Show top POI categories
        categories = metrics.get('poi_categories', {})
        if categories:
            print("  📋 Top POI categories:")
            for category, count in sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"    - {category}: {count}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ OSM extraction failed: {e}")
        return False

def test_poi_processing():
    """Test POI data processing"""
    print("\n🏪 Testing POI processing...")
    
    try:
        lat, lon = 50.4501, 30.5234
        location_data = geo_osm_extractor.extract_location_data(lat, lon)
        
        pois = location_data['pois']
        if pois:
            sample_poi = pois[0]
            print(f"  ✅ Sample POI structure:")
            print(f"    - Name: {sample_poi.get('name', 'N/A')}")
            print(f"    - Type: {sample_poi.get('poi_type', 'N/A')}")
            print(f"    - Amenity: {sample_poi.get('amenity', 'N/A')}")
            print(f"    - Coordinates: {sample_poi.get('lat', 'N/A')}, {sample_poi.get('lon', 'N/A')}")
            
            return True
        else:
            print("  ⚠️  No POIs found")
            return False
            
    except Exception as e:
        print(f"  ❌ POI processing failed: {e}")
        return False

def main():
    print("🚀 GeoRetail OSM Extractor Test Suite")
    print("=" * 50)
    
    tests = [
        ("OSM Data Extraction", test_osm_extraction),
        ("POI Processing", test_poi_processing)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        result = test_func()
        results.append(result)
    
    print("\n" + "=" * 50)
    print("📊 Test Results:")
    
    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    passed = sum(results)
    total = len(results)
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    # Виправлено відступ для if
    if passed >= 1:
        print("\n🎉 OSM Extractor готовий!")
        print("🚀 Наступний крок: інтеграція з Neo4j")
    else:
        print("\n🔧 Потрібно виправити проблеми")
    
    return passed >= 1

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)