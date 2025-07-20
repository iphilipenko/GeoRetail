"""
Test full GeoRetail system integration
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.graph.graph_builder import geo_graph_builder
from src.graph.neo4j_client import geo_neo4j_client

def test_schema_setup():
    """Test database schema setup"""
    print("🏗️  Testing schema setup...")
    
    try:
        geo_neo4j_client.setup_graph_schema()
        print("  ✅ Schema setup successful")
        return True
    except Exception as e:
        print(f"  ❌ Schema setup failed: {e}")
        return False

def test_single_location_graph():
    """Test building graph for single location"""
    print("\n📍 Testing single location graph building...")
    
    try:
        # Test with Kyiv center
        result = geo_graph_builder.build_location_graph(
            lat=50.4501,
            lon=30.5234,
            location_name="Kyiv Center Test",
            test_location=True
        )
        
        print(f"  ✅ Location ID: {result['location_id']}")
        print(f"  ✅ POIs created: {result['pois_created']}")
        print(f"  ✅ Relationships: {result['relationships_created']}")
        print(f"  📊 POI density: {result['spatial_metrics']['poi_density']:.2f}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Single location test failed: {e}")
        return False

def test_location_enrichment():
    """Test location enrichment functionality"""
    print("\n🔍 Testing location enrichment...")
    
    try:
        # Find a location to enrich
        locations_query = "MATCH (l:Location) RETURN l.location_id as id LIMIT 1"
        result = geo_neo4j_client.execute_query(locations_query)
        
        if not result:
            print("  ⚠️  No locations found for enrichment test")
            return False
        
        location_id = result[0]["id"]
        enriched = geo_graph_builder.enrich_existing_location(location_id)
        
        print(f"  ✅ Enriched location: {location_id}")
        print(f"  📊 Competition score: {enriched['competitive_analysis']['competition_score']:.2f}")
        print(f"  📊 Accessibility score: {enriched['accessibility_analysis']['accessibility_score']:.2f}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Location enrichment failed: {e}")
        return False

def test_database_overview():
    """Test database overview functionality"""
    print("\n📊 Testing database overview...")
    
    try:
        overview = geo_graph_builder.get_database_overview()
        
        stats = overview["database_stats"]
        print(f"  ✅ Total locations: {stats.get('locations', 0)}")
        print(f"  ✅ Total POIs: {stats.get('pois', 0)}")
        print(f"  ✅ Total relationships: {stats.get('relationships', 0)}")
        
        if overview["category_distribution"]:
            print("  📋 Top POI categories:")
            for category, count in list(overview["category_distribution"].items())[:3]:
                print(f"    - {category}: {count}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Database overview failed: {e}")
        return False

def test_multiple_locations():
    """Test building graphs for multiple locations"""
    print("\n🗺️  Testing multiple locations...")
    
    try:
        # Test locations around Kyiv
        test_locations = [
            {"lat": 50.4547, "lon": 30.5238, "name": "Khreshchatyk"},
            {"lat": 50.4433, "lon": 30.5100, "name": "Pechersk"},
            {"lat": 50.4551, "lon": 30.4914, "name": "Podil"}
        ]
        
        results = geo_graph_builder.build_multiple_locations(test_locations)
        
        successful = [r for r in results if "error" not in r]
        print(f"  ✅ Successfully processed: {len(successful)}/{len(test_locations)}")
        
        if successful:
            avg_pois = sum(r["pois_created"] for r in successful) / len(successful)
            print(f"  📊 Average POIs per location: {avg_pois:.1f}")
        
        return len(successful) > 0
        
    except Exception as e:
        print(f"  ❌ Multiple locations test failed: {e}")
        return False

def main():
    print("🚀 GeoRetail Full Integration Test Suite")
    print("=" * 60)
    
    # Warning about database operations
    print("⚠️  This test will create data in Neo4j database")
    print("   Make sure you're using a test database!")
    
    response = input("\nProceed with testing? (y/N): ")
    if response.lower() != 'y':
        print("Test cancelled.")
        return False
    
    tests = [
        ("Schema Setup", test_schema_setup),
        ("Single Location Graph", test_single_location_graph),
        ("Location Enrichment", test_location_enrichment),
        ("Database Overview", test_database_overview),
        ("Multiple Locations", test_multiple_locations)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n--- {test_name} ---")
        result = test_func()
        results.append(result)
    
    print("\n" + "=" * 60)
    print("📊 Final Results:")
    
    for i, (test_name, _) in enumerate(tests):
        status = "✅ PASS" if results[i] else "❌ FAIL"
        print(f"  {test_name}: {status}")
    
    passed = sum(results)
    total = len(results)
    
    print(f"\n📈 Overall: {passed}/{total} tests passed")
    
    if passed >= 4:
        print("\n🎉 GeoRetail system готовий до використання!")
        print("🚀 Наступні кроки:")
        print("   1. Graph embeddings (Node2Vec/GraphSAGE)")
        print("   2. ML pipeline для прогнозування виторгу")
        print("   3. REST API для інтеграції")
    else:
        print("\n🔧 Деякі компоненти потребують виправлення")
    
    return passed >= 4

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    finally:
        # Always close connections
        try:
            geo_neo4j_client.close()
        except:
            pass