"""
Test basic functionality of core modules
"""
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

def test_neo4j_client():
    """Test Neo4j client functionality"""
    print("🗃️  Testing Neo4j Client...")
    
    try:
        from src.graph.neo4j_client import neo4j_client
        
        # Test connection
        result = neo4j_client.execute_query("RETURN 'Hello GeoRetail!' as message")
        print(f"  ✅ Connection: {result[0]['message']}")
        
        # Setup schema
        neo4j_client.setup_graph_schema()
        print("  ✅ Schema setup complete")
        
        return True
    except Exception as e:
        print(f"  ❌ Neo4j test failed: {e}")
        return False

def test_osm_extractor():
    """Test OSM data extraction"""
    print("\n🗺️  Testing OSM Extractor...")
    
    try:
        from src.data.osm_extractor import osm_extractor
        
        # Test with Kyiv center
        location_data = osm_extractor.extract_location_data(50.4501, 30.5234)
        
        print(f"  ✅ POIs found: {len(location_data['pois'])}")
        print(f"  ✅ Buildings: {location_data['buildings']['count']}")
        print(f"  ✅ Road nodes: {location_data['road_network']['nodes_count']}")
        
        return True
    except Exception as e:
        print(f"  ❌ OSM test failed: {e}")
        return False

def test_integration():
    """Test integration between components"""
    print("\n🔗 Testing Integration...")
    
    try:
        from src.graph.neo4j_client import neo4j_client
        from src.data.osm_extractor import osm_extractor
        
        # Extract data for a location
        lat, lon = 50.4501, 30.5234
        location_data = osm_extractor.extract_location_data(lat, lon)
        
        # Create location node
        location_id = neo4j_client.create_location_node(
            lat=lat, 
            lon=lon,
            poi_count=len(location_data['pois']),
            building_count=location_data['buildings']['count']
        )
        print(f"  ✅ Created location node: {location_id}")
        
        # Create some POI nodes
        poi_count = 0
        for poi in location_data['pois'][:5]:  # First 5 POIs
            try:
                neo4j_client.create_poi_node(poi)
                poi_count += 1
            except:
                continue
        
        print(f"  ✅ Created {poi_count} POI nodes")
        
        return True
    except Exception as e:
        print(f"  ❌ Integration test failed: {e}")
        return False

def main():
    print("🚀 GeoRetail Basic Functionality Test")
    print("=" * 50)
    
    tests = [
        test_neo4j_client,
        test_osm_extractor,
        test_integration
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print("\n" + "=" * 50)
    if all(results):
        print("🎉 All tests passed! Core functionality working!")
    else:
        print("🔧 Some tests failed. Check the errors above.")
    
    return all(results)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)