"""
GeoRetail Neo4j Client with full spatial capabilities
"""
from neo4j import GraphDatabase
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
import sys
import json

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.settings import settings

logger = logging.getLogger(__name__)

class GeoRetailNeo4jClient:
    """Enhanced Neo4j client for GeoRetail with spatial capabilities"""
    
    def __init__(self):
        self.driver = None
        self.connect()
    
    def connect(self):
        """Establish connection to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_user, settings.neo4j_password)
            )
            # Test connection
            with self.driver.session(database=settings.neo4j_database) as session:
                session.run("RETURN 1")
            logger.info("✅ Connected to Neo4j GeoRetail database")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Neo4j: {e}")
            raise
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def execute_query(self, query: str, parameters: Dict[str, Any] = None) -> List[Dict]:
        """Execute Cypher query and return results"""
        try:
            with self.driver.session(database=settings.neo4j_database) as session:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def setup_graph_schema(self):
        """Create comprehensive graph schema for GeoRetail"""
        logger.info("🏗️  Setting up GeoRetail graph schema...")
        
        schema_queries = [
            # Constraints for unique identifiers
            "CREATE CONSTRAINT location_id_unique IF NOT EXISTS FOR (l:Location) REQUIRE l.location_id IS UNIQUE",
            "CREATE CONSTRAINT poi_osm_id_unique IF NOT EXISTS FOR (p:POI) REQUIRE p.osm_id IS UNIQUE",
            "CREATE CONSTRAINT store_id_unique IF NOT EXISTS FOR (s:Store) REQUIRE s.store_id IS UNIQUE",
            
            # Indexes for spatial coordinates
            "CREATE INDEX location_coordinates IF NOT EXISTS FOR (l:Location) ON (l.lat, l.lon)",
            "CREATE INDEX poi_coordinates IF NOT EXISTS FOR (p:POI) ON (p.lat, p.lon)",
            "CREATE INDEX store_coordinates IF NOT EXISTS FOR (s:Store) ON (s.lat, s.lon)",
            
            # Indexes for categorical data
            "CREATE INDEX poi_amenity IF NOT EXISTS FOR (p:POI) ON p.amenity",
            "CREATE INDEX poi_category IF NOT EXISTS FOR (p:POI) ON p.category",
            "CREATE INDEX store_format IF NOT EXISTS FOR (s:Store) ON s.format",
            
            # Performance indexes
            "CREATE INDEX location_created IF NOT EXISTS FOR (l:Location) ON l.created_at",
            "CREATE INDEX poi_distance IF NOT EXISTS FOR ()-[r:NEAR_POI]-() ON r.distance"
        ]
        
        for query in schema_queries:
            try:
                self.execute_query(query)
                constraint_name = query.split()[2] if "CONSTRAINT" in query else query.split()[2]
                logger.info(f"  ✅ {constraint_name}")
            except Exception as e:
                logger.warning(f"  ⚠️  Schema: {e}")
    
    def clear_database(self):
        """Clear all nodes and relationships (use with caution!)"""
        logger.warning("🗑️  Clearing database...")
        self.execute_query("MATCH (n) DETACH DELETE n")
        logger.info("✅ Database cleared")
    
    def create_location_node(self, lat: float, lon: float, **properties) -> str:
        """Create location node with spatial point"""
        query = """
        CREATE (l:Location {
            location_id: $location_id,
            lat: $lat,
            lon: $lon,
            created_at: datetime(),
            point: point({latitude: $lat, longitude: $lon})
        })
        SET l += $properties
        RETURN l.location_id as location_id
        """
        
        location_id = f"loc_{lat:.6f}_{lon:.6f}".replace(".", "_").replace("-", "neg")
        parameters = {
            "location_id": location_id,
            "lat": lat,
            "lon": lon,
            "properties": properties
        }
        
        result = self.execute_query(query, parameters)
        return result[0]["location_id"] if result else location_id
    
    def create_poi_node(self, poi_data: Dict[str, Any]) -> str:
        """Create POI node with comprehensive data"""
        query = """
        MERGE (p:POI {osm_id: $osm_id})
        ON CREATE SET
            p.name = $name,
            p.amenity = $amenity,
            p.category = $category,
            p.lat = $lat,
            p.lon = $lon,
            p.point = point({latitude: $lat, longitude: $lon}),
            p.created_at = datetime()
        SET p += $additional_properties
        RETURN p.osm_id as osm_id
        """
        
        # Categorize POI for better organization
        category = self._categorize_poi(poi_data.get("amenity", ""), poi_data.get("poi_type", ""))
        
        parameters = {
            "osm_id": str(poi_data.get("osm_id")),
            "name": poi_data.get("name", "Unknown"),
            "amenity": poi_data.get("amenity"),
            "category": category,
            "lat": poi_data.get("lat"),
            "lon": poi_data.get("lon"),
            "additional_properties": {k: v for k, v in poi_data.items() 
                                   if k not in ["osm_id", "name", "amenity", "lat", "lon"]}
        }
        
        result = self.execute_query(query, parameters)
        return result[0]["osm_id"] if result else str(poi_data.get("osm_id"))
    
    def create_spatial_relationships(self, location_id: str, radius_km: float = 0.5):
        """Create spatial relationships between location and nearby POIs"""
        query = """
        MATCH (l:Location {location_id: $location_id})
        MATCH (p:POI)
        WITH l, p, point.distance(l.point, p.point) as distance
        WHERE distance <= $radius_meters
        MERGE (l)-[r:NEAR_POI]->(p)
        SET r.distance = distance,
            r.distance_km = distance / 1000.0,
            r.created_at = datetime()
        RETURN count(r) as relationships_created
        """
        
        parameters = {
            "location_id": location_id,
            "radius_meters": radius_km * 1000  # Convert to meters
        }
        
        result = self.execute_query(query, parameters)
        count = result[0]["relationships_created"] if result else 0
        logger.info(f"✅ Created {count} spatial relationships for {location_id}")
        return count
    
    def _categorize_poi(self, amenity: str, poi_type: str) -> str:
        """Categorize POI for retail analysis"""
        retail_categories = {
            "food": ["restaurant", "cafe", "fast_food", "food_court", "bar", "pub"],
            "shopping": ["supermarket", "convenience", "department_store", "mall", "shop"],
            "services": ["bank", "atm", "pharmacy", "hospital", "clinic", "post_office"],
            "education": ["school", "university", "college", "kindergarten", "library"],
            "transport": ["bus_station", "subway_entrance", "parking", "fuel"],
            "leisure": ["cinema", "theatre", "park", "fitness_centre", "gym"],
            "accommodation": ["hotel", "hostel", "guest_house"]
        }
        
        amenity_lower = amenity.lower() if amenity else ""
        
        for category, keywords in retail_categories.items():
            if amenity_lower in keywords:
                return category
        
        return "other"
    
    def get_location_summary(self, location_id: str) -> Dict[str, Any]:
        """Get comprehensive summary of location"""
        query = """
        MATCH (l:Location {location_id: $location_id})
        OPTIONAL MATCH (l)-[r:NEAR_POI]->(p:POI)
        WITH l, 
             count(p) as total_pois,
             collect(DISTINCT p.category) as categories,
             avg(r.distance_km) as avg_distance_km
        RETURN {
            location_id: l.location_id,
            coordinates: {lat: l.lat, lon: l.lon},
            total_pois: total_pois,
            categories: categories,
            avg_distance_km: avg_distance_km
        } as summary
        """
        
        result = self.execute_query(query, {"location_id": location_id})
        return result[0]["summary"] if result else {}
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        stats = {}
        
        # Count different node types
        node_queries = {
            "locations": "MATCH (l:Location) RETURN count(l) as count",
            "pois": "MATCH (p:POI) RETURN count(p) as count",
            "stores": "MATCH (s:Store) RETURN count(s) as count",
            "total_nodes": "MATCH (n) RETURN count(n) as count"
        }
        
        for stat_name, query in node_queries.items():
            try:
                result = self.execute_query(query)
                stats[stat_name] = result[0]["count"] if result else 0
            except:
                stats[stat_name] = 0
        
        # Count relationships
        rel_query = "MATCH ()-[r]->() RETURN count(r) as count"
        try:
            result = self.execute_query(rel_query)
            stats["relationships"] = result[0]["count"] if result else 0
        except:
            stats["relationships"] = 0
        
        return stats

# Global client instance
geo_neo4j_client = GeoRetailNeo4jClient()