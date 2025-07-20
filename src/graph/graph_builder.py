"""
Graph Builder - integration between OSM data and Neo4j
"""
import sys
from pathlib import Path
import logging
from typing import Dict, Any, List

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.data.osm_extractor import geo_osm_extractor
from src.graph.neo4j_client import geo_neo4j_client

logger = logging.getLogger(__name__)

class GeoRetailGraphBuilder:
    """Build knowledge graph from OSM data for retail location analysis"""
    
    def __init__(self):
        self.osm_extractor = geo_osm_extractor
        self.neo4j_client = geo_neo4j_client
    
    def build_location_graph(self, lat: float, lon: float, 
                           location_name: str = None, **metadata) -> Dict[str, Any]:
        """Build complete knowledge graph for a location"""
        
        logger.info(f"🏗️  Building graph for location: {lat:.6f}, {lon:.6f}")
        
        try:
            # Step 1: Extract OSM data
            logger.info("📍 Extracting OSM data...")
            location_data = self.osm_extractor.extract_location_data(lat, lon)
            
            # Step 2: Create location node
            logger.info("🎯 Creating location node...")
            location_props = {
                "name": location_name or f"Location_{lat:.4f}_{lon:.4f}",
                **location_data["spatial_metrics"],
                **metadata
            }
            
            location_id = self.neo4j_client.create_location_node(
                lat=lat, 
                lon=lon, 
                **location_props
            )
            
            # Step 3: Create POI nodes
            logger.info(f"🏪 Creating {len(location_data['pois'])} POI nodes...")
            poi_count = 0
            
            for poi_data in location_data['pois']:
                try:
                    self.neo4j_client.create_poi_node(poi_data)
                    poi_count += 1
                except Exception as e:
                    logger.warning(f"Failed to create POI {poi_data.get('osm_id')}: {e}")
                    continue
            
            # Step 4: Create spatial relationships
            logger.info("🔗 Creating spatial relationships...")
            relationship_count = self.neo4j_client.create_spatial_relationships(
                location_id, 
                radius_km=self.osm_extractor.radius_meters / 1000
            )
            
            # Step 5: Generate summary
            summary = {
                "location_id": location_id,
                "coordinates": {"lat": lat, "lon": lon},
                "pois_created": poi_count,
                "relationships_created": relationship_count,
                "spatial_metrics": location_data["spatial_metrics"],
                "osm_data": {
                    "buildings_count": location_data["buildings"]["count"],
                    "road_edges": location_data["road_network"]["edges_count"],
                    "landuse_types": len(location_data["landuse"]["types"])
                }
            }
            
            logger.info(f"✅ Graph built successfully for {location_id}")
            logger.info(f"   📊 POIs: {poi_count}, Relationships: {relationship_count}")
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ Graph building failed: {e}")
            raise
    
    def build_multiple_locations(self, locations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build graphs for multiple locations"""
        
        logger.info(f"🏗️  Building graphs for {len(locations)} locations...")
        
        results = []
        
        for i, location in enumerate(locations, 1):
            try:
                logger.info(f"Processing location {i}/{len(locations)}")
                
                lat = location["lat"]
                lon = location["lon"]
                name = location.get("name")
                metadata = {k: v for k, v in location.items() 
                           if k not in ["lat", "lon", "name"]}
                
                result = self.build_location_graph(lat, lon, name, **metadata)
                results.append(result)
                
            except Exception as e:
                logger.error(f"Failed to process location {i}: {e}")
                results.append({
                    "error": str(e),
                    "location": location
                })
                continue
        
        logger.info(f"✅ Completed processing {len(locations)} locations")
        return results
    
    def enrich_existing_location(self, location_id: str) -> Dict[str, Any]:
        """Enrich existing location with additional analysis"""
        
        logger.info(f"🔍 Enriching location: {location_id}")
        
        try:
            # Get current location summary
            summary = self.neo4j_client.get_location_summary(location_id)
            
            if not summary:
                raise ValueError(f"Location {location_id} not found")
            
            # Add competitive analysis
            competitive_analysis = self._analyze_competition(location_id)
            
            # Add accessibility analysis
            accessibility_analysis = self._analyze_accessibility(location_id)
            
            enriched_summary = {
                **summary,
                "competitive_analysis": competitive_analysis,
                "accessibility_analysis": accessibility_analysis,
                "enrichment_timestamp": "now()"
            }
            
            logger.info(f"✅ Location {location_id} enriched successfully")
            return enriched_summary
            
        except Exception as e:
            logger.error(f"❌ Location enrichment failed: {e}")
            raise
    
    def _analyze_competition(self, location_id: str) -> Dict[str, Any]:
        """Analyze competitive landscape around location"""
        
        query = """
        MATCH (l:Location {location_id: $location_id})-[:NEAR_POI]->(p:POI)
        WHERE p.category IN ['shopping', 'food']
        WITH l, p.category as category, count(p) as count
        RETURN category, count
        ORDER BY count DESC
        """
        
        try:
            result = self.neo4j_client.execute_query(query, {"location_id": location_id})
            
            competition_score = 0
            category_counts = {}
            
            for record in result:
                category = record["category"]
                count = record["count"]
                category_counts[category] = count
                competition_score += count * 0.1  # Simple scoring
            
            return {
                "competition_score": min(competition_score, 10.0),  # Cap at 10
                "category_counts": category_counts,
                "total_competitors": sum(category_counts.values())
            }
            
        except Exception as e:
            logger.warning(f"Competition analysis failed: {e}")
            return {"competition_score": 0, "category_counts": {}, "total_competitors": 0}
    
    def _analyze_accessibility(self, location_id: str) -> Dict[str, Any]:
        """Analyze accessibility and transport connectivity"""
        
        query = """
        MATCH (l:Location {location_id: $location_id})-[:NEAR_POI]->(p:POI)
        WHERE p.category = 'transport'
        WITH l, count(p) as transport_count, avg(p.distance_km) as avg_distance
        RETURN transport_count, avg_distance
        """
        
        try:
            result = self.neo4j_client.execute_query(query, {"location_id": location_id})
            
            if result:
                transport_count = result[0]["transport_count"] or 0
                avg_distance = result[0]["avg_distance"] or 1.0
            else:
                transport_count = 0
                avg_distance = 1.0
            
            # Calculate accessibility score (0-10)
            accessibility_score = min(transport_count * 2.0, 10.0) * (1.0 / max(avg_distance, 0.1))
            accessibility_score = min(accessibility_score, 10.0)
            
            return {
                "accessibility_score": accessibility_score,
                "transport_options": transport_count,
                "avg_transport_distance": avg_distance
            }
            
        except Exception as e:
            logger.warning(f"Accessibility analysis failed: {e}")
            return {"accessibility_score": 0, "transport_options": 0, "avg_transport_distance": 1.0}
    
    def get_database_overview(self) -> Dict[str, Any]:
        """Get comprehensive database overview"""
        
        try:
            # Get basic stats
            stats = self.neo4j_client.get_database_stats()
            
            # Get category distribution
            category_query = """
            MATCH (p:POI)
            WITH p.category as category, count(p) as count
            RETURN category, count
            ORDER BY count DESC
            LIMIT 10
            """
            
            category_result = self.neo4j_client.execute_query(category_query)
            category_distribution = {record["category"]: record["count"] 
                                   for record in category_result}
            
            return {
                "database_stats": stats,
                "category_distribution": category_distribution,
                "top_categories": list(category_distribution.keys())[:5]
            }
            
        except Exception as e:
            logger.error(f"Database overview failed: {e}")
            return {"error": str(e)}

# Global instance
geo_graph_builder = GeoRetailGraphBuilder()