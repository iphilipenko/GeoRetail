"""
OSM Data Extractor for GeoRetail with enhanced spatial analysis
"""
import osmnx as ox
import geopandas as gpd
import pandas as pd
from typing import Dict, List, Tuple, Any
from pathlib import Path
import logging
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from config.settings import settings

logger = logging.getLogger(__name__)

class GeoRetailOSMExtractor:
    """Advanced OSM data extractor for retail location analysis"""
    
    def __init__(self, radius_meters: int = None):
        self.radius_meters = radius_meters or settings.osm_radius_meters
        self.cache_dir = Path(settings.osm_cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure OSMnx for better performance
        ox.config(
            use_cache=True, 
            cache_folder=str(self.cache_dir),
            log_console=True
        )
    
    def extract_location_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """Extract comprehensive location data from OSM"""
        location = (lat, lon)
        
        logger.info(f"📍 Extracting OSM data for: {lat:.6f}, {lon:.6f}")
        
        try:
            # Extract different data layers
            road_network = self._extract_road_network(location)
            pois = self._extract_pois(location)
            buildings = self._extract_buildings(location)
            landuse = self._extract_landuse(location)
            
            # Calculate comprehensive spatial metrics
            spatial_metrics = self._calculate_spatial_metrics(
                road_network, pois, buildings, landuse, location
            )
            
            location_data = {
                "coordinates": {"lat": lat, "lon": lon},
                "road_network": road_network,
                "pois": pois,
                "buildings": buildings,
                "landuse": landuse,
                "spatial_metrics": spatial_metrics,
                "extraction_radius_m": self.radius_meters
            }
            
            logger.info(f"✅ Extracted: {len(pois)} POIs, {buildings['count']} buildings")
            return location_data
            
        except Exception as e:
            logger.error(f"❌ OSM extraction failed: {e}")
            raise
    
    def _extract_road_network(self, location: Tuple[float, float]) -> Dict[str, Any]:
        """Extract and analyze road network"""
        try:
            # Download road network
            graph = ox.graph_from_point(
                location, 
                dist=self.radius_meters, 
                network_type='drive',
                simplify=True
            )
            
            # Convert to GeoDataFrames for analysis
            nodes_gdf, edges_gdf = ox.graph_to_gdfs(graph)
            
            # Calculate network metrics
            network_metrics = self._calculate_network_metrics(graph, edges_gdf)
            
            return {
                "graph": graph,
                "nodes_count": len(nodes_gdf),
                "edges_count": len(edges_gdf),
                "total_length_km": edges_gdf['length'].sum() / 1000,
                "metrics": network_metrics
            }
            
        except Exception as e:
            logger.warning(f"Road network extraction failed: {e}")
            return {
                "graph": None, 
                "nodes_count": 0, 
                "edges_count": 0,
                "total_length_km": 0,
                "metrics": {}
            }
    
    def _extract_pois(self, location: Tuple[float, float]) -> List[Dict[str, Any]]:
        """Extract Points of Interest with retail focus"""
        
        # Enhanced POI categories for retail analysis
        poi_tags = {
            'amenity': [
                # Food & Beverage
                'restaurant', 'cafe', 'fast_food', 'bar', 'pub', 'food_court',
                # Retail
                'marketplace', 'shopping_mall', 'supermarket', 'convenience',
                # Services
                'bank', 'atm', 'pharmacy', 'post_office', 'fuel',
                # Public Services
                'hospital', 'clinic', 'school', 'university', 'library',
                # Transport
                'bus_station', 'taxi', 'parking'
            ],
            'shop': True,  # All types of shops
            'office': True,  # All types of offices
            'leisure': ['park', 'fitness_centre', 'sports_centre', 'cinema', 'theatre'],
            'tourism': ['hotel', 'attraction', 'museum', 'gallery'],
            'public_transport': ['station', 'stop_position', 'platform']
        }
        
        all_pois = []
        
        for tag_key, tag_values in poi_tags.items():
            try:
                if tag_values is True:
                    # Get all values for this tag
                    features = ox.features_from_point(
                        location, 
                        tags={tag_key: True}, 
                        dist=self.radius_meters
                    )
                    if not features.empty:
                        all_pois.extend(self._process_poi_features(features, tag_key))
                else:
                    # Get specific values
                    for tag_value in tag_values:
                        try:
                            features = ox.features_from_point(
                                location,
                                tags={tag_key: tag_value},
                                dist=self.radius_meters
                            )
                            if not features.empty:
                                all_pois.extend(self._process_poi_features(features, tag_key, tag_value))
                        except:
                            continue
                            
            except Exception as e:
                logger.warning(f"Failed to extract {tag_key} POIs: {e}")
                continue
        
        # Remove duplicates based on coordinates
        unique_pois = self._deduplicate_pois(all_pois)
        
        logger.info(f"Extracted {len(unique_pois)} unique POIs")
        return unique_pois
    
    def _process_poi_features(self, features: gpd.GeoDataFrame, 
                            tag_key: str, tag_value: str = None) -> List[Dict[str, Any]]:
        """Process POI features into standardized format"""
        pois = []
        
        for idx, feature in features.iterrows():
            try:
                # Get centroid coordinates
                if feature.geometry.geom_type == 'Point':
                    coords = (feature.geometry.y, feature.geometry.x)
                else:
                    centroid = feature.geometry.centroid
                    coords = (centroid.y, centroid.x)
                
                # Extract POI data
                poi_data = {
                    "osm_id": str(idx),
                    "lat": coords[0],
                    "lon": coords[1],
                    "poi_type": tag_key,
                    "amenity": tag_value or feature.get(tag_key, 'unknown'),
                    "name": feature.get('name', 'Unknown'),
                    "geometry_type": feature.geometry.geom_type
                }
                
                # Add relevant OSM tags
                relevant_tags = [
                    'brand', 'cuisine', 'opening_hours', 'phone', 'website',
                    'wheelchair', 'wifi', 'payment:cash', 'payment:cards'
                ]
                
                for tag in relevant_tags:
                    if tag in feature.index and pd.notna(feature[tag]):
                        poi_data[f"osm_{tag}"] = str(feature[tag])
                
                pois.append(poi_data)
                
            except Exception as e:
                logger.warning(f"Failed to process POI {idx}: {e}")
                continue
        
        return pois
    
    def _deduplicate_pois(self, pois: List[Dict[str, Any]], 
                         threshold_meters: float = 10) -> List[Dict[str, Any]]:
        """Remove duplicate POIs based on proximity"""
        if not pois:
            return pois
        
        unique_pois = []
        
        for poi in pois:
            is_duplicate = False
            poi_lat, poi_lon = poi['lat'], poi['lon']
            
            for unique_poi in unique_pois:
                # Simple distance calculation (approximation)
                lat_diff = abs(poi_lat - unique_poi['lat'])
                lon_diff = abs(poi_lon - unique_poi['lon'])
                
                # Rough distance in meters (works for small distances)
                distance_m = ((lat_diff * 111000) ** 2 + (lon_diff * 111000) ** 2) ** 0.5
                
                if distance_m < threshold_meters:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                unique_pois.append(poi)
        
        return unique_pois
    
    def _extract_buildings(self, location: Tuple[float, float]) -> Dict[str, Any]:
        """Extract and analyze building data"""
        try:
            buildings = ox.features_from_point(
                location,
                tags={'building': True},
                dist=self.radius_meters
            )
            
            if buildings.empty:
                return {"count": 0, "total_area": 0, "types": {}, "density": 0}
            
            # Analyze building types
            building_types = buildings.get('building', pd.Series()).value_counts().to_dict()
            
            # Calculate total built area (approximate)
            total_area = 0
            if 'geometry' in buildings.columns:
                # Project to meter-based CRS for area calculation
                buildings_projected = buildings.to_crs('EPSG:3857')
                total_area = buildings_projected.geometry.area.sum()
            
            # Calculate building density
            area_km2 = (self.radius_meters / 1000) ** 2 * 3.14159
            density = len(buildings) / area_km2 if area_km2 > 0 else 0
            
            return {
                "count": len(buildings),
                "total_area": total_area,
                "types": building_types,
                "density": density
            }
            
        except Exception as e:
            logger.warning(f"Building extraction failed: {e}")
            return {"count": 0, "total_area": 0, "types": {}, "density": 0}
    
    def _extract_landuse(self, location: Tuple[float, float]) -> Dict[str, Any]:
        """Extract land use data"""
        try:
            landuse = ox.features_from_point(
                location,
                tags={'landuse': True},
                dist=self.radius_meters
            )
            
            if landuse.empty:
                return {"types": {}, "commercial_ratio": 0}
            
            landuse_types = landuse.get('landuse', pd.Series()).value_counts().to_dict()
            
            # Calculate commercial land use ratio
            commercial_types = ['commercial', 'retail', 'industrial']
            commercial_count = sum(landuse_types.get(t, 0) for t in commercial_types)
            commercial_ratio = commercial_count / len(landuse) if len(landuse) > 0 else 0
            
            return {
                "types": landuse_types,
                "commercial_ratio": commercial_ratio
            }
            
        except Exception as e:
            logger.warning(f"Landuse extraction failed: {e}")
            return {"types": {}, "commercial_ratio": 0}
    
    def _calculate_network_metrics(self, graph, edges_gdf) -> Dict[str, float]:
        """Calculate road network connectivity metrics"""
        try:
            metrics = {}
            
            if graph and len(graph.nodes) > 0:
                # Basic connectivity metrics
                metrics['average_degree'] = sum(dict(graph.degree()).values()) / len(graph.nodes)
                metrics['edge_density'] = len(graph.edges) / len(graph.nodes) if len(graph.nodes) > 0 else 0
                
                # Road type diversity
                if 'highway' in edges_gdf.columns:
                    road_types = edges_gdf['highway'].value_counts()
                    metrics['road_type_diversity'] = len(road_types)
                    metrics['primary_roads_ratio'] = road_types.get('primary', 0) / len(edges_gdf)
                
            return metrics
            
        except Exception as e:
            logger.warning(f"Network metrics calculation failed: {e}")
            return {}
    
    def _calculate_spatial_metrics(self, road_network: Dict, pois: List, 
                                 buildings: Dict, landuse: Dict, 
                                 location: Tuple[float, float]) -> Dict[str, float]:
        """Calculate comprehensive spatial metrics for location analysis"""
        
        # Calculate circular area
        area_km2 = (self.radius_meters / 1000) ** 2 * 3.14159
        
        # POI-based metrics
        poi_density = len(pois) / area_km2 if area_km2 > 0 else 0
        
        # POI diversity
        poi_categories = set()
        poi_by_category = {}
        
        for poi in pois:
            category = poi.get('poi_type', 'unknown')
            poi_categories.add(category)
            poi_by_category[category] = poi_by_category.get(category, 0) + 1
        
        poi_diversity = len(poi_categories)
        
        # Retail-specific metrics
        retail_keywords = ['shop', 'supermarket', 'mall', 'store', 'market']
        retail_pois = [poi for poi in pois 
                      if any(keyword in poi.get('amenity', '').lower() 
                           for keyword in retail_keywords)]
        retail_density = len(retail_pois) / area_km2 if area_km2 > 0 else 0
        
        # Transport accessibility
        transport_keywords = ['bus', 'metro', 'subway', 'station', 'stop']
        transport_pois = [poi for poi in pois 
                         if any(keyword in poi.get('amenity', '').lower() 
                              for keyword in transport_keywords)]
        transport_accessibility = len(transport_pois) / area_km2 if area_km2 > 0 else 0
        
        return {
            "area_km2": area_km2,
            "poi_density": poi_density,
            "poi_diversity": poi_diversity,
            "retail_density": retail_density,
            "transport_accessibility": transport_accessibility,
            "building_density": buildings.get("density", 0),
            "road_density": road_network.get("edges_count", 0) / area_km2 if area_km2 > 0 else 0,
            "commercial_landuse_ratio": landuse.get("commercial_ratio", 0),
            "poi_categories": poi_by_category
        }

# Global extractor instance
geo_osm_extractor = GeoRetailOSMExtractor()