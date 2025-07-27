# scripts/collect_osm_fixed.py
import requests
import time
import json
from sqlalchemy import create_engine, text
import pandas as pd
from tqdm import tqdm
import logging
from math import cos, radians

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OSMStoreCollector:
    def __init__(self):
        self.engine = create_engine('postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail')
        self.api_url = "http://localhost:8000"
    
    def get_stores_to_process(self):
        """Отримати магазини які ще не оброблені"""
        query = """
        SELECT s.shop_id, s.lat, s.lon, s.format
        FROM stores.shops s
        LEFT JOIN osm_cache.osm_extracts c ON 
            ABS(s.lat - c.center_lat) < 0.001 AND 
            ABS(s.lon - c.center_lon) < 0.001
        WHERE c.cache_id IS NULL
        ORDER BY s.shop_id
        """
        df = pd.read_sql(query, self.engine)
        
        # Явно конвертуємо в float
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        
        logger.info(f"📊 Found {len(df)} stores to process")
        return df
    
    def extract_osm_data(self, lat, lon, radius=500):
        """Витягти OSM дані через API"""
        try:
            params = {
                "lat": float(lat), 
                "lon": float(lon), 
                "radius": int(radius)
            }
            
            logger.info(f"🌐 POST request with params: {params}")
            
            response = requests.post(
                f"{self.api_url}/osm/extract", 
                params=params,  # Query parameters для POST
                timeout=120
            )
            
            logger.info(f"📡 Status: {response.status_code}, URL: {response.url}")
            
            if response.status_code == 200:
                result = response.json()
                pois = result.get('extraction_summary', {}).get('total_pois', 0)
                logger.info(f"✅ Success: {pois} POIs")
                return result
            else:
                logger.error(f"❌ Error {response.status_code}: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Exception: {e}")
            return None
    
    def save_to_cache(self, lat, lon, radius, osm_data):
        """Зберегти в osm_cache.osm_extracts"""
        if not osm_data or osm_data.get("status") != "success":
            return False
            
        try:
            data = osm_data.get("data", {})
            summary = osm_data.get("extraction_summary", {})
            
            lat_float = float(lat)
            lon_float = float(lon)
            lat_offset = radius / 111000
            lon_offset = radius / (111000 * abs(cos(radians(lat_float))))
            
            bbox_wkt = f"POLYGON(({lon_float-lon_offset} {lat_float-lat_offset}, {lon_float+lon_offset} {lat_float-lat_offset}, {lon_float+lon_offset} {lat_float+lat_offset}, {lon_float-lon_offset} {lat_float+lat_offset}, {lon_float-lon_offset} {lat_float-lat_offset}))"
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO osm_cache.osm_extracts 
                    (bbox, center_lat, center_lon, radius_meters, osm_data, 
                     pois_count, roads_count, buildings_count)
                    VALUES (ST_GeomFromText(:bbox, 4326), :lat, :lon, :radius, 
                            :osm_data, :pois_count, :roads_count, :buildings_count)
                """), {
                    'bbox': bbox_wkt,
                    'lat': lat_float,
                    'lon': lon_float, 
                    'radius': radius,
                    'osm_data': json.dumps(data),
                    'pois_count': summary.get('total_pois', 0),
                    'roads_count': data.get('road_network', {}).get('edges_count', 0),
                    'buildings_count': summary.get('total_buildings', 0)
                })
                conn.commit()
                
            logger.info(f"💾 Saved to database")
            return True
                
        except Exception as e:
            logger.error(f"❌ Save error: {e}")
            return False
    
    def process_stores(self):
        """Обробити магазини"""
        stores = self.get_stores_to_process()
        
        if len(stores) == 0:
            logger.info("✅ All stores processed!")
            return
            
        success = 0
        errors = 0
        
        for _, store in stores.iterrows():
            logger.info(f"\n🏪 Shop {store['shop_id']} ({store['format']})")
            
            osm_data = self.extract_osm_data(store['lat'], store['lon'])
            
            if osm_data and self.save_to_cache(store['lat'], store['lon'], 500, osm_data):
                success += 1
            else:
                errors += 1
            
            time.sleep(2)
        
        logger.info(f"\n📈 Results: ✅{success} ❌{errors}")

if __name__ == "__main__":
    collector = OSMStoreCollector()
    collector.process_stores()