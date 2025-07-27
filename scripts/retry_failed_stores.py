# scripts/retry_failed_stores.py
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

class RetryFailedStores:
    def __init__(self):
        self.engine = create_engine('postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail')
        self.api_url = "http://localhost:8000"
    
    def get_failed_stores(self):
        """Отримати магазини які не оброблені"""
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
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        
        logger.info(f"📊 Found {len(df)} failed stores to retry")
        return df
    
    def extract_osm_with_fallback(self, lat, lon, radius=500):
        """Витягти OSM дані з fallback стратегіями"""
        
        # Стратегія 1: Зменшити радіус до 300м
        try:
            params = {"lat": float(lat), "lon": float(lon), "radius": 300}
            
            response = requests.post(f"{self.api_url}/osm/extract", 
                                   params=params, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "success":
                    logger.info(f"✅ Success with 300m radius")
                    return result, 300
                    
        except Exception as e:
            logger.warning(f"⚠️ 300m radius failed: {e}")
        
        # Стратегія 2: Ще менший радіус 200м
        try:
            params = {"lat": float(lat), "lon": float(lon), "radius": 200}
            
            response = requests.post(f"{self.api_url}/osm/extract", 
                                   params=params, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "success":
                    logger.info(f"✅ Success with 200m radius")
                    return result, 200
                    
        except Exception as e:
            logger.warning(f"⚠️ 200m radius failed: {e}")
        
        # Стратегія 3: Мінімальний радіус 100м
        try:
            params = {"lat": float(lat), "lon": float(lon), "radius": 100}
            
            response = requests.post(f"{self.api_url}/osm/extract", 
                                   params=params, timeout=60)
            
            if response.status_code == 200:
                result = response.json()
                if result.get("status") == "success":
                    logger.info(f"✅ Success with 100m radius")
                    return result, 100
                    
        except Exception as e:
            logger.warning(f"⚠️ 100m radius failed: {e}")
        
        # Якщо всі стратегії не спрацювали
        logger.error(f"❌ All fallback strategies failed")
        return None, 0
    
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
                
            return True
                
        except Exception as e:
            logger.error(f"❌ Save error: {e}")
            return False
    
    def process_failed_stores(self):
        """Обробити проблемні магазини"""
        stores = self.get_failed_stores()
        
        if len(stores) == 0:
            logger.info("✅ No failed stores to retry!")
            return
            
        success = 0
        still_failed = 0
        
        for _, store in stores.iterrows():
            logger.info(f"\n🔄 Retrying Shop {store['shop_id']} ({store['format']})")
            
            osm_data, used_radius = self.extract_osm_with_fallback(store['lat'], store['lon'])
            
            if osm_data and self.save_to_cache(store['lat'], store['lon'], used_radius, osm_data):
                pois = osm_data.get('extraction_summary', {}).get('total_pois', 0)
                logger.info(f"✅ Success: {pois} POIs with {used_radius}m radius")
                success += 1
            else:
                logger.error(f"❌ Still failed after all attempts")
                still_failed += 1
            
            time.sleep(3)  # Затримка між спробами
        
        logger.info(f"\n📈 Retry Results:")
        logger.info(f"✅ Newly successful: {success}")
        logger.info(f"❌ Still failed: {still_failed}")
        
        # Показати фінальну статистику
        self.show_final_stats()
    
    def show_final_stats(self):
        """Показати фінальну статистику"""
        try:
            with self.engine.connect() as conn:
                # Загальна статистика
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_extracts,
                        SUM(pois_count) as total_pois,
                        AVG(pois_count) as avg_pois,
                        MAX(pois_count) as max_pois
                    FROM osm_cache.osm_extracts
                """))
                
                stats = result.fetchone()
                
                # Кількість магазинів без даних
                result2 = conn.execute(text("""
                    SELECT COUNT(*) as missing_stores
                    FROM stores.shops s
                    LEFT JOIN osm_cache.osm_extracts c ON 
                        ABS(s.lat - c.center_lat) < 0.001 AND 
                        ABS(s.lon - c.center_lon) < 0.001
                    WHERE c.cache_id IS NULL
                """))
                
                missing = result2.fetchone()[0]
                
                logger.info(f"\n🎯 Final OSM Collection Statistics:")
                logger.info(f"Total stores: 221")
                logger.info(f"Successful extracts: {stats[0]}")
                logger.info(f"Still missing: {missing}")
                logger.info(f"Success rate: {(stats[0]/221)*100:.1f}%")
                logger.info(f"Total POIs collected: {stats[1]:,}")
                logger.info(f"Average POIs per store: {stats[2]:.1f}")
                logger.info(f"Max POIs in area: {stats[3]:,}")
                
        except Exception as e:
            logger.error(f"❌ Error getting final stats: {e}")

if __name__ == "__main__":
    retrier = RetryFailedStores()
    retrier.process_failed_stores()