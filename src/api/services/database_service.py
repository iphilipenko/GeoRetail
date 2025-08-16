# src/api/services/database_service.py
"""
🗄️ Database Service для H3 Modal API
Простий та швидкий доступ до PostgreSQL з готовим connection string
"""

import psycopg2
import psycopg2.extras
from typing import List, Dict, Any, Optional, Tuple
import logging
import json
from contextlib import contextmanager
from datetime import datetime
import h3

# Налаштування логування
logger = logging.getLogger(__name__)

# З існуючої конфігурації
DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"

class DatabaseService:
    """Сервіс для роботи з PostgreSQL базою даних"""
    
    def __init__(self, connection_string: str = DB_CONNECTION_STRING):
        self.connection_string = connection_string
        self._connection = None
        
    @contextmanager
    def get_connection(self):
        """Context manager для з'єднання з БД"""
        conn = None
        try:
            conn = psycopg2.connect(
                self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """Виконання SELECT запиту"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                return [dict(row) for row in cur.fetchall()]
    
    def execute_single(self, query: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """Виконання запиту що повертає один рядок"""
        results = self.execute_query(query, params)
        return results[0] if results else None
    
    def execute_scalar(self, query: str, params: Optional[Tuple] = None) -> Any:
        """Виконання запиту що повертає одне значення"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params or ())
                result = cur.fetchone()
                return result[0] if result else None
    
    def test_connection(self) -> Dict[str, Any]:
        """Тест з'єднання з базою даних"""
        try:
            print(f"🔍 Attempting to connect to: {self.connection_string}")
            
            with self.get_connection() as conn:
                print("✅ Connection established successfully")
                
                with conn.cursor() as cur:
                    print("✅ Cursor created successfully")
                    
                    # Базова перевірка
                    print("🔍 Executing SELECT version()...")
                    cur.execute("SELECT version()")
                    version_result = cur.fetchone()
                    print(f"✅ Version query result: {version_result}")
                    
                    if version_result and len(version_result) > 0:
                        version = version_result[0]
                        print(f"✅ PostgreSQL version: {version}")
                    else:
                        print("❌ No version result")
                        return {
                            "status": "error", 
                            "error": "No version returned from database",
                            "connection_time": datetime.now().isoformat()
                        }
                    
                    # Перевірка схеми osm_ukraine
                    print("🔍 Checking osm_ukraine schema...")
                    cur.execute("""
                        SELECT EXISTS (
                            SELECT schema_name 
                            FROM information_schema.schemata 
                            WHERE schema_name = 'osm_ukraine'
                        )
                    """)
                    schema_result = cur.fetchone()
                    schema_exists = schema_result[0] if schema_result else False
                    print(f"✅ Schema exists: {schema_exists}")
                    
                    # Перевірка розширень
                    print("🔍 Checking extensions...")
                    cur.execute("""
                        SELECT extname 
                        FROM pg_extension 
                        WHERE extname IN ('postgis', 'h3', 'h3_postgis')
                        ORDER BY extname
                    """)
                    ext_results = cur.fetchall()
                    extensions = [row[0] for row in ext_results] if ext_results else []
                    print(f"✅ Extensions found: {extensions}")
                    
                    # Перевірка таблиць
                    print("🔍 Checking tables...")
                    cur.execute("""
                        SELECT table_name, 
                               (SELECT COUNT(*) FROM information_schema.columns 
                                WHERE table_schema = 'osm_ukraine' 
                                AND table_name = t.table_name) as column_count
                        FROM information_schema.tables t
                        WHERE table_schema = 'osm_ukraine'
                        ORDER BY table_name
                    """)
                    table_results = cur.fetchall()
                    tables = [dict(row) for row in table_results] if table_results else []
                    print(f"✅ Tables found: {len(tables)} tables")
                    
                    result = {
                        "status": "success",
                        "version": version,
                        "schema_osm_ukraine_exists": schema_exists,
                        "extensions": extensions,
                        "tables": tables,
                        "connection_time": datetime.now().isoformat()
                    }
                    
                    print(f"✅ Returning successful result: {result}")
                    return result
                    
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            
            print(f"❌ Database connection error: {error_type}: {error_msg}")
            
            import traceback
            print(f"❌ Full traceback: {traceback.format_exc()}")
            
            return {
                "status": "error",
                "error_type": error_type,
                "error": error_msg,
                "connection_string_used": self.connection_string,
                "connection_time": datetime.now().isoformat(),
                "traceback": traceback.format_exc()
            }
    
    # ===============================================
    # H3 та POI специфічні методи
    # ===============================================
    
    def get_poi_in_hexagon(self, h3_index: str, include_neighbors: bool = False) -> List[Dict[str, Any]]:
        """Отримання POI в гексагоні H3"""
        try:
            if include_neighbors:
                # Отримуємо сусідні гексагони - H3 v4.x
                neighbors = h3.grid_disk(h3_index, 1)  # 1 кільце сусідів
                h3_indices = list(neighbors)
            else:
                h3_indices = [h3_index]
            
            # Заглушка: в реальності тут буде запит до таблиці poi_processed
            # На даний момент повертаємо тестові дані
            
            placeholder_pois = []
            for i, h3_idx in enumerate(h3_indices):
                # Імітуємо кілька POI в кожному гексагоні
                for j in range(2):  # 2 POI на гексагон
                    # H3 v4.x координати
                    coords = h3.cell_to_latlng(h3_idx)
                    poi = {
                        "poi_id": f"poi_{h3_idx}_{j}",
                        "osm_id": 1000000 + i * 10 + j,
                        "name": f"Test POI {i}-{j}",
                        "canonical_name": f"Brand {(i+j) % 5}",
                        "primary_category": "retail",
                        "secondary_category": "convenience",
                        "influence_weight": round(0.5 + (i + j) * 0.1, 1),
                        "h3_index": h3_idx,
                        "latitude": coords[0],
                        "longitude": coords[1],
                        "distance_from_center": i * 100  # метри
                    }
                    placeholder_pois.append(poi)
            
            return placeholder_pois
            
        except Exception as e:
            logger.error(f"Error getting POI in hexagon {h3_index}: {e}")
            return []
    
    def get_h3_analytics(self, h3_index: str) -> Optional[Dict[str, Any]]:
        """Отримання аналітичних даних для H3 гексагона"""
        try:
            # Заглушка: в реальності запит до h3_analytics_current
            
            # Генеруємо базові метрики на основі H3 індексу - H3 v4.x
            coords = h3.cell_to_latlng(h3_index)
            resolution = h3.get_resolution(h3_index)
            area = h3.average_hexagon_area(resolution, unit='km^2')
            
            # Імітуємо метрики
            mock_metrics = {
                "h3_index": h3_index,
                "resolution": resolution,
                "center_lat": coords[0],
                "center_lon": coords[1],
                "area_km2": round(area, 6),
                "poi_density": round((hash(h3_index) % 50) / 10.0, 1),  # 0-5 POI/км²
                "population_estimate": (hash(h3_index) % 1000) + 100,
                "foot_traffic_score": round((hash(h3_index) % 100) / 100.0, 2),
                "competition_score": round((hash(h3_index[:8]) % 100) / 100.0, 2),
                "transport_accessibility": round((hash(h3_index[-6:]) % 100) / 100.0, 2),
                "data_quality_score": 0.85,
                "last_updated": datetime.now().isoformat()
            }
            
            return mock_metrics
            
        except Exception as e:
            logger.error(f"Error getting H3 analytics for {h3_index}: {e}")
            return None
    
    def get_competitive_analysis(self, h3_index: str, radius_rings: int = 2) -> Dict[str, Any]:
        """Конкурентний аналіз навколо гексагона"""
        try:
            # Отримуємо всі гексагони в радіусі - H3 v4.x
            area_hexagons = h3.grid_disk(h3_index, radius_rings)
            
            # Заглушка конкурентного аналізу
            competitors = []
            for i, hex_id in enumerate(list(area_hexagons)[:10]):  # Обмежуємо до 10
                if hex_id != h3_index:  # Виключаємо центральний
                    coords = h3.cell_to_latlng(hex_id)
                    competitor = {
                        "h3_index": hex_id,
                        "name": f"Competitor {i+1}",
                        "brand": f"Brand {(i % 3) + 1}",
                        "latitude": coords[0],
                        "longitude": coords[1],
                        "competition_strength": round((hash(hex_id) % 100) / 100.0, 2),
                        "distance_rings": min(radius_rings, (hash(hex_id) % radius_rings) + 1)
                    }
                    competitors.append(competitor)
            
            # Метрики конкуренції
            analysis = {
                "center_h3": h3_index,
                "radius_rings": radius_rings,
                "total_hexagons_analyzed": len(area_hexagons),
                "competitors_found": len(competitors),
                "competitors": competitors,
                "competition_summary": {
                    "market_saturation": round((len(competitors) / len(area_hexagons)) * 100, 1),
                    "average_competition_strength": round(
                        sum(c["competition_strength"] for c in competitors) / max(len(competitors), 1), 2
                    ),
                    "dominant_brands": ["Brand 1", "Brand 2", "Brand 3"][:3]
                },
                "recommendations": {
                    "market_opportunity": "medium" if len(competitors) < 5 else "low",
                    "optimal_positioning": "differentiation",
                    "risk_factors": ["High competition", "Market saturation"] if len(competitors) > 7 else ["Moderate competition"]
                }
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in competitive analysis for {h3_index}: {e}")
            return {
                "error": str(e),
                "center_h3": h3_index,
                "competitors": []
            }
    
    def check_h3_exists(self, h3_index: str) -> bool:
        """Перевірка чи існує H3 індекс в базі даних"""
        try:
            # Спочатку перевіряємо валідність H3 індексу - H3 v4.x
            if not h3.is_valid_cell(h3_index):
                return False
            
            # В реальності тут буде запит до БД
            # Поки що повертаємо True для всіх валідних H3 індексів
            return True
            
        except Exception as e:
            logger.error(f"Error checking H3 existence for {h3_index}: {e}")
            return False

# Глобальний екземпляр сервісу
db_service = DatabaseService()

# Функції для зручного використання
def get_database_service() -> DatabaseService:
    """Отримання екземпляру database service"""
    return db_service

def test_database_connection() -> Dict[str, Any]:
    """Швидкий тест підключення до БД"""
    return db_service.test_connection()
