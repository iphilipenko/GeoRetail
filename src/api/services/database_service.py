# src/api/services/database_service.py
"""
🗄️ Database Service для H3 Modal API
Робота з реальними даними PostgreSQL - остаточна виправлена версія
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
                return list(result.values())[0] if result else None
    
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
                    
                    if version_result and 'version' in version_result:
                        version = version_result['version']
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
                        ) as exists
                    """)
                    schema_result = cur.fetchone()
                    schema_exists = schema_result['exists'] if schema_result else False
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
                    extensions = [row['extname'] for row in ext_results] if ext_results else []
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
    
    # ==============================================
    # H3 АНАЛІТИЧНІ МЕТОДИ З РЕАЛЬНИМИ ДАНИМИ
    # ==============================================
    
    def get_poi_in_hexagon(self, h3_index: str, resolution: int = 10, include_neighbors: bool = False) -> List[Dict[str, Any]]:
        """Отримання POI в заданому гексагоні з реальних даних - остаточна версія"""
        try:
            # Перевіряємо валідність H3 індексу
            if not h3.is_valid_cell(h3_index):
                logger.error(f"Invalid H3 index: {h3_index}")
                return []
            
            # Вибираємо відповідну колонку H3 в залежності від резолюції
            h3_column = f"h3_res_{resolution}" if resolution <= 10 else "h3_res_10"
            
            # Запит БЕЗ геометрії - використовуємо H3 координати
            query = f"""
                SELECT 
                    entity_id,
                    osm_id,
                    entity_type,
                    primary_category,
                    secondary_category,
                    name_standardized as name,
                    brand_normalized as brand,
                    functional_group,
                    COALESCE(influence_weight, 0.0) as influence_weight,
                    COALESCE(quality_score, 0.0) as quality_score,
                    {h3_column} as h3_index,
                    region_name,
                    COALESCE(brand_confidence, 0.0) as brand_confidence,
                    brand_match_type
                FROM osm_ukraine.poi_processed 
                WHERE {h3_column} = %s
                    AND entity_type = 'poi'
                ORDER BY 
                    CASE functional_group 
                        WHEN 'competitor' THEN 1
                        WHEN 'traffic_generator' THEN 2
                        ELSE 3
                    END,
                    quality_score DESC NULLS LAST
                LIMIT 100
            """
            
            params = [h3_index]
            results = self.execute_query(query, params)
            
            # Додаємо координати з H3 математики
            for poi in results:
                if poi.get('h3_index'):
                    try:
                        lat, lon = h3.cell_to_latlng(poi['h3_index'])
                        poi['lat'] = lat
                        poi['lon'] = lon
                        poi['distance_from_center'] = h3.grid_distance(h3_index, poi['h3_index']) * 100
                    except:
                        poi['lat'] = None
                        poi['lon'] = None
                        poi['distance_from_center'] = 0.0
                else:
                    poi['lat'] = None
                    poi['lon'] = None
                    poi['distance_from_center'] = 0.0
                
                poi['is_neighbor'] = False
            
            # Якщо включені сусідні гексагони
            if include_neighbors and len(results) > 0:
                try:
                    neighbors = list(h3.grid_ring(h3_index, 1))
                    
                    if neighbors:
                        neighbor_query = f"""
                            SELECT 
                                entity_id,
                                osm_id,
                                entity_type,
                                primary_category,
                                secondary_category,
                                name_standardized as name,
                                brand_normalized as brand,
                                functional_group,
                                COALESCE(influence_weight, 0.0) as influence_weight,
                                COALESCE(quality_score, 0.0) as quality_score,
                                {h3_column} as h3_index,
                                region_name,
                                COALESCE(brand_confidence, 0.0) as brand_confidence,
                                brand_match_type
                            FROM osm_ukraine.poi_processed 
                            WHERE {h3_column} = ANY(%s)
                                AND entity_type = 'poi'
                            ORDER BY quality_score DESC NULLS LAST
                            LIMIT 50
                        """
                        
                        neighbor_results = self.execute_query(neighbor_query, [neighbors])
                        
                        # Додаємо координати для сусідніх POI
                        for poi in neighbor_results:
                            if poi.get('h3_index'):
                                try:
                                    lat, lon = h3.cell_to_latlng(poi['h3_index'])
                                    poi['lat'] = lat
                                    poi['lon'] = lon
                                    poi['distance_from_center'] = h3.grid_distance(h3_index, poi['h3_index']) * 100
                                except:
                                    poi['lat'] = None
                                    poi['lon'] = None
                                    poi['distance_from_center'] = 0.0
                            else:
                                poi['lat'] = None
                                poi['lon'] = None
                                poi['distance_from_center'] = 0.0
                            
                            poi['is_neighbor'] = True
                        
                        results.extend(neighbor_results)
                        
                except Exception as e:
                    logger.warning(f"Could not fetch neighbor data for {h3_index}: {e}")
            
            logger.info(f"Found {len(results)} POIs for H3 {h3_index}")
            return results
            
        except Exception as e:
            logger.error(f"Error getting POI for H3 {h3_index}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return []
    
    def get_h3_analytics(self, h3_index: str, resolution: int = 10) -> Dict[str, Any]:
        """Отримання аналітичних даних для гексагона з реальних даних - остаточна версія"""
        try:
            # Перевірка валідності H3
            if not h3.is_valid_cell(h3_index):
                return {"error": "Invalid H3 index", "h3_index": h3_index}
            
            # Запит аналітичних даних
            analytics_query = """
                SELECT 
                    h3_index,
                    resolution,
                    last_updated,
                    COALESCE(poi_total_count, 0) as poi_total_count,
                    COALESCE(retail_count, 0) as retail_count,
                    COALESCE(competitor_count, 0) as competitor_count,
                    COALESCE(traffic_generator_count, 0) as traffic_generator_count,
                    COALESCE(infrastructure_count, 0) as infrastructure_count,
                    COALESCE(competitor_breakdown, '{}') as competitor_breakdown,
                    COALESCE(poi_density, 0.0) as poi_density,
                    COALESCE(retail_density, 0.0) as retail_density,
                    COALESCE(competition_intensity, 0.0) as competition_intensity,
                    COALESCE(total_positive_influence, 0.0) as total_positive_influence,
                    COALESCE(total_negative_influence, 0.0) as total_negative_influence,
                    COALESCE(net_influence_score, 0.0) as net_influence_score,
                    COALESCE(transport_accessibility_score, 0.0) as transport_accessibility_score,
                    nearest_metro_distance_km,
                    nearest_bus_stop_distance_km,
                    COALESCE(public_transport_density, 0.0) as public_transport_density,
                    COALESCE(road_density_km_per_km2, 0.0) as road_density_km_per_km2,
                    COALESCE(primary_road_access, false) as primary_road_access,
                    COALESCE(average_road_quality, 0.0) as average_road_quality,
                    COALESCE(avg_poi_quality, 0.0) as avg_poi_quality,
                    COALESCE(branded_poi_ratio, 0.0) as branded_poi_ratio,
                    COALESCE(data_completeness, 0.0) as data_completeness,
                    COALESCE(residential_indicator_score, 0.0) as residential_indicator_score,
                    COALESCE(commercial_activity_score, 0.0) as commercial_activity_score,
                    COALESCE(market_saturation_index, 0.0) as market_saturation_index,
                    COALESCE(opportunity_score, 0.0) as opportunity_score
                FROM osm_ukraine.h3_analytics_current
                WHERE h3_index = %s
                ORDER BY resolution DESC
                LIMIT 1
            """
            
            analytics_data = self.execute_single(analytics_query, [h3_index])
            
            if not analytics_data:
                return {
                    "error": "No analytics data found",
                    "h3_index": h3_index,
                    "resolution": resolution,
                    "suggestion": "Try with different H3 index or check if data exists in h3_analytics_current table"
                }
            
            # Демографічний запит тільки з доступними колонками
            demographics_query = """
                SELECT 
                    population,
                    population_density
                FROM demographics.h3_population
                WHERE hex_id = %s
            """
            
            demographics_data = self.execute_single(demographics_query, [h3_index])
            
            # Отримуємо координати центру
            lat, lon = h3.cell_to_latlng(h3_index)
            area_km2 = h3.cell_area(h3_index, unit='km^2')
            
            # Парсимо JSON поля безпечно
            try:
                competitor_breakdown = json.loads(analytics_data.get('competitor_breakdown', '{}')) if analytics_data.get('competitor_breakdown') else {}
            except:
                competitor_breakdown = {}
            
            # Формуємо повну аналітику
            analytics = {
                "h3_index": h3_index,
                "resolution": analytics_data.get('resolution', resolution),
                "center_coordinates": {"lat": lat, "lon": lon},
                "area_km2": round(area_km2, 6),
                "last_updated": str(analytics_data.get('last_updated')) if analytics_data.get('last_updated') else None,
                
                # POI статистика з реальних даних
                "poi_stats": {
                    "total_pois": int(analytics_data.get('poi_total_count', 0)),
                    "retail_count": int(analytics_data.get('retail_count', 0)),
                    "competitor_count": int(analytics_data.get('competitor_count', 0)),
                    "traffic_generators": int(analytics_data.get('traffic_generator_count', 0)),
                    "infrastructure": int(analytics_data.get('infrastructure_count', 0)),
                    "competitor_breakdown": competitor_breakdown
                },
                
                # Щільність та конкуренція
                "density_metrics": {
                    "poi_density": float(analytics_data.get('poi_density', 0)),
                    "retail_density": float(analytics_data.get('retail_density', 0)),
                    "competition_intensity": float(analytics_data.get('competition_intensity', 0))
                },
                
                # Влив та якість
                "influence_metrics": {
                    "positive_influence": float(analytics_data.get('total_positive_influence', 0)),
                    "negative_influence": float(analytics_data.get('total_negative_influence', 0)),
                    "net_influence_score": float(analytics_data.get('net_influence_score', 0)),
                    "avg_poi_quality": float(analytics_data.get('avg_poi_quality', 0)),
                    "branded_poi_ratio": float(analytics_data.get('branded_poi_ratio', 0))
                },
                
                # Доступність транспорту
                "accessibility": {
                    "transport_accessibility_score": float(analytics_data.get('transport_accessibility_score', 0)),
                    "nearest_metro_distance_km": analytics_data.get('nearest_metro_distance_km'),
                    "nearest_bus_stop_distance_km": analytics_data.get('nearest_bus_stop_distance_km'),
                    "public_transport_density": float(analytics_data.get('public_transport_density', 0)),
                    "primary_road_access": bool(analytics_data.get('primary_road_access', False))
                },
                
                # Дорожня мережа
                "road_network": {
                    "road_density_km_per_km2": float(analytics_data.get('road_density_km_per_km2', 0)),
                    "average_road_quality": float(analytics_data.get('average_road_quality', 0))
                },
                
                # Ринкові метрики
                "market_metrics": {
                    "market_saturation_index": float(analytics_data.get('market_saturation_index', 0)),
                    "opportunity_score": float(analytics_data.get('opportunity_score', 0)),
                    "residential_indicator": float(analytics_data.get('residential_indicator_score', 0)),
                    "commercial_activity": float(analytics_data.get('commercial_activity_score', 0))
                },
                
                # Демографія (якщо доступна)
                "demographics": {}
            }
            
            # Додаємо демографічні дані якщо є
            if demographics_data:
                analytics["demographics"] = {
                    "population": float(demographics_data.get('population', 0)),
                    "population_density": float(demographics_data.get('population_density', 0)),
                    "data_source": "h3_population"
                }
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error in H3 analytics for {h3_index}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {
                "error": str(e),
                "h3_index": h3_index
            }
    
    def get_competitive_analysis(self, h3_index: str, radius_rings: int = 2, resolution: int = 10) -> Dict[str, Any]:
        """Аналіз конкуренції в радіусі навколо гексагона з реальних даних - остаточна версія"""
        try:
            # Перевірка валідності H3
            if not h3.is_valid_cell(h3_index):
                return {"error": "Invalid H3 index", "h3_index": h3_index}
            
            # Отримуємо всі гексагони в радіусі
            area_hexagons = list(h3.grid_disk(h3_index, radius_rings))
            
            # Вибираємо відповідну колонку H3
            h3_column = f"h3_res_{resolution}" if resolution <= 10 else "h3_res_10"
            
            # Запит для пошуку конкурентів БЕЗ геометрії
            competitors_query = f"""
                SELECT 
                    entity_id,
                    osm_id,
                    name_standardized as name,
                    brand_normalized as brand,
                    functional_group,
                    primary_category,
                    secondary_category,
                    COALESCE(influence_weight, 0.0) as influence_weight,
                    COALESCE(quality_score, 0.0) as quality_score,
                    COALESCE(brand_confidence, 0.0) as brand_confidence,
                    {h3_column} as h3_location,
                    region_name
                FROM osm_ukraine.poi_processed
                WHERE {h3_column} = ANY(%s)
                    AND functional_group = 'competitor'
                    AND entity_type = 'poi'
                    AND brand_normalized IS NOT NULL
                ORDER BY influence_weight DESC NULLS LAST, quality_score DESC NULLS LAST
                LIMIT 100
            """
            
            competitors_data = self.execute_query(competitors_query, [area_hexagons])
            
            # Обробляємо конкурентів
            competitors = []
            brand_stats = {}
            
            for comp in competitors_data:
                # Розраховуємо відстань від центру
                try:
                    distance = h3.grid_distance(h3_index, comp['h3_location'])
                except Exception:
                    distance = 0
                
                # Генеруємо координати з H3
                try:
                    lat, lon = h3.cell_to_latlng(comp['h3_location'])
                except:
                    lat, lon = None, None
                
                competitor = {
                    "id": str(comp['entity_id']),
                    "name": comp['name'] or "Unknown",
                    "brand": comp['brand'] or "Unknown",
                    "type": comp['secondary_category'] or comp['primary_category'] or "retail",
                    "h3_location": comp['h3_location'],
                    "coordinates": {"lat": lat, "lon": lon} if lat and lon else {"lat": None, "lon": None},
                    "distance_rings": distance,
                    "influence_weight": float(comp['influence_weight']),
                    "quality_score": float(comp['quality_score']),
                    "brand_confidence": float(comp['brand_confidence']),
                    "competition_strength": round(
                        float(comp['influence_weight']) * float(comp['quality_score']), 2
                    )
                }
                
                competitors.append(competitor)
                
                # Статистика по брендах
                brand = comp['brand'] or "Unknown"
                if brand not in brand_stats:
                    brand_stats[brand] = {"count": 0, "total_influence": 0.0}
                brand_stats[brand]["count"] += 1
                brand_stats[brand]["total_influence"] += float(comp['influence_weight'])
            
            # Топ-3 домінуючих брендів
            dominant_brands = sorted(
                brand_stats.items(),
                key=lambda x: (x[1]["count"], x[1]["total_influence"]),
                reverse=True
            )[:3]
            
            # Розрахунки аналітики
            total_hexagons = len(area_hexagons)
            competitor_count = len(competitors)
            
            market_saturation = round((competitor_count / total_hexagons) * 100, 1) if total_hexagons > 0 else 0
            avg_competition_strength = round(
                sum(c["competition_strength"] for c in competitors) / max(competitor_count, 1), 2
            )
            
            # Визначення можливостей ринку
            if competitor_count < 3:
                market_opportunity = "high"
                risk_level = "low"
            elif competitor_count < 7:
                market_opportunity = "medium"  
                risk_level = "medium"
            else:
                market_opportunity = "low"
                risk_level = "high"
            
            # Формуємо аналіз
            analysis = {
                "center_h3": h3_index,
                "radius_rings": radius_rings,
                "resolution": resolution,
                "area_analysis": {
                    "total_hexagons_analyzed": total_hexagons,
                    "hexagons_with_competitors": len(set(c["h3_location"] for c in competitors)),
                    "coverage_percentage": round(
                        len(set(c["h3_location"] for c in competitors)) / total_hexagons * 100, 1
                    ) if total_hexagons > 0 else 0
                },
                "competitors_found": competitor_count,
                "competitors": competitors,
                "brand_analysis": {
                    "unique_brands": len(brand_stats),
                    "dominant_brands": [brand for brand, stats in dominant_brands],
                    "brand_distribution": {brand: stats["count"] for brand, stats in brand_stats.items()}
                },
                "competition_summary": {
                    "market_saturation": market_saturation,
                    "average_competition_strength": avg_competition_strength,
                    "competition_density": round(competitor_count / (radius_rings + 1) ** 2, 2),
                    "market_opportunity": market_opportunity,
                    "risk_level": risk_level
                },
                "recommendations": {
                    "market_opportunity": market_opportunity,
                    "optimal_positioning": "differentiation" if competitor_count > 5 else "market_leader",
                    "risk_factors": self._generate_risk_factors(competitor_count, market_saturation),
                    "success_factors": self._generate_success_factors(competitors, brand_stats)
                }
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error in competitive analysis for {h3_index}: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return {
                "error": str(e),
                "center_h3": h3_index,
                "competitors": []
            }
    
    def check_h3_exists(self, h3_index: str) -> bool:
        """Перевірка чи існує H3 індекс в базі даних"""
        try:
            # Спочатку перевіряємо валідність H3 індексу
            if not h3.is_valid_cell(h3_index):
                return False
            
            # Перевіряємо чи є дані в h3_analytics_current
            query = """
                SELECT EXISTS(
                    SELECT 1 FROM osm_ukraine.h3_analytics_current 
                    WHERE h3_index = %s
                ) as exists
            """
            
            result = self.execute_single(query, [h3_index])
            return result['exists'] if result else False
            
        except Exception as e:
            logger.error(f"Error checking H3 existence for {h3_index}: {e}")
            return False
    
    # ==============================================
    # ДОПОМІЖНІ МЕТОДИ
    # ==============================================
    
    def _calculate_distance_from_center(self, h3_index: str, lat: float, lon: float) -> float:
        """Розрахунок відстані від центру гексагона"""
        try:
            center_lat, center_lon = h3.cell_to_latlng(h3_index)
            
            from math import radians, cos, sin, asin, sqrt
            
            def haversine(lon1, lat1, lon2, lat2):
                lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * asin(sqrt(a))
                r = 6371000  # Радіус Землі в метрах
                return c * r
            
            distance = haversine(center_lon, center_lat, lon, lat)
            return round(distance, 0)
            
        except Exception:
            return 0.0
    
    def _generate_risk_factors(self, competitor_count: int, market_saturation: float) -> List[str]:
        """Генерація факторів ризику"""
        risks = []
        
        if competitor_count > 10:
            risks.append("High competitor density")
        if market_saturation > 50:
            risks.append("Market saturation")
        if competitor_count > 5:
            risks.append("Intense competition")
        if not risks:
            risks.append("Low competition risk")
            
        return risks
    
    def _generate_success_factors(self, competitors: List[Dict], brand_stats: Dict) -> List[str]:
        """Генерація факторів успіху"""
        factors = []
        
        if len(competitors) < 3:
            factors.append("Low competition advantage")
        if len(brand_stats) < 5:
            factors.append("Limited brand diversity")
        
        high_quality_competitors = [c for c in competitors if c["quality_score"] > 0.8]
        if len(high_quality_competitors) < 2:
            factors.append("Quality differentiation opportunity")
            
        if not factors:
            factors.append("Standard market conditions")
            
        return factors

# Глобальний екземпляр сервісу
db_service = DatabaseService()

# Функції для зручного використання
def get_database_service() -> DatabaseService:
    """Отримання екземпляру database service"""
    return db_service

def test_database_connection() -> Dict[str, Any]:
    """Швидкий тест підключення до БД"""
    return db_service.test_connection()