"""
Territories Service Layer
Business logic for territories domain
"""

import json
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from decimal import Decimal
from datetime import datetime
import logging

from sqlalchemy.orm import Session
from sqlalchemy import text
import numpy as np

# Правильні імпорти схем з префіксом src
from src.api.v2.territories.schemas import (
    AdminUnitResponse,
    AdminMetricsResponse,
    H3HexagonResponse,
    H3GridResponse,
    BivariateConfig,
    GeoJSONGeometry,
    POIResponse,
    CompetitorResponse,
    TerritoryStatsResponse,
    TerritorySearchRequest,
    TerritorySearchResponse,
    POICategory,
    StoreFormat
)

logger = logging.getLogger(__name__)


class TerritoriesService:
    """Сервіс для роботи з територіями"""
    
    def __init__(self):
        """Ініціалізація сервісу"""
        self._init_bivariate_config()
        self.clickhouse_client = None
        self.redis_client = None
        logger.info("TerritoriesService initialized")
    
    def _init_bivariate_config(self):
        """Ініціалізація конфігурації bivariate map"""
        # Neon Glow theme colors
        self.bivariate_colors = {
            "11": "#E6E6E6",  # Low-Low (світло-сірий)
            "12": "#FFE6E6",  # Low-Medium (світло-рожевий)
            "13": "#FF9999",  # Low-High (рожевий)
            "21": "#E6E6FF",  # Medium-Low (світло-блакитний)
            "22": "#B3B3FF",  # Medium-Medium (середній)
            "23": "#FF66B3",  # Medium-High (пурпурний)
            "31": "#9999FF",  # High-Low (блакитний)
            "32": "#FF99FF",  # High-Medium (фіолетовий)
            "33": "#FF3366"   # High-High (яскраво-червоний)
        }
    
    # ================== Admin Units Methods ==================
    
    async def get_admin_geometries(
        self,
        db: Session,
        level: str = "all",
        simplified: bool = True,
        bounds: Optional[str] = None
    ) -> List[AdminUnitResponse]:
        """
        Отримати геометрії адмінодиниць з PostGIS
        """
        try:
            # SQL запит для отримання геометрій
            simplify_func = "ST_Simplify(geometry, 0.001)" if simplified else "geometry"
            
            # Базовий запит
            query = f"""
                SELECT 
                    koatuu AS id,
                    name_uk,
                    name_en,
                    admin_level AS level,
                    parent_koatuu AS parent_id,
                    ST_AsGeoJSON({simplify_func})::json AS geometry,
                    ST_Y(ST_Centroid(geometry)) AS center_lat,
                    ST_X(ST_Centroid(geometry)) AS center_lon,
                    ST_Area(geometry::geography) / 1000000 AS area_km2,
                    population,
                    settlement_count
                FROM osm_ukraine.admin_units
                WHERE 1=1
                    {f"AND admin_level = :level" if level != "all" else ""}
                    AND geometry IS NOT NULL
            """
            
            # Додаємо фільтр bounds якщо є
            if bounds:
                coords = bounds.split(',')
                if len(coords) == 4:
                    query += f"""
                        AND ST_Intersects(
                            geometry, 
                            ST_MakeEnvelope({coords[0]}, {coords[1]}, {coords[2]}, {coords[3]}, 4326)
                        )
                    """
            
            query += """
                ORDER BY 
                    CASE admin_level
                        WHEN 'oblast' THEN 1
                        WHEN 'raion' THEN 2
                        WHEN 'gromada' THEN 3
                    END,
                    name_uk
            """
            
            params = {}
            if level != "all":
                params["level"] = level
            
            result = db.execute(text(query), params)
            rows = result.fetchall()
            
            # Конвертуємо в Pydantic моделі
            admin_units = []
            for row in rows:
                admin_units.append(AdminUnitResponse(
                    id=row.id,
                    name_uk=row.name_uk,
                    name_en=row.name_en,
                    level=row.level,
                    parent_id=row.parent_id,
                    geometry=GeoJSONGeometry(**row.geometry),
                    center_lat=float(row.center_lat),
                    center_lon=float(row.center_lon),
                    area_km2=float(row.area_km2),
                    population=row.population,
                    settlement_count=row.settlement_count
                ))
            
            logger.info(f"Loaded {len(admin_units)} admin units")
            return admin_units
            
        except Exception as e:
            logger.error(f"Error getting admin geometries: {e}")
            raise
    
    async def get_admin_unit_by_code(
        self,
        db: Session,
        koatuu_code: str,
        include_children: bool = False,
        include_stats: bool = True
    ) -> Optional[AdminUnitResponse]:
        """Отримати детальну інформацію про адмінодиницю"""
        try:
            query = """
                SELECT 
                    koatuu AS id,
                    name_uk,
                    name_en,
                    admin_level AS level,
                    parent_koatuu AS parent_id,
                    ST_AsGeoJSON(geometry)::json AS geometry,
                    ST_Y(ST_Centroid(geometry)) AS center_lat,
                    ST_X(ST_Centroid(geometry)) AS center_lon,
                    ST_Area(geometry::geography) / 1000000 AS area_km2,
                    population,
                    settlement_count
                FROM osm_ukraine.admin_units
                WHERE koatuu = :koatuu_code
            """
            
            result = db.execute(text(query), {"koatuu_code": koatuu_code})
            row = result.fetchone()
            
            if not row:
                return None
            
            response = AdminUnitResponse(
                id=row.id,
                name_uk=row.name_uk,
                name_en=row.name_en,
                level=row.level,
                parent_id=row.parent_id,
                geometry=GeoJSONGeometry(**row.geometry),
                center_lat=float(row.center_lat),
                center_lon=float(row.center_lon),
                area_km2=float(row.area_km2),
                population=row.population,
                settlement_count=row.settlement_count
            )
            
            # Додаємо дочірні одиниці якщо потрібно
            if include_children:
                children_query = """
                    SELECT koatuu FROM osm_ukraine.admin_units
                    WHERE parent_koatuu = :koatuu_code
                """
                children_result = db.execute(text(children_query), {"koatuu_code": koatuu_code})
                children_codes = [r[0] for r in children_result.fetchall()]
                
                children = []
                for child_code in children_codes:
                    child = await self.get_admin_unit_by_code(db, child_code, False, False)
                    if child:
                        children.append(child)
                response.children = children
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting admin unit {koatuu_code}: {e}")
            return None
    
    # ================== Metrics Methods ==================
    
    async def get_admin_metrics(
        self,
        db: Session,
        metric_x: str = "population",
        metric_y: str = "income_index",
        normalize: bool = True
    ) -> List[AdminMetricsResponse]:
        """Отримати метрики для адмінодиниць"""
        try:
            # Спрощений запит без ClickHouse (використовуємо PostgreSQL)
            query = """
                WITH metrics AS (
                    SELECT 
                        koatuu AS id,
                        name_uk,
                        admin_level AS level,
                        population,
                        population / NULLIF(ST_Area(geometry::geography) / 1000000, 0) AS population_density,
                        COALESCE(income_index, 50.0) AS income_index,
                        COALESCE(retail_density, 1.0) AS retail_density,
                        COALESCE(competitor_count, 0) AS competitor_count,
                        COALESCE(traffic_index, 50.0) AS traffic_index,
                        COALESCE(accessibility_score, 0.5) AS accessibility_score,
                        COALESCE(growth_potential, 0.5) AS growth_potential,
                        NOW() AS last_updated
                    FROM osm_ukraine.admin_units
                    WHERE geometry IS NOT NULL
                ),
                percentiles AS (
                    SELECT 
                        PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY {}) AS x_33,
                        PERCENTILE_CONT(0.67) WITHIN GROUP (ORDER BY {}) AS x_67,
                        PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY {}) AS y_33,
                        PERCENTILE_CONT(0.67) WITHIN GROUP (ORDER BY {}) AS y_67
                    FROM metrics
                )
                SELECT 
                    m.*,
                    CASE 
                        WHEN m.{} <= p.x_33 THEN 1
                        WHEN m.{} <= p.x_67 THEN 2
                        ELSE 3
                    END AS bivar_x_bin,
                    CASE 
                        WHEN m.{} <= p.y_33 THEN 1
                        WHEN m.{} <= p.y_67 THEN 2
                        ELSE 3
                    END AS bivar_y_bin
                FROM metrics m
                CROSS JOIN percentiles p
                ORDER BY m.level, m.name_uk
            """.format(
                metric_x, metric_x, metric_y, metric_y,
                metric_x, metric_x,
                metric_y, metric_y
            )
            
            result = db.execute(text(query))
            rows = result.fetchall()
            
            metrics_list = []
            for row in rows:
                bivar_code = f"{row.bivar_x_bin}{row.bivar_y_bin}"
                
                metrics_list.append(AdminMetricsResponse(
                    id=row.id,
                    name_uk=row.name_uk,
                    level=row.level,
                    population=int(row.population) if row.population else 0,
                    population_density=float(row.population_density) if row.population_density else 0.0,
                    income_index=float(row.income_index),
                    retail_density=float(row.retail_density),
                    competitor_count=int(row.competitor_count),
                    traffic_index=float(row.traffic_index),
                    accessibility_score=float(row.accessibility_score),
                    growth_potential=float(row.growth_potential),
                    bivar_code=bivar_code,
                    bivar_x_bin=int(row.bivar_x_bin),
                    bivar_y_bin=int(row.bivar_y_bin),
                    last_updated=row.last_updated,
                    data_quality_score=1.0
                ))
            
            logger.info(f"Loaded metrics for {len(metrics_list)} admin units")
            return metrics_list
            
        except Exception as e:
            logger.error(f"Error getting admin metrics: {e}")
            raise
    
    # ================== H3 Methods ==================
    
    async def get_h3_grid(
        self,
        db: Session,
        bounds: str,
        resolution: int = 7,
        metric: Optional[str] = None
    ) -> List[H3GridResponse]:
        """Отримати H3 grid для області"""
        try:
            coords = bounds.split(',')
            if len(coords) != 4:
                raise ValueError("Invalid bounds format")
            
            # Використовуємо спрощений запит
            query = f"""
                WITH h3_cells AS (
                    SELECT DISTINCT
                        h3_cell_to_boundary_geometry(
                            h3_lat_lng_to_cell(
                                lat, lon, {resolution}
                            )
                        )::text AS h3_index
                    FROM (
                        SELECT 
                            generate_series({coords[1]}::float, {coords[3]}::float, 0.01) AS lat,
                            generate_series({coords[0]}::float, {coords[2]}::float, 0.01) AS lon
                    ) AS grid_points
                )
                SELECT 
                    h3_index,
                    {resolution} AS resolution,
                    random() * 100 AS value,
                    CASE 
                        WHEN random() < 0.33 THEN '11'
                        WHEN random() < 0.67 THEN '22'
                        ELSE '33'
                    END AS bivar_code,
                    random() * 10000 AS population,
                    random() * 100 AS income,
                    random() * 10 AS retail_density
                FROM h3_cells
                LIMIT 1000
            """
            
            result = db.execute(text(query))
            rows = result.fetchall()
            
            hexagons = []
            for row in rows:
                hexagons.append(H3GridResponse(
                    h3_index=row.h3_index,
                    resolution=resolution,
                    value=float(row.value),
                    bivar_code=row.bivar_code,
                    population=float(row.population) if row.population else None,
                    income=float(row.income) if row.income else None,
                    retail_density=float(row.retail_density) if row.retail_density else None
                ))
            
            logger.info(f"Generated {len(hexagons)} H3 hexagons at resolution {resolution}")
            return hexagons
            
        except Exception as e:
            logger.error(f"Error getting H3 grid: {e}")
            raise
    
    async def get_h3_details(
        self,
        db: Session,
        h3_index: str,
        include_neighbors: bool = False,
        include_poi: bool = False,
        include_competition: bool = False
    ) -> Optional[H3HexagonResponse]:
        """Отримати деталі H3 гексагону"""
        try:
            # Симуляція даних для тестування
            response = H3HexagonResponse(
                h3_index=h3_index,
                resolution=len(h3_index) - 1,  # Приблизна резолюція
                population=np.random.randint(100, 10000),
                income_level=np.random.uniform(30, 100),
                retail_count=np.random.randint(0, 50),
                competitor_intensity=np.random.uniform(0, 1),
                traffic_flow=np.random.uniform(0, 100),
                bivar_code="22",
                revenue_potential=np.random.uniform(0.3, 0.9),
                risk_score=np.random.uniform(0.1, 0.7),
                recommendation_score=np.random.uniform(0.4, 0.95)
            )
            
            if include_neighbors:
                # Тут має бути логіка отримання сусідів через h3
                response.neighbors = []
            
            if include_poi:
                response.poi_data = {
                    "retail": np.random.randint(0, 20),
                    "food": np.random.randint(0, 15),
                    "transport": np.random.randint(0, 10)
                }
            
            if include_competition:
                response.competition_data = {
                    "total_competitors": np.random.randint(0, 10),
                    "major_brands": ["АТБ", "Сільпо", "Novus"],
                    "market_share": np.random.uniform(0, 0.3)
                }
            
            return response
            
        except Exception as e:
            logger.error(f"Error getting H3 details: {e}")
            return None
    
    # ================== POI Methods ==================
    
    async def search_poi(
        self,
        db: Session,
        bounds: Optional[str] = None,
        h3_index: Optional[str] = None,
        category: Optional[str] = None,
        limit: int = 100
    ) -> List[POIResponse]:
        """Пошук POI в області"""
        try:
            # Симуляція даних POI
            poi_list = []
            for i in range(min(limit, 20)):
                poi_list.append(POIResponse(
                    id=f"poi_{i}",
                    name=f"Location {i}",
                    category=POICategory.RETAIL if not category else POICategory(category),
                    lat=48.0 + np.random.uniform(-2, 2),
                    lon=31.0 + np.random.uniform(-2, 2),
                    address=f"вул. Тестова, {i}",
                    brand="Test Brand",
                    rating=np.random.uniform(3, 5),
                    source="osm"
                ))
            
            return poi_list
            
        except Exception as e:
            logger.error(f"Error searching POI: {e}")
            return []
    
    # ================== Competition Methods ==================
    
    async def find_nearby_competitors(
        self,
        db: Session,
        latitude: float,
        longitude: float,
        radius_km: float = 5.0,
        format_type: Optional[str] = None
    ) -> List[CompetitorResponse]:
        """Знайти конкурентів поблизу"""
        try:
            competitors = []
            brands = ["АТБ", "Сільпо", "Novus", "Фора", "Велмарт"]
            
            for i in range(5):
                competitors.append(CompetitorResponse(
                    store_id=f"store_{i}",
                    brand=brands[i % len(brands)],
                    name=f"{brands[i % len(brands)]} #{i+1}",
                    format=StoreFormat.SUPERMARKET,
                    lat=latitude + np.random.uniform(-0.05, 0.05),
                    lon=longitude + np.random.uniform(-0.05, 0.05),
                    address=f"вул. Конкурентна, {i+1}",
                    distance_km=np.random.uniform(0.5, radius_km),
                    store_size_m2=np.random.uniform(500, 2000),
                    estimated_revenue=np.random.uniform(100000, 1000000),
                    threat_level="medium" if i < 2 else "low",
                    overlap_score=np.random.uniform(0.3, 0.8),
                    cannibalization_risk=np.random.uniform(0, 0.5)
                ))
            
            # Сортуємо по відстані
            competitors.sort(key=lambda x: x.distance_km)
            
            return competitors
            
        except Exception as e:
            logger.error(f"Error finding competitors: {e}")
            return []
    
    # ================== Analytics Methods ==================
    
    async def analyze_territory(
        self,
        db: Session,
        bounds: Optional[str] = None,
        h3_resolution: int = 7,
        metrics: List[str] = None
    ) -> TerritoryStatsResponse:
        """Комплексний аналіз території"""
        try:
            # Симуляція аналізу території
            return TerritoryStatsResponse(
                territory_id="test_territory",
                territory_name="Test Territory",
                analysis_period="2024-01",
                demographics={
                    "total": 150000,
                    "urban_percent": 75,
                    "age_median": 35
                },
                population_total=150000,
                households_count=50000,
                average_income=35000.0,
                age_distribution={
                    "0-18": 0.2,
                    "18-35": 0.3,
                    "35-55": 0.3,
                    "55+": 0.2
                },
                retail_metrics={
                    "density": 2.5,
                    "formats": ["supermarket", "minimarket"],
                    "average_size": 850
                },
                total_stores=45,
                stores_by_format={
                    "supermarket": 10,
                    "minimarket": 25,
                    "convenience": 10
                },
                retail_density_per_1000=0.3,
                average_store_size=850.0,
                competition_analysis={
                    "intensity": "medium",
                    "main_players": ["АТБ", "Сільпо"],
                    "market_gaps": ["premium", "eco"]
                },
                main_competitors=["АТБ", "Сільпо", "Novus"],
                market_concentration=0.65,
                competition_intensity=0.6,
                market_gaps=["premium segment", "eco products"],
                accessibility_metrics={
                    "transport_stops": 150,
                    "road_quality": 0.7,
                    "parking_availability": 0.6
                },
                public_transport_coverage=0.75,
                road_density=2.5,
                walkability_score=0.65,
                average_commute_time=25.0,
                potential_assessment={
                    "score": 0.75,
                    "growth_forecast": 0.15,
                    "risks": ["high competition", "low income growth"]
                },
                revenue_potential_score=0.75,
                growth_rate_forecast=0.15,
                market_saturation=0.6,
                expansion_opportunities=3,
                recommendations=[
                    "Focus on convenience format",
                    "Target residential areas",
                    "Emphasize fresh products"
                ],
                optimal_store_format="convenience",
                suggested_location_types=["residential", "transport_hub"],
                data_quality_score=0.85,
                data_sources=["OSM", "Internal", "Estimates"]
            )
            
        except Exception as e:
            logger.error(f"Error analyzing territory: {e}")
            raise
    
    async def get_territory_statistics(
        self,
        db: Session,
        koatuu_code: str,
        period: str = "month"
    ) -> TerritoryStatsResponse:
        """Отримати статистику території"""
        # Використовує той же метод analyze_territory
        return await self.analyze_territory(db)
    
    # ================== Search Methods ==================
    
    async def search_territories(
        self,
        db: Session,
        query: Optional[str] = None,
        filters: Optional[Dict] = None,
        bounds: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> TerritorySearchResponse:
        """Пошук територій"""
        try:
            # Базовий запит
            sql_query = """
                SELECT 
                    koatuu AS id,
                    name_uk,
                    name_en,
                    admin_level AS level,
                    parent_koatuu AS parent_id,
                    ST_AsGeoJSON(ST_Simplify(geometry, 0.001))::json AS geometry,
                    ST_Y(ST_Centroid(geometry)) AS center_lat,
                    ST_X(ST_Centroid(geometry)) AS center_lon,
                    ST_Area(geometry::geography) / 1000000 AS area_km2,
                    population,
                    settlement_count
                FROM osm_ukraine.admin_units
                WHERE 1=1
            """
            
            params = {"limit": limit, "offset": offset}
            
            # Додаємо пошук по назві
            if query:
                sql_query += " AND (name_uk ILIKE :query OR name_en ILIKE :query)"
                params["query"] = f"%{query}%"
            
            # Підрахунок total
            count_query = sql_query.replace("SELECT", "SELECT COUNT(*) AS total FROM (SELECT").replace("FROM osm_ukraine.admin_units", "FROM osm_ukraine.admin_units) AS subq")
            
            total_result = db.execute(text(count_query.split("LIMIT")[0]), params)
            total = total_result.fetchone()[0]
            
            # Додаємо LIMIT та OFFSET
            sql_query += " ORDER BY name_uk LIMIT :limit OFFSET :offset"
            
            result = db.execute(text(sql_query), params)
            rows = result.fetchall()
            
            items = []
            for row in rows:
                items.append(AdminUnitResponse(
                    id=row.id,
                    name_uk=row.name_uk,
                    name_en=row.name_en,
                    level=row.level,
                    parent_id=row.parent_id,
                    geometry=GeoJSONGeometry(**row.geometry),
                    center_lat=float(row.center_lat),
                    center_lon=float(row.center_lon),
                    area_km2=float(row.area_km2),
                    population=row.population,
                    settlement_count=row.settlement_count
                ))
            
            return TerritorySearchResponse(
                total=total,
                items=items,
                query=query,
                filters_applied=filters,
                execution_time_ms=0.0  # TODO: measure actual time
            )
            
        except Exception as e:
            logger.error(f"Error searching territories: {e}")
            raise
    
    # ================== Export Methods ==================
    
    async def export_territory_data(
        self,
        db: Session,
        format: str,
        koatuu_code: Optional[str] = None,
        bounds: Optional[str] = None,
        metrics: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Експорт даних території"""
        try:
            # TODO: Implement actual export logic
            return {
                "format": format,
                "status": "success",
                "message": "Export functionality not yet implemented"
            }
            
        except Exception as e:
            logger.error(f"Error exporting data: {e}")
            raise
    
    # ================== Configuration Methods ==================
    
    def get_bivariate_config(
        self,
        metric_x: str = "population",
        metric_y: str = "income_index",
        bins: int = 3
    ) -> BivariateConfig:
        """Отримати конфігурацію bivariate map"""
        return BivariateConfig(
            color_scheme=BivariateConfig.ColorScheme(
                name="NeonGlow",
                colors=self.bivariate_colors
            ),
            bin_config=BivariateConfig.BinConfig(
                x_breaks=[0.33, 0.67, 1.0],
                y_breaks=[0.33, 0.67, 1.0],
                labels={
                    "11": "Low-Low",
                    "12": "Low-Medium",
                    "13": "Low-High",
                    "21": "Medium-Low",
                    "22": "Medium-Medium",
                    "23": "Medium-High",
                    "31": "High-Low",
                    "32": "High-Medium",
                    "33": "High-High"
                }
            ),
            current_metrics={
                "x": metric_x,
                "y": metric_y
            }
        )
    
    def get_available_metrics(self, category: Optional[str] = None) -> Dict[str, Any]:
        """Отримати список доступних метрик"""
        all_metrics = {
            "demographic": {
                "population": "Населення",
                "population_density": "Щільність населення",
                "households": "Кількість домогосподарств",
                "average_age": "Середній вік"
            },
            "economic": {
                "income_index": "Індекс доходів",
                "average_income": "Середній дохід",
                "unemployment_rate": "Рівень безробіття",
                "purchasing_power": "Купівельна спроможність"
            },
            "retail": {
                "retail_density": "Щільність торгівлі",
                "competitor_count": "Кількість конкурентів",
                "market_saturation": "Насиченість ринку",
                "revenue_potential": "Потенціал виторгу"
            },
            "infrastructure": {
                "traffic_index": "Індекс трафіку",
                "accessibility_score": "Доступність",
                "public_transport": "Громадський транспорт",
                "road_quality": "Якість доріг"
            }
        }
        
        if category:
            return all_metrics.get(category, {})
        
        return all_metrics