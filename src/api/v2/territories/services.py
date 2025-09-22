"""
services module for src\api\v2\territories
"""

# TODO: Implement services
"""
Territories Service Layer
Business logic for territories domain
"""

import json
import asyncio
from typing import List, Optional, Dict, Any
from decimal import Decimal
from datetime import datetime

import asyncpg
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from clickhouse_driver import Client as ClickHouseClient
import redis.asyncio as redis

from api.v2.territories.schemas import (
    AdminUnitResponse,
    AdminMetricsResponse,
    H3HexagonResponse,
    BivariateConfig,
    HexagonDetailsResponse,
    GeoJSONGeometry
)


class TerritoriesService:
    """Сервіс для роботи з територіями"""
    
    def __init__(self):
        # Ініціалізуємо підключення (буде замінено на DI)
        self.redis_client = None
        self.clickhouse_client = None
        self._init_bivariate_config()
    
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
    
    async def get_admin_geometries(
        self,
        db: AsyncSession,
        level: str = "all",
        simplified: bool = True
    ) -> List[AdminUnitResponse]:
        """
        Отримати геометрії адмінодиниць з PostGIS
        
        Args:
            db: Сесія бази даних
            level: Рівень адмінодиниць (oblast/raion/gromada/all)
            simplified: Використовувати спрощені геометрії
        
        Returns:
            Список адмінодиниць з геометріями
        """
        
        # SQL запит для отримання геометрій
        simplify_func = "ST_Simplify(geometry, 0.001)" if simplified else "geometry"
        
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
        
        result = await db.execute(text(query), params)
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
                center_lat=row.center_lat,
                center_lon=row.center_lon,
                area_km2=float(row.area_km2),
                population=row.population,
                settlement_count=row.settlement_count
            ))
        
        return admin_units
    
    async def get_admin_metrics(
        self,
        metric_x: str = "population",
        metric_y: str = "income_index"
    ) -> List[AdminMetricsResponse]:
        """
        Отримати метрики з ClickHouse
        
        Args:
            metric_x: Метрика для осі X
            metric_y: Метрика для осі Y
        
        Returns:
            Список метрик з bivariate codes
        """
        
        # Підключення до ClickHouse
        ch_client = ClickHouseClient(
            host='localhost',
            port=32769,
            user='webuser',
            password='password123',
            database='geo_analytics'
        )
        
        # Запит до ClickHouse для метрик
        query = """
            SELECT
                koatuu_code AS id,
                name_uk,
                admin_level AS level,
                population,
                population_density,
                income_index,
                retail_density,
                competitor_count,
                traffic_index,
                accessibility_score,
                growth_potential,
                poi_total,
                road_density_km,
                public_transport_stops,
                average_speed_kmh,
                updated_at AS last_updated,
                data_quality_score,
                -- Обчислення bivariate bins
                CASE
                    WHEN {metric_x} <= quantile(0.33)({metric_x}) OVER () THEN 1
                    WHEN {metric_x} <= quantile(0.67)({metric_x}) OVER () THEN 2
                    ELSE 3
                END AS bivar_x_bin,
                CASE
                    WHEN {metric_y} <= quantile(0.33)({metric_y}) OVER () THEN 1
                    WHEN {metric_y} <= quantile(0.67)({metric_y}) OVER () THEN 2
                    ELSE 3
                END AS bivar_y_bin
            FROM admin_analytics
            WHERE 1=1
            ORDER BY admin_level, name_uk
        """.format(metric_x=metric_x, metric_y=metric_y)
        
        rows = ch_client.execute(query)
        
        # Конвертуємо в Pydantic моделі
        metrics_list = []
        for row in rows:
            bivar_code = f"{row[15]}{row[16]}"  # Об'єднуємо bins в код
            
            metrics_list.append(AdminMetricsResponse(
                id=row[0],
                name_uk=row[1],
                level=row[2],
                population=row[3],
                population_density=row[4],
                income_index=row[5],
                retail_density=row[6],
                competitor_count=row[7],
                traffic_index=row[8],
                accessibility_score=row[9],
                growth_potential=row[10],
                bivar_code=bivar_code,
                bivar_x_bin=row[15],
                bivar_y_bin=row[16],
                poi_total=row[11],
                road_density_km=row[12],
                public_transport_stops=row[13],
                average_speed_kmh=row[14],
                last_updated=row[14],
                data_quality_score=row[15] if len(row) > 15 else 1.0
            ))
        
        return metrics_list
    
    async def get_h3_hexagons(
        self,
        db: AsyncSession,
        admin_id: str,
        resolution: int = 7,
        include_metrics: bool = True
    ) -> List[H3HexagonResponse]:
        """
        Отримати H3 гексагони для адмінодиниці
        
        Args:
            db: Сесія бази даних
            admin_id: KOATUU код адмінодиниці
            resolution: H3 резолюція (7-10)
            include_metrics: Включити метрики
        
        Returns:
            Список H3 гексагонів (без геометрій!)
        """
        
        # SQL запит до таблиці h3_grid
        query = f"""
            SELECT 
                h3_res_{resolution} AS h3_index,
                {resolution} AS resolution,
                oblast_koatuu,
                raion_koatuu,
                gromada_koatuu,
                population,
                income_level,
                retail_count,
                competitor_intensity,
                traffic_flow,
                revenue_potential,
                risk_score,
                recommendation_score
            FROM h3_data.h3_grid
            WHERE 1=1
                AND (
                    oblast_koatuu = :admin_id OR
                    raion_koatuu = :admin_id OR
                    gromada_koatuu = :admin_id
                )
                AND h3_res_{resolution} IS NOT NULL
            {"" if include_metrics else "LIMIT 10000"}
        """
        
        result = await db.execute(
            text(query),
            {"admin_id": admin_id}
        )
        rows = result.fetchall()
        
        # Обчислюємо bivariate codes
        hexagons = []
        for row in rows:
            # Простий алгоритм для bivariate bins
            pop_bin = 1 if row.population < 1000 else (2 if row.population < 5000 else 3)
            income_bin = 1 if row.income_level < 30 else (2 if row.income_level < 70 else 3)
            bivar_code = f"{pop_bin}{income_bin}"
            
            hexagons.append(H3HexagonResponse(
                h3_index=row.h3_index,
                resolution=resolution,
                oblast_id=row.oblast_koatuu,
                raion_id=row.raion_koatuu,
                gromada_id=row.gromada_koatuu,
                population=row.population,
                income_level=row.income_level,
                retail_count=row.retail_count,
                competitor_intensity=row.competitor_intensity,
                traffic_flow=row.traffic_flow,
                bivar_code=bivar_code,
                revenue_potential=row.revenue_potential,
                risk_score=row.risk_score,
                recommendation_score=row.recommendation_score
            ))
        
        return hexagons
    
    async def get_hexagon_details(
        self,
        db: AsyncSession,
        h3_indexes: List[str]
    ) -> Dict[str, HexagonDetailsResponse]:
        """
        Отримати детальну інформацію про гексагони
        
        Args:
            db: Сесія бази даних
            h3_indexes: Список H3 індексів
        
        Returns:
            Словник з детальною інформацією
        """
        
        # Підготовка для batch запиту
        indexes_str = ",".join([f"'{idx}'" for idx in h3_indexes])
        
        query = f"""
            WITH hexagon_data AS (
                SELECT 
                    COALESCE(h3_res_10, h3_res_9, h3_res_8, h3_res_7) AS h3_index,
                    ST_Y(ST_Centroid(h3_to_geometry(h3_index))) AS center_lat,
                    ST_X(ST_Centroid(h3_to_geometry(h3_index))) AS center_lon,
                    population,
                    income_level,
                    retail_count,
                    competitor_intensity,
                    traffic_flow,
                    walkability_score,
                    public_transport_stops,
                    revenue_potential,
                    risk_score
                FROM h3_data.h3_grid
                WHERE COALESCE(h3_res_10, h3_res_9, h3_res_8, h3_res_7) 
                    IN ({indexes_str})
            ),
            percentiles AS (
                SELECT 
                    h3_index,
                    PERCENT_RANK() OVER (ORDER BY population) * 100 AS pop_percentile,
                    PERCENT_RANK() OVER (ORDER BY income_level) * 100 AS income_percentile,
                    PERCENT_RANK() OVER (ORDER BY traffic_flow) * 100 AS traffic_percentile
                FROM hexagon_data
            )
            SELECT 
                h.*, 
                p.pop_percentile,
                p.income_percentile,
                p.traffic_percentile
            FROM hexagon_data h
            JOIN percentiles p ON h.h3_index = p.h3_index
        """
        
        result = await db.execute(text(query))
        rows = result.fetchall()
        
        # Формуємо детальні відповіді
        details = {}
        for row in rows:
            details[row.h3_index] = HexagonDetailsResponse(
                h3_index=row.h3_index,
                resolution=len(row.h3_index) - 1,  # Приблизно
                center_lat=row.center_lat,
                center_lon=row.center_lon,
                metrics={
                    "population": {
                        "value": row.population,
                        "percentile": row.pop_percentile
                    },
                    "income_level": {
                        "value": row.income_level,
                        "percentile": row.income_percentile
                    },
                    "traffic_flow": {
                        "value": row.traffic_flow,
                        "percentile": row.traffic_percentile
                    }
                },
                poi_summary={
                    "retail": row.retail_count,
                    "transport": row.public_transport_stops
                },
                transport_access={
                    "public_transport_stops": row.public_transport_stops,
                    "walkability_score": row.walkability_score
                },
                insights={
                    "revenue_potential": row.revenue_potential,
                    "risk_score": row.risk_score
                },
                recommendations=self._generate_recommendations(row)
            )
        
        return details
    
    def _generate_recommendations(self, hexagon_data) -> List[str]:
        """Генерація рекомендацій на основі метрик"""
        recommendations = []
        
        if hexagon_data.revenue_potential > 0.7:
            recommendations.append("Високий потенціал доходу - рекомендована локація")
        
        if hexagon_data.competitor_intensity > 0.8:
            recommendations.append("Висока конкуренція - потрібна унікальна пропозиція")
        
        if hexagon_data.walkability_score < 0.3:
            recommendations.append("Низька пішохідна доступність - забезпечте парковку")
        
        return recommendations
    
    def get_bivariate_config(self) -> BivariateConfig:
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
                "x": "population",
                "y": "income_index"
            }
        )