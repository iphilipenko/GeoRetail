# src/api/endpoints/h3_visualization.py
"""
H3 Visualization API для Київська область MVP
Повний, завершений файл для FastAPI backend
"""

from fastapi import APIRouter, Query, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal
import psycopg2
import psycopg2.extras
import json
from datetime import datetime
import logging
from contextlib import contextmanager

# H3 library для генерації геометрій - УНІВЕРСАЛЬНИЙ IMPORT
try:
    import h3
    H3_AVAILABLE = True
    
    # Визначаємо версію H3 та доступні функції
    h3_version = getattr(h3, '__version__', 'unknown')
    h3_functions = [f for f in dir(h3) if not f.startswith('_')]
    
    print(f"✅ H3 imported successfully. Version: {h3_version}")
    print(f"✅ Available functions: {len(h3_functions)} total")
    
    # Визначаємо які функції доступні (v3.x vs v4.x compatibility)
    HAS_CELL_TO_LATLNG = hasattr(h3, 'cell_to_latlng')
    HAS_CELL_TO_BOUNDARY = hasattr(h3, 'cell_to_boundary') 
    HAS_H3_TO_GEO = hasattr(h3, 'h3_to_geo')
    HAS_H3_TO_GEO_BOUNDARY = hasattr(h3, 'h3_to_geo_boundary')
    
    print(f"✅ Functions check: cell_to_latlng={HAS_CELL_TO_LATLNG}, h3_to_geo={HAS_H3_TO_GEO}")
    
except ImportError as e:
    h3 = None
    H3_AVAILABLE = False
    HAS_CELL_TO_LATLNG = False
    HAS_CELL_TO_BOUNDARY = False
    HAS_H3_TO_GEO = False
    HAS_H3_TO_GEO_BOUNDARY = False
    print(f"⚠️ H3 library not available: {e}")

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/visualization", tags=["H3 Visualization"])

# =================================================================
# МОДЕЛІ ДАНИХ
# =================================================================

class H3MetricType(str):
    COMPETITION = "competition"
    OPPORTUNITY = "opportunity"

class H3HexagonData(BaseModel):
    h3_index: str
    resolution: int
    
    # Core metrics
    competition_intensity: float = Field(ge=0, le=1, description="Competition intensity (0-1)")
    transport_accessibility_score: float = Field(ge=0, le=1, description="Transport score (0-1)")
    residential_indicator_score: float = Field(ge=0, le=1, description="Residential score (0-1)")
    commercial_activity_score: float = Field(ge=0, le=1, description="Commercial score (0-1)")
    
    # Calculated metric
    market_opportunity_score: float = Field(ge=0, le=1, description="Calculated opportunity (0-1)")
    
    # POI counts
    poi_total_count: int
    retail_count: int
    competitor_count: int
    
    # Geometry
    geometry: Dict[str, Any] = Field(description="GeoJSON geometry")
    
    # Display category based on selected metric
    display_category: str = Field(description="Color category for visualization")
    display_value: float = Field(description="Value for current metric")

class H3VisualizationResponse(BaseModel):
    metric_type: str
    resolution: int
    total_hexagons: int
    data_bounds: Dict[str, float]
    hexagons: List[H3HexagonData]
    generated_at: datetime

# =================================================================
# BUSINESS LOGIC
# =================================================================

def calculate_market_opportunity(
    competition: float,
    transport: float, 
    residential: float,
    commercial: float
) -> float:
    """
    Розрахунок Market Opportunity Score
    Formula: (1 - Competition) * 0.5 + (Transport + Residential + Commercial) / 3 * 0.5
    """
    try:
        # ВИПРАВЛЕНО: Конвертуємо Decimal в float для PostgreSQL compatibility
        competition = float(competition or 0.0)
        transport = float(transport or 0.0)
        residential = float(residential or 0.0)
        commercial = float(commercial or 0.0)
        
        # Формула Market Opportunity
        opportunity = (1 - competition) * 0.5 + (transport + residential + commercial) / 3 * 0.5
        
        # Обмежуємо 0-1
        result = max(0.0, min(1.0, opportunity))
        
        return result
    except Exception as e:
        logger.warning(f"Error calculating opportunity: {e}")
        return 0.0

def get_competition_category(intensity: float) -> str:
    """Категорії для Competition Intensity"""
    if intensity <= 0.2:
        return "low"        # 🟢 Зелений - низька конкуренція
    elif intensity <= 0.4:
        return "medium"     # 🟡 Жовтий - помірна конкуренція  
    elif intensity <= 0.6:
        return "high"       # 🟠 Оранжевий - висока конкуренція
    else:
        return "maximum"    # 🔴 Червоний - максимальна конкуренція

def get_opportunity_category(score: float) -> str:
    """Категорії для Market Opportunity"""
    if score >= 0.7:
        return "high"       # 💎 Фіолетовий - high opportunity
    elif score >= 0.4:
        return "medium"     # 🔵 Синій - medium opportunity
    else:
        return "low"        # ⚫ Сірий - low opportunity

# =================================================================
# H3 GEOMETRY GENERATION - УНІВЕРСАЛЬНА ПІДТРИМКА v3.x та v4.x
# =================================================================

def generate_h3_geometry(h3_index: str) -> Dict[str, Any]:
    """
    УНІВЕРСАЛЬНА генерація GeoJSON геометрії з H3 індексу
    Підтримує H3 v3.x та v4.x API
    """
    if not H3_AVAILABLE or h3 is None:
        logger.warning("H3 library not available - using fallback geometry")
        return {
            "type": "Polygon", 
            "coordinates": [[[30.5, 50.4], [30.51, 50.4], [30.51, 50.41], [30.5, 50.41], [30.5, 50.4]]]
        }
        
    try:
        # СПРОБУЄМО H3 v4.x API спочатку
        if HAS_CELL_TO_BOUNDARY:
            boundary = h3.cell_to_boundary(h3_index)
            # Конвертуємо в GeoJSON format (lon, lat)
            geojson_coords = [[lon, lat] for lat, lon in boundary]
            geojson_coords.append(geojson_coords[0])  # Замикаємо полігон
            
            return {
                "type": "Polygon",
                "coordinates": [geojson_coords]
            }
            
        # FALLBACK H3 v3.x API
        elif HAS_H3_TO_GEO_BOUNDARY:
            boundary = h3.h3_to_geo_boundary(h3_index)
            # v3.x може повертати інший формат
            geojson_coords = [[lon, lat] for lat, lon in boundary] 
            geojson_coords.append(geojson_coords[0])
            
            return {
                "type": "Polygon",
                "coordinates": [geojson_coords]
            }
            
        else:
            logger.warning(f"No boundary function available in H3")
            raise Exception("No boundary functions found")
            
    except Exception as e:
        logger.warning(f"Failed to generate geometry for {h3_index}: {e}")
        
        # СПРОБУЄМО отримати центр принаймні
        try:
            if HAS_CELL_TO_LATLNG:
                lat, lon = h3.cell_to_latlng(h3_index)
            elif HAS_H3_TO_GEO:
                lat, lon = h3.h3_to_geo(h3_index)
            else:
                raise Exception("No center functions available")
                
            logger.info(f"H3 {h3_index} center: {lat}, {lon}")
            
            # Створюємо маленький квадрат навколо центру
            offset = 0.001
            return {
                "type": "Polygon",
                "coordinates": [[
                    [lon - offset, lat - offset],
                    [lon + offset, lat - offset], 
                    [lon + offset, lat + offset],
                    [lon - offset, lat + offset],
                    [lon - offset, lat - offset]
                ]]
            }
            
        except Exception as e2:
            logger.warning(f"Fallback center failed too: {e2}")
            # Повний fallback - Київ координати
            return {
                "type": "Polygon", 
                "coordinates": [[[30.5, 50.4], [30.51, 50.4], [30.51, 50.41], [30.5, 50.41], [30.5, 50.4]]]
            }

# =================================================================
# DATABASE CONNECTION
# =================================================================

# Database configuration (адаптовано під існуючу архітектуру)
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'georetail',
    'user': 'georetail_user',
    'password': 'georetail_secure_2024'
}

@contextmanager
def get_db_connection():
    """Context manager для підключення до PostgreSQL"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.set_session(autocommit=True)
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")
    finally:
        if conn:
            conn.close()

# =================================================================
# DATABASE QUERIES
# =================================================================

def get_kyiv_h3_data_with_fallback_geometry(
    resolution: int = 10,
    limit: int = 10000
) -> List[Dict]:
    """
    Отримання H3 даних для Києва БЕЗ JOIN з h3_grid
    Геометрії генеруємо через H3 Python library
    """
    query = """
    WITH kyiv_h3 AS (
        SELECT DISTINCT h3_res_10
        FROM osm_ukraine.poi_processed 
        WHERE region_name = 'Kyiv' 
          AND h3_res_10 IS NOT NULL
        LIMIT %s
    )
    SELECT 
        h.h3_index,
        h.resolution,
        COALESCE(h.competition_intensity, 0.0) as competition_intensity,
        COALESCE(h.transport_accessibility_score, 0.0) as transport_accessibility_score,
        COALESCE(h.residential_indicator_score, 0.0) as residential_indicator_score,
        COALESCE(h.commercial_activity_score, 0.0) as commercial_activity_score,
        COALESCE(h.opportunity_score, 0.0) as opportunity_score,
        COALESCE(h.poi_total_count, 0) as poi_total_count,
        COALESCE(h.retail_count, 0) as retail_count,
        COALESCE(h.competitor_count, 0) as competitor_count
    FROM osm_ukraine.h3_analytics_current h
    JOIN kyiv_h3 k ON h.h3_index = k.h3_res_10
    WHERE h.resolution = %s
    ORDER BY h.competition_intensity DESC NULLS LAST, h.poi_total_count DESC
    LIMIT %s;
    """
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (limit * 2, resolution, limit))
            rows = cur.fetchall()
            return [dict(row) for row in rows]

def get_data_bounds(resolution: int = 10) -> Dict[str, float]:
    """Отримання мінімальних/максимальних значень для нормалізації"""
    query = """
    WITH kyiv_h3 AS (
        SELECT DISTINCT h3_res_10
        FROM osm_ukraine.poi_processed 
        WHERE region_name = 'Kyiv' AND h3_res_10 IS NOT NULL
    )
    SELECT 
        MIN(COALESCE(h.competition_intensity, 0)) as min_competition,
        MAX(COALESCE(h.competition_intensity, 0)) as max_competition,
        MIN(COALESCE(h.transport_accessibility_score, 0)) as min_transport,
        MAX(COALESCE(h.transport_accessibility_score, 0)) as max_transport,
        MIN(COALESCE(h.residential_indicator_score, 0)) as min_residential,
        MAX(COALESCE(h.residential_indicator_score, 0)) as max_residential,
        MIN(COALESCE(h.commercial_activity_score, 0)) as min_commercial,
        MAX(COALESCE(h.commercial_activity_score, 0)) as max_commercial,
        COUNT(*) as total_count
    FROM osm_ukraine.h3_analytics_current h
    JOIN kyiv_h3 k ON h.h3_index = k.h3_res_10
    WHERE h.resolution = %s;
    """
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, (resolution,))
            row = cur.fetchone()
            return dict(row) if row else {}

# =================================================================
# API ENDPOINTS
# =================================================================

@router.get("/kyiv-h3", response_model=H3VisualizationResponse)
def get_kyiv_h3_visualization(
    metric: Literal["competition", "opportunity"] = Query(
        "competition", 
        description="Metric to visualize: competition or opportunity"
    ),
    resolution: int = Query(
        10, 
        ge=7, 
        le=10, 
        description="H3 resolution level (7-10, higher = more detailed)"
    ),
    limit: int = Query(
        10000, 
        ge=1, 
        le=100000, 
        description="Maximum number of hexagons to return"
    )
):
    """
    🗺️ **Головний endpoint для MVP візуалізації Київської області**
    
    Повертає H3 гексагони з метриками для інтерактивної карти:
    - **Competition Intensity**: Рівень конкуренції (0-100%)
    - **Market Opportunity**: Комбінована метрика можливостей
    
    **Auto-zoom logic**: 
    - H3-7: Oblast overview
    - H3-8: District level  
    - H3-9: City level
    - H3-10: Neighborhood detail
    """
    
    try:
        # Отримуємо дані з бази (БЕЗ h3_grid JOIN)
        raw_data = get_kyiv_h3_data_with_fallback_geometry(resolution, limit)
        bounds = get_data_bounds(resolution)
        
        if not raw_data:
            raise HTTPException(
                status_code=404, 
                detail=f"No H3 data found for Kyiv Oblast at resolution {resolution}"
            )
        
        # Обробляємо дані
        hexagons = []
        for row in raw_data:
            try:
                # Генеруємо геометрію через H3 Python library
                geometry = generate_h3_geometry(row['h3_index'])
                
                if not geometry:
                    logger.warning(f"Could not generate geometry for {row['h3_index']}")
                    continue
                
                # Розраховуємо Market Opportunity
                market_opportunity = calculate_market_opportunity(
                    competition=row['competition_intensity'],
                    transport=row['transport_accessibility_score'],
                    residential=row['residential_indicator_score'],
                    commercial=row['commercial_activity_score']
                )
                
                # Визначаємо display values based on selected metric
                if metric == "competition":
                    display_value = row['competition_intensity']
                    display_category = get_competition_category(display_value)
                else:  # opportunity
                    display_value = market_opportunity
                    display_category = get_opportunity_category(display_value)
                
                hexagon_data = H3HexagonData(
                    h3_index=row['h3_index'],
                    resolution=row['resolution'],
                    competition_intensity=row['competition_intensity'],
                    transport_accessibility_score=row['transport_accessibility_score'],
                    residential_indicator_score=row['residential_indicator_score'],
                    commercial_activity_score=row['commercial_activity_score'],
                    market_opportunity_score=market_opportunity,
                    poi_total_count=row['poi_total_count'],
                    retail_count=row['retail_count'],
                    competitor_count=row['competitor_count'],
                    geometry=geometry,
                    display_category=display_category,
                    display_value=display_value
                )
                
                hexagons.append(hexagon_data)
                
            except Exception as e:
                logger.warning(f"Error processing hexagon {row.get('h3_index', 'unknown')}: {e}")
                continue
        
        if not hexagons:
            raise HTTPException(
                status_code=404,
                detail="No valid hexagon data could be processed"
            )
        
        # Формуємо відповідь
        response = H3VisualizationResponse(
            metric_type=metric,
            resolution=resolution,
            total_hexagons=len(hexagons),
            data_bounds=bounds,
            hexagons=hexagons,
            generated_at=datetime.now()
        )
        
        logger.info(f"Returned {len(hexagons)} hexagons for {metric} at resolution {resolution}")
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_kyiv_h3_visualization: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/health")
def visualization_health():
    """Health check для visualization endpoints"""
    return {
        "status": "healthy",
        "service": "h3_visualization",
        "timestamp": datetime.now().isoformat(),
        "available_metrics": ["competition", "opportunity"],
        "supported_resolutions": [7, 8, 9, 10]
    }

# =================================================================
# DEBUG ENDPOINTS
# =================================================================

@router.get("/debug/kyiv-raw")
def debug_kyiv_raw_data(resolution: int = Query(10, ge=7, le=10)):
    """🔍 Debug endpoint - повертає сирі дані без обробки геометрій"""
    
    # Простіший запит без JOIN з h3_grid
    query = """
    WITH kyiv_h3 AS (
        SELECT DISTINCT h3_res_10
        FROM osm_ukraine.poi_processed 
        WHERE region_name = 'Kyiv' 
          AND h3_res_10 IS NOT NULL
        LIMIT 20
    )
    SELECT 
        h.h3_index,
        h.resolution,
        COALESCE(h.competition_intensity, 0.0) as competition_intensity,
        COALESCE(h.transport_accessibility_score, 0.0) as transport_accessibility_score,
        COALESCE(h.residential_indicator_score, 0.0) as residential_indicator_score,
        COALESCE(h.commercial_activity_score, 0.0) as commercial_activity_score,
        COALESCE(h.poi_total_count, 0) as poi_total_count,
        COALESCE(h.retail_count, 0) as retail_count,
        COALESCE(h.competitor_count, 0) as competitor_count
    FROM osm_ukraine.h3_analytics_current h
    JOIN kyiv_h3 k ON h.h3_index = k.h3_res_10
    WHERE h.resolution = %s
    ORDER BY h.competition_intensity DESC NULLS LAST
    LIMIT 10;
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (resolution,))
                rows = cur.fetchall()
                
                return {
                    "resolution": resolution,
                    "total_found": len(rows),
                    "data": [dict(row) for row in rows] if rows else [],
                    "debug_info": {
                        "query_executed": True,
                        "message": "Raw data without geometry processing"
                    }
                }
                
    except Exception as e:
        logger.error(f"Debug query failed: {e}")
        return {
            "error": str(e),
            "resolution": resolution,
            "debug_info": {
                "query_executed": False,
                "message": "Query failed - check database connection"
            }
        }

# =================================================================
# INTEGRATION ТОЧКА
# =================================================================

# Додати цей router до головного FastAPI app (main_safe.py):
# from api.endpoints.h3_visualization import router as visualization_router
# app.include_router(visualization_router)