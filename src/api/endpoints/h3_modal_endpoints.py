# src/api/endpoints/h3_modal_endpoints.py
"""
🗂️ H3 Modal API Endpoints
RESTful API для детального аналізу H3 гексагонів з інтеграцією реальних даних
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Path
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional, Union
import h3
import logging
from datetime import datetime

# Імпорт database service
try:
    from ..services.database_service import get_database_service, DatabaseService
    DATABASE_SERVICE_AVAILABLE = True
except ImportError:
    DATABASE_SERVICE_AVAILABLE = False
    # Fallback якщо database service недоступний
    class DatabaseService:
        """Placeholder class for type hints when database service is unavailable"""
        def get_h3_analytics(self, h3_index: str, resolution: int = 10) -> Dict[str, Any]:
            return {"error": "Database service not available"}
        
        def get_poi_in_hexagon(self, h3_index: str, resolution: int = 10, include_neighbors: bool = False) -> List[Dict[str, Any]]:
            return []
        
        def get_competitive_analysis(self, h3_index: str, radius_rings: int = 2, resolution: int = 10) -> Dict[str, Any]:
            return {"error": "Database service not available", "competitors": []}
    
    def get_database_service():
        return DatabaseService()

# Налаштування логування
logger = logging.getLogger(__name__)

# Створюємо роутер з префіксом
router = APIRouter(
    prefix="/api/v1/hexagon-details",
    tags=["H3 Hexagon Analysis"]
)

# ===============================================
# КОНФІГУРАЦІЇ АНАЛІЗУ
# ===============================================

ANALYSIS_CONFIGS = {
    "pedestrian_competition": {
        "name": "Пішохідна конкуренція",
        "description": "Аналіз конкурентів в пішохідній доступності (~500м)",
        "target_area_km2": 0.5,
        "max_rings": 3,
        "focus": ["competitors", "retail"],
        "weight_multiplier": 1.5
    },
    "transport_accessibility": {
        "name": "Транспортна доступність",
        "description": "Аналіз доступності громадським транспортом (~2км)",
        "target_area_km2": 2.0,
        "max_rings": 5,
        "focus": ["transport", "infrastructure"],
        "weight_multiplier": 1.2
    },
    "market_overview": {
        "name": "Огляд ринку",
        "description": "Широкий аналіз ринкового середовища (~5км)",
        "target_area_km2": 5.0,
        "max_rings": 7,
        "focus": ["all"],
        "weight_multiplier": 1.0
    },
    "site_selection": {
        "name": "Вибір локації",
        "description": "Оптимальна зона для нового магазину (~1.5км)",
        "target_area_km2": 1.5,
        "max_rings": 4,
        "focus": ["demographics", "competitors"],
        "weight_multiplier": 1.3
    },
    "custom": {
        "name": "Користувацький",
        "description": "Аналіз з вказаним користувачем радіусом",
        "target_area_km2": 1.0,
        "max_rings": 10,
        "focus": ["all"],
        "weight_multiplier": 1.0
    }
}

# ===============================================
# H3 МАТЕМАТИЧНІ ФУНКЦІЇ
# ===============================================

def calculate_optimal_rings(resolution: int, target_area_km2: float) -> int:
    """Розрахунок оптимальної кількості кілець для цільової площі"""
    try:
        if resolution < 7 or resolution > 10:
            raise ValueError(f"Resolution {resolution} не підтримується (7-10)")
        
        # Площа одного гексагона для resolution
        single_hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        
        if target_area_km2 <= single_hex_area:
            return 0  # Тільки центральний гексагон
        
        # Формула для кількості гексагонів в k-ring: 1 + 3*k*(k+1)
        for rings in range(1, 15):
            total_hexagons = 1 + 3 * rings * (rings + 1)
            total_area = total_hexagons * single_hex_area
            
            if total_area >= target_area_km2:
                return rings
        
        return 10  # Максимум 10 кілець
        
    except Exception as e:
        logger.error(f"Error calculating optimal rings: {e}")
        return 2  # Fallback

def get_area_coverage(resolution: int, rings: int) -> float:
    """Розрахунок загальної площі покриття для заданої кількості кілець"""
    try:
        single_hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        total_hexagons = 1 + 3 * rings * (rings + 1)
        return total_hexagons * single_hex_area
    except Exception:
        return 0.0

# ===============================================
# MAIN API ENDPOINTS
# ===============================================

@router.get("/details/{h3_index}")
async def get_hexagon_details(
    h3_index: str = Path(..., description="H3 індекс гексагона"),
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution (7-10)"),
    analysis_type: str = Query("pedestrian_competition", description="Тип аналізу"),
    custom_rings: Optional[int] = Query(None, ge=1, le=10, description="Кількість кілець для custom аналізу"),
    db: DatabaseService = Depends(get_database_service)
) -> Dict[str, Any]:
    """🎯 Детальний аналіз H3 гексагона з конфігурованими параметрами"""
    
    # Валідація H3 індексу
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail=f"Invalid H3 index: {h3_index}")
    
    # Отримуємо конфігурацію аналізу
    if analysis_type not in ANALYSIS_CONFIGS:
        raise HTTPException(status_code=400, detail=f"Unknown analysis type: {analysis_type}")
    
    config = ANALYSIS_CONFIGS[analysis_type].copy()
    
    # Обробляємо custom кільця
    if analysis_type == "custom" and custom_rings:
        config["max_rings"] = custom_rings
        config["target_area_km2"] = get_area_coverage(resolution, custom_rings)
    
    # Розраховуємо кількість кілець
    optimal_rings = calculate_optimal_rings(resolution, config["target_area_km2"])
    actual_rings = min(optimal_rings, config["max_rings"])
    
    # Базова інформація про локацію
    lat, lon = h3.cell_to_latlng(h3_index)
    area_km2 = h3.cell_area(h3_index, unit='km^2')
    
    # Отримуємо аналітичні дані через database service
    analytics_data = db.get_h3_analytics(h3_index, resolution)
    
    # Отримуємо POI дані
    poi_data = db.get_poi_in_hexagon(h3_index, resolution, include_neighbors=True)
    
    # Розраховуємо покриття сусідніх областей
    neighbor_coverage = {
        "rings": actual_rings,
        "hexagon_count": 1 + 3 * actual_rings * (actual_rings + 1),
        "area_km2": round(get_area_coverage(resolution, actual_rings), 3),
        "radius_estimate_m": round((get_area_coverage(resolution, actual_rings) / 3.14159) ** 0.5 * 1000, 0)
    }
    
    # Формуємо відповідь
    result = {
        "location_info": {
            "h3_index": h3_index,
            "resolution": resolution,
            "center_lat": lat,
            "center_lon": lon,
            "area_km2": round(area_km2, 6)
        },
        "analysis_config": {
            "type": analysis_type,
            "name": config["name"],
            "description": config["description"],
            "rings_used": actual_rings
        },
        "metrics": analytics_data.get("density_metrics", {}),
        "poi_details": {
            "total_found": len(poi_data),
            "summary": analytics_data.get("poi_stats", {}),
            "top_pois": poi_data[:10]  # Топ 10 POI
        },
        "influence_analysis": analytics_data.get("influence_metrics", {}),
        "neighbor_coverage": neighbor_coverage,
        "available_analyses": list(ANALYSIS_CONFIGS.keys()),
        "database_status": "available" if DATABASE_SERVICE_AVAILABLE else "mock_data",
        "generated_at": datetime.now().isoformat()
    }
    
    return result

@router.get("/poi-in-hexagon/{h3_index}")
async def get_poi_in_hexagon(
    h3_index: str = Path(..., description="H3 індекс гексагона"),
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution (7-10)"),
    include_neighbors: int = Query(0, ge=0, le=3, description="Кількість кілець сусідніх гексагонів"),
    poi_types: Optional[str] = Query(None, description="Фільтр по типах POI (через кому)"),
    db: DatabaseService = Depends(get_database_service)
) -> Dict[str, Any]:
    """🏪 Отримання POI в гексагоні та сусідніх областях"""
    
    # Валідація H3 індексу
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail=f"Invalid H3 index: {h3_index}")
    
    # Отримуємо POI дані
    poi_data = db.get_poi_in_hexagon(
        h3_index=h3_index,
        resolution=resolution,
        include_neighbors=(include_neighbors > 0)
    )
    
    # Фільтрація по типах якщо вказано
    if poi_types:
        allowed_types = [t.strip().lower() for t in poi_types.split(',')]
        poi_data = [
            poi for poi in poi_data 
            if poi.get('functional_group', '').lower() in allowed_types
            or poi.get('primary_category', '').lower() in allowed_types
        ]
    
    # Статистика
    total_poi = len(poi_data)
    by_type = {}
    by_brand = {}
    
    for poi in poi_data:
        # Статистика по типах
        poi_type = poi.get('functional_group', 'unknown')
        by_type[poi_type] = by_type.get(poi_type, 0) + 1
        
        # Статистика по брендах
        brand = poi.get('brand', 'unknown')
        if brand and brand != 'unknown':
            by_brand[brand] = by_brand.get(brand, 0) + 1
    
    # Базова інформація про локацію
    lat, lon = h3.cell_to_latlng(h3_index)
    
    return {
        "location_info": {
            "h3_index": h3_index,
            "resolution": resolution,
            "center_lat": lat,
            "center_lon": lon,
            "include_neighbors": include_neighbors
        },
        "poi_summary": {
            "total_found": total_poi,
            "by_type": by_type,
            "by_brand": dict(sorted(by_brand.items(), key=lambda x: x[1], reverse=True)[:10]),
            "unique_brands": len(by_brand),
            "competitors": len([poi for poi in poi_data if poi.get('functional_group') == 'competitor'])
        },
        "poi_details": poi_data,
        "search_parameters": {
            "resolution": resolution,
            "include_neighbors": include_neighbors,
            "poi_types_filter": poi_types,
            "database_status": "available" if DATABASE_SERVICE_AVAILABLE else "mock_data",
            "search_timestamp": datetime.now().isoformat()
        }
    }

@router.get("/competitive-analysis/{h3_index}")
async def get_competitive_analysis(
    h3_index: str = Path(..., description="H3 індекс центрального гексагона"),
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution (7-10)"),
    radius_rings: int = Query(2, ge=1, le=5, description="Радіус в кільцях навколо центру"),
    competitor_types: Optional[str] = Query(None, description="Типи конкурентів для аналізу"),
    db: DatabaseService = Depends(get_database_service)
) -> Dict[str, Any]:
    """🥊 Детальний аналіз конкуренції навколо локації"""
    
    # Валідація H3 індексу
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail=f"Invalid H3 index: {h3_index}")
    
    # Отримуємо конкурентний аналіз
    competitive_data = db.get_competitive_analysis(
        h3_index=h3_index,
        radius_rings=radius_rings,
        resolution=resolution
    )
    
    # Фільтрація конкурентів якщо вказано
    if competitor_types:
        allowed_types = [t.strip().lower() for t in competitor_types.split(',')]
        competitors = competitive_data.get("competitors", [])
        filtered_competitors = [
            comp for comp in competitors
            if comp.get('type', '').lower() in allowed_types
        ]
        competitive_data["competitors"] = filtered_competitors
        competitive_data["competitors_found"] = len(filtered_competitors)
    
    # Базова інформація про локацію
    lat, lon = h3.cell_to_latlng(h3_index)
    
    # Додаткова аналітика
    competitors = competitive_data.get("competitors", [])
    
    # Розподіл по відстанях
    distance_distribution = {}
    for comp in competitors:
        distance = comp.get("distance_rings", 0)
        distance_distribution[f"ring_{distance}"] = distance_distribution.get(f"ring_{distance}", 0) + 1
    
    return {
        "location_info": {
            "h3_index": h3_index,
            "resolution": resolution,
            "center_lat": lat,
            "center_lon": lon,
            "analysis_radius_rings": radius_rings
        },
        "competitive_analysis": competitive_data,
        "distance_analysis": {
            "distribution_by_rings": distance_distribution,
            "average_distance": round(
                sum(comp.get("distance_rings", 0) for comp in competitors) / max(len(competitors), 1), 2
            ),
            "closest_competitor": min(competitors, key=lambda x: x.get("distance_rings", 999)) if competitors else None
        },
        "market_insights": {
            "saturation_level": "high" if len(competitors) > 10 else "medium" if len(competitors) > 5 else "low",
            "opportunity_assessment": competitive_data.get("recommendations", {}).get("market_opportunity", "unknown"),
            "key_risks": competitive_data.get("recommendations", {}).get("risk_factors", [])
        },
        "analysis_metadata": {
            "competitor_types_filter": competitor_types,
            "database_status": "available" if DATABASE_SERVICE_AVAILABLE else "mock_data",
            "search_timestamp": datetime.now().isoformat(),
            "data_quality": "real_data" if competitors else "no_data"
        }
    }

@router.get("/analysis-preview/{h3_index}")
async def get_analysis_preview(
    h3_index: str = Path(..., description="H3 індекс гексагона"),
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution (7-10)")
) -> Dict[str, Any]:
    """📊 Швидкий preview доступних аналізів для гексагона"""
    
    # Валідація H3 індексу
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail=f"Invalid H3 index: {h3_index}")
    
    # Базова інформація
    lat, lon = h3.cell_to_latlng(h3_index)
    area_km2 = h3.cell_area(h3_index, unit='km^2')
    
    # Формуємо preview доступних аналізів
    available_analyses = []
    for analysis_type, config in ANALYSIS_CONFIGS.items():
        rings = calculate_optimal_rings(resolution, config["target_area_km2"])
        actual_rings = min(rings, config["max_rings"])
        
        analysis_info = {
            "type": analysis_type,
            "name": config["name"],
            "description": config["description"],
            "estimated_rings": actual_rings,
            "estimated_area_km2": round(get_area_coverage(resolution, actual_rings), 2),
            "focus_areas": config["focus"],
            "endpoint": f"/api/v1/hexagon-details/details/{h3_index}?resolution={resolution}&analysis_type={analysis_type}"
        }
        available_analyses.append(analysis_info)
    
    return {
        "location_info": {
            "h3_index": h3_index,
            "resolution": resolution,
            "center_lat": lat,
            "center_lon": lon,
            "single_hex_area_km2": round(area_km2, 6)
        },
        "available_analyses": available_analyses,
        "quick_actions": {
            "poi_search": f"/api/v1/hexagon-details/poi-in-hexagon/{h3_index}?resolution={resolution}",
            "competitive_analysis": f"/api/v1/hexagon-details/competitive-analysis/{h3_index}?resolution={resolution}",
            "coverage_calculator": f"/api/v1/hexagon-details/coverage-calculator?resolution={resolution}&rings=2"
        },
        "system_status": {
            "database_service": "available" if DATABASE_SERVICE_AVAILABLE else "fallback",
            "h3_library": "v4.3.0",
            "data_source": "real_postgresql" if DATABASE_SERVICE_AVAILABLE else "mock_data"
        },
        "generated_at": datetime.now().isoformat()
    }

# ===============================================
# UTILITY ENDPOINTS
# ===============================================

@router.get("/coverage-calculator")
async def calculate_coverage(
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution (7-10)"),
    rings: int = Query(..., ge=0, le=10, description="Кількість кілець навколо центру")
) -> Dict[str, Any]:
    """🧮 Калькулятор покриття площі для H3 резолюції та кілець"""
    
    try:
        # Розрахунки H3
        single_hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        total_hexagons = 1 + 3 * rings * (rings + 1) if rings > 0 else 1
        total_area = total_hexagons * single_hex_area
        
        # Оцінка радіусу
        radius_km = (total_area / 3.14159) ** 0.5
        radius_m = radius_km * 1000
        
        # Розподіл по кільцях
        ring_breakdown = []
        for ring in range(rings + 1):
            if ring == 0:
                hexagons_in_ring = 1
            else:
                hexagons_in_ring = 6 * ring
            
            ring_breakdown.append({
                "ring": ring,
                "hexagons": hexagons_in_ring,
                "area_km2": round(hexagons_in_ring * single_hex_area, 4)
            })
        
        return {
            "input_parameters": {
                "resolution": resolution,
                "rings": rings
            },
            "coverage_results": {
                "single_hex_area_km2": round(single_hex_area, 6),
                "total_hexagons": total_hexagons,
                "total_area_km2": round(total_area, 4),
                "radius_estimate_km": round(radius_km, 2),
                "radius_estimate_m": round(radius_m, 0)
            },
            "ring_breakdown": ring_breakdown,
            "comparison": {
                "pedestrian_distance": "~500m" if radius_m <= 600 else "beyond walking",
                "cycling_distance": "~2km" if radius_km <= 3 else "beyond cycling",
                "driving_distance": "~5km" if radius_km <= 7 else "extended driving"
            },
            "calculated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error in coverage calculation: {e}")
        raise HTTPException(status_code=500, detail=f"Calculation error: {str(e)}")

@router.get("/health")
async def health_check() -> Dict[str, Any]:
    """🏥 Health check для H3 Modal API"""
    
    # Тест H3 функціональності
    test_h3_index = "8a1fb46622d7fff"
    h3_working = False
    
    try:
        if h3.is_valid_cell(test_h3_index):
            lat, lon = h3.cell_to_latlng(test_h3_index)
            h3_working = True
    except Exception:
        h3_working = False
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "h3_library": {
                "status": "operational" if h3_working else "error",
                "version": "4.3.0",
                "test_passed": h3_working
            },
            "database_service": {
                "status": "available" if DATABASE_SERVICE_AVAILABLE else "fallback",
                "data_source": "postgresql" if DATABASE_SERVICE_AVAILABLE else "mock"
            },
            "endpoints": {
                "details": "operational",
                "poi_search": "operational",
                "competitive_analysis": "operational",
                "coverage_calculator": "operational"
            }
        },
        "capabilities": {
            "h3_mathematics": True,
            "real_data_integration": DATABASE_SERVICE_AVAILABLE,
            "mock_data_fallback": True,
            "competitive_analysis": True,
            "poi_analysis": True
        }
    }

# ===============================================
# ROUTER EXPORT
# ===============================================

# Експорт router для використання в main файлах
__all__ = ["router"]