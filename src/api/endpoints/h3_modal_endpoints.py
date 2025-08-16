# src/api/endpoints/h3_modal_endpoints.py
"""
🗂️ H3 Modal API Endpoints
RESTful API для детального аналізу H3 гексагонів з інтеграцією бази даних
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
except ImportError:
    # Fallback якщо database service недоступний
    class DatabaseService:
        """Placeholder class for type hints when database service is unavailable"""
        pass
    
    def get_database_service():
        return None

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
        # H3 v4.x uses average_hexagon_area with resolution
        single_hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        
        if target_area_km2 <= single_hex_area:
            return 0  # Тільки центральний гексагон
        
        # Формула для кількості гексагонів в k-ring: 1 + 3*k*(k+1)
        # Підбираємо k так, щоб площа була близька до цільової
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
    """Розрахунок загальної площі покриття"""
    try:
        # H3 v4.x uses average_hexagon_area with resolution
        single_hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        
        if rings == 0:
            return single_hex_area
        
        total_hexagons = 1 + 3 * rings * (rings + 1)
        return total_hexagons * single_hex_area
        
    except Exception as e:
        logger.error(f"Error calculating area coverage: {e}")
        # Fallback to approximate values
        approximate_areas = {
            7: 5.161293360,
            8: 0.737327598,
            9: 0.105332513,
            10: 0.015047502
        }
        single_hex_area = approximate_areas.get(resolution, 0.015)
        if rings == 0:
            return single_hex_area
        total_hexagons = 1 + 3 * rings * (rings + 1)
        return total_hexagons * single_hex_area

# ===============================================
# API ENDPOINTS
# ===============================================

@router.get("/details/{h3_index}", tags=["Hexagon Analysis"])
async def get_hexagon_details(
    h3_index: str,
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution (7-10)"),
    analysis_type: str = Query("pedestrian_competition", description="Type of analysis"),
    custom_rings: Optional[int] = Query(None, ge=0, le=10, description="Custom rings (only for 'custom' analysis)"),
    db: Union[DatabaseService, None] = Depends(get_database_service)
) -> Dict[str, Any]:
    """Get detailed information about a specific H3 hexagon with analysis"""
    
    # Validate H3 index
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail="Invalid H3 index")
    
    # Validate analysis type
    if analysis_type not in ANALYSIS_CONFIGS:
        raise HTTPException(status_code=400, detail=f"Invalid analysis type. Available: {list(ANALYSIS_CONFIGS.keys())}")
    
    try:
        # Get H3 center coordinates - H3 v4.x compatibility
        center_lat, center_lon = h3.cell_to_latlng(h3_index)
        
        # Get hex area
        hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        
        # Determine analysis configuration
        if analysis_type == "custom" and custom_rings is not None:
            rings = custom_rings
            target_area = get_area_coverage(resolution, rings)
        else:
            config = ANALYSIS_CONFIGS.get(analysis_type, ANALYSIS_CONFIGS["pedestrian_competition"])
            rings = calculate_optimal_rings(resolution, config["target_area_km2"])
            rings = min(rings, config["max_rings"])
        
        # Calculate coverage metrics
        area_coverage = get_area_coverage(resolution, rings)
        neighbor_count = 1 + 3 * rings * (rings + 1) if rings > 0 else 1
        
        # Get POI data from database (or mock data)
        poi_data = []
        if db:
            try:
                poi_data = db.get_poi_in_hexagon(h3_index, include_neighbors=(rings > 0))
            except Exception as e:
                logger.warning(f"Database POI query failed: {e}, using mock data")
        
        # Generate mock data if no database or query failed
        if not poi_data:
            poi_data = [
                {
                    "poi_id": "mock_001",
                    "name": "Sample Store 1",
                    "canonical_name": "Brand A",
                    "primary_category": "retail",
                    "secondary_category": "convenience",
                    "influence_weight": 0.8,
                    "distance_from_center": 150,
                    "h3_index": h3_index
                },
                {
                    "poi_id": "mock_002", 
                    "name": "Sample Store 2",
                    "canonical_name": "Brand B",
                    "primary_category": "food",
                    "secondary_category": "restaurant", 
                    "influence_weight": 0.6,
                    "distance_from_center": 300,
                    "h3_index": h3_index
                }
            ]
        
        # Get analytics from database (or mock)
        metrics = {}
        if db:
            try:
                analytics = db.get_h3_analytics(h3_index)
                if analytics:
                    metrics = {
                        "poi_density": analytics.get("poi_density", 0),
                        "population_estimate": analytics.get("population_estimate", 0),
                        "foot_traffic_score": analytics.get("foot_traffic_score", 0),
                        "competition_score": analytics.get("competition_score", 0),
                        "transport_accessibility": analytics.get("transport_accessibility", 0),
                        "data_quality_score": analytics.get("data_quality_score", 0.85)
                    }
            except Exception as e:
                logger.warning(f"Database analytics query failed: {e}, using mock data")
        
        # Generate mock metrics if needed
        if not metrics:
            # Generate deterministic mock data based on H3 index
            hash_val = hash(h3_index)
            metrics = {
                "poi_density": round((abs(hash_val) % 50) / 10.0, 1),
                "population_estimate": (abs(hash_val) % 1000) + 100,
                "foot_traffic_score": round((abs(hash_val) % 100) / 100.0, 2),
                "competition_score": round((abs(hash_val >> 8) % 100) / 100.0, 2),
                "transport_accessibility": round((abs(hash_val >> 16) % 100) / 100.0, 2),
                "data_quality_score": 0.85
            }
        
        # Build response
        response = {
            "location_info": {
                "h3_index": h3_index,
                "resolution": resolution,
                "center_lat": center_lat,
                "center_lon": center_lon,
                "area_km2": hex_area
            },
            "analysis_config": {
                "type": analysis_type,
                "rings_analyzed": rings,
                "custom_rings_used": analysis_type == "custom"
            },
            "neighbor_coverage": {
                "rings": rings,
                "hexagon_count": neighbor_count,
                "area_km2": area_coverage
            },
            "metrics": metrics,
            "poi_details": poi_data,
            "analysis_timestamp": datetime.now().isoformat()
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error processing hexagon details for {h3_index}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/analysis-preview/{h3_index}", tags=["Analysis Preview"])
async def get_analysis_preview(
    h3_index: str,
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution")
) -> Dict[str, Any]:
    """Get preview of all available analysis types for a hexagon"""
    
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail="Invalid H3 index")
    
    try:
        # Get basic location info - H3 v4.x compatibility
        center_lat, center_lon = h3.cell_to_latlng(h3_index)
        
        # Generate preview for all analysis types
        available_analyses = []
        for analysis_name, config in ANALYSIS_CONFIGS.items():
            optimal_rings = calculate_optimal_rings(resolution, config["target_area_km2"])
            optimal_rings = min(optimal_rings, config["max_rings"])
            estimated_area = get_area_coverage(resolution, optimal_rings)
            estimated_count = 1 + 3 * optimal_rings * (optimal_rings + 1) if optimal_rings > 0 else 1
            
            available_analyses.append({
                "analysis_type": analysis_name,
                "name": config["name"],
                "description": config["description"],
                "optimal_rings": optimal_rings,
                "estimated_area_km2": estimated_area,
                "hexagon_count": estimated_count,
                "max_rings": config["max_rings"]
            })
        
        return {
            "h3_index": h3_index,
            "resolution": resolution,
            "center_coordinates": {
                "latitude": center_lat,
                "longitude": center_lon
            },
            "available_analyses": available_analyses,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting analysis preview for {h3_index}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/generate-kyiv-h3/{resolution}", tags=["Utilities"])
async def generate_kyiv_h3(resolution: int = Path(..., ge=7, le=10, description="H3 resolution")) -> Dict[str, Any]:
    """Generate valid H3 index for Kyiv center"""
    try:
        # Київ центр: 50.4501, 30.5234
        kyiv_lat = 50.4501
        kyiv_lon = 30.5234
        
        # Generate H3 cell
        h3_index = h3.latlng_to_cell(kyiv_lat, kyiv_lon, resolution)
        
        # Verify it's valid
        is_valid = h3.is_valid_cell(h3_index)
        
        # Get center back
        center_lat, center_lon = h3.cell_to_latlng(h3_index)
        
        # Get area
        area_km2 = h3.average_hexagon_area(resolution, unit='km^2')
        
        return {
            "h3_index": h3_index,
            "resolution": resolution,
            "is_valid": is_valid,
            "center_coordinates": {
                "latitude": center_lat,
                "longitude": center_lon
            },
            "area_km2": area_km2,
            "original_coordinates": {
                "latitude": kyiv_lat,
                "longitude": kyiv_lon
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating H3 for Kyiv: {str(e)}")

@router.get("/debug-h3", tags=["Debug"])
async def debug_h3_functions() -> Dict[str, Any]:
    """Debug H3 functions to find working area calculation"""
    import h3
    
    results = {
        "h3_version": getattr(h3, '__version__', 'unknown'),
        "available_functions": [f for f in dir(h3) if not f.startswith('_')],
        "area_tests": {},
        "coordinate_tests": {},
        "validation_tests": {}
    }
    
    resolution = 10
    test_h3 = "8a1fb6699bfffff"
    
    # Test area functions
    area_functions = [
        ('hex_area(res)', lambda: h3.hex_area(resolution)),
        ('hex_area(res, "km^2")', lambda: h3.hex_area(resolution, "km^2")),
        ('hex_area(res, unit="km^2")', lambda: h3.hex_area(resolution, unit="km^2")),
        ('cell_area(res)', lambda: h3.cell_area(resolution) if hasattr(h3, 'cell_area') else None),
        ('cell_area(res, "km^2")', lambda: h3.cell_area(resolution, "km^2") if hasattr(h3, 'cell_area') else None),
        ('hex_area_km2(res)', lambda: h3.hex_area_km2(resolution) if hasattr(h3, 'hex_area_km2') else None),
        ('hex_area_m2(res)', lambda: h3.hex_area_m2(resolution) if hasattr(h3, 'hex_area_m2') else None),
    ]
    
    for name, func in area_functions:
        try:
            result = func()
            results["area_tests"][name] = {"value": result, "status": "success"}
        except Exception as e:
            results["area_tests"][name] = {"error": str(e), "status": "failed"}
    
    # Test coordinate functions
    coord_functions = [
        ('h3_to_geo', lambda: h3.h3_to_geo(test_h3) if hasattr(h3, 'h3_to_geo') else None),
        ('cell_to_latlng', lambda: h3.cell_to_latlng(test_h3) if hasattr(h3, 'cell_to_latlng') else None),
        ('h3_to_lat_lng', lambda: h3.h3_to_lat_lng(test_h3) if hasattr(h3, 'h3_to_lat_lng') else None),
    ]
    
    for name, func in coord_functions:
        try:
            result = func()
            results["coordinate_tests"][name] = {"value": result, "status": "success"}
        except Exception as e:
            results["coordinate_tests"][name] = {"error": str(e), "status": "failed"}
    
    # Test validation functions
    validation_functions = [
        ('h3_is_valid', lambda: h3.h3_is_valid(test_h3) if hasattr(h3, 'h3_is_valid') else None),
        ('is_valid_cell', lambda: h3.is_valid_cell(test_h3) if hasattr(h3, 'is_valid_cell') else None),
    ]
    
    for name, func in validation_functions:
        try:
            result = func()
            results["validation_tests"][name] = {"value": result, "status": "success"}
        except Exception as e:
            results["validation_tests"][name] = {"error": str(e), "status": "failed"}
    
    # Add approximate areas as fallback
    results["approximate_areas"] = {
        7: 5.161293360,
        8: 0.737327598,
        9: 0.105332513,
        10: 0.015047502
    }
    
    return results

@router.get("/coverage-calculator", tags=["Utilities"])
async def get_coverage_calculator(
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution"),
    rings: int = Query(..., ge=0, le=10, description="Number of rings")
) -> Dict[str, Any]:
    """Calculate coverage area for given resolution and rings"""
    
    try:
        area_coverage = get_area_coverage(resolution, rings)
        hexagon_count = 1 + 3 * rings * (rings + 1) if rings > 0 else 1
        radius_estimate = int((area_coverage / 3.14159) ** 0.5 * 1000)
        
        # Generate step-by-step breakdown
        coverage_breakdown = []
        for r in range(0, min(rings + 1, 6)):  # Show up to 5 steps
            step_area = get_area_coverage(resolution, r)
            step_count = 1 + 3 * r * (r + 1) if r > 0 else 1
            description = "Центральний гексагон" if r == 0 else f"+{r} кільце{'а' if r < 5 else 'ець'}"
            
            coverage_breakdown.append({
                "rings": r,
                "area_km2": step_area,
                "hexagon_count": step_count,
                "description": description
            })
        
        return {
            "resolution": resolution,
            "rings": rings,
            "total_area_km2": area_coverage,
            "total_hexagon_count": hexagon_count,
            "radius_estimate_m": radius_estimate,
            "coverage_breakdown": coverage_breakdown,
            "recommendations": {
                "pedestrian_range": rings <= 3,
                "car_accessible": rings >= 2,
                "market_overview": rings >= 4
            },
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error calculating coverage: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/poi-in-hexagon/{h3_index}", tags=["POI Analysis"])
async def get_poi_in_hexagon(
    h3_index: str,
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution"),
    include_neighbors: int = Query(0, ge=0, le=3, description="Include neighbor rings (0-3)"),
    db: Union[DatabaseService, None] = Depends(get_database_service)
) -> Dict[str, Any]:
    """Get all POIs within a hexagon and optionally its neighbors"""
    
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail="Invalid H3 index")
    
    try:
        # Get hexagon info
        center_lat, center_lon = h3.cell_to_latlng(h3_index)
        hex_area = h3.average_hexagon_area(resolution, unit='km^2')
        
        # Calculate coverage area if neighbors included
        total_area = get_area_coverage(resolution, include_neighbors) if include_neighbors > 0 else hex_area
        total_hexagons = 1 + 3 * include_neighbors * (include_neighbors + 1) if include_neighbors > 0 else 1
        
        # Get POI data
        poi_data = []
        if db:
            try:
                poi_data = db.get_poi_in_hexagon(h3_index, include_neighbors=(include_neighbors > 0))
            except Exception as e:
                logger.warning(f"Database POI query failed: {e}, using mock data")
        
        # Generate mock data if needed
        if not poi_data:
            poi_data = [
                {
                    "poi_id": f"mock_{i:03d}",
                    "name": f"POI {i}",
                    "primary_category": "retail" if i % 2 == 0 else "food",
                    "canonical_name": f"Brand {(i % 5) + 1}",
                    "distance_from_center": i * 50,
                    "h3_index": h3_index
                }
                for i in range(1, min(include_neighbors * 3 + 2, 15))
            ]
        
        response = {
            "h3_index": h3_index,
            "resolution": resolution,
            "center_coordinates": {
                "latitude": center_lat,
                "longitude": center_lon
            },
            "coverage": {
                "neighbor_rings": include_neighbors,
                "total_hexagons": total_hexagons,
                "total_area_km2": total_area
            },
            "poi_summary": {
                "total_pois": len(poi_data),
                "poi_density_per_km2": round(len(poi_data) / total_area, 2) if total_area > 0 else 0
            },
            "poi_details": poi_data,
            "timestamp": datetime.now().isoformat()
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error getting POI data for {h3_index}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get("/competitive-analysis/{h3_index}", tags=["Competitive Analysis"])
async def get_competitive_analysis(
    h3_index: str,
    resolution: int = Query(..., ge=7, le=10, description="H3 resolution"),
    radius_rings: int = Query(2, ge=1, le=5, description="Analysis radius in rings"),
    db: Union[DatabaseService, None] = Depends(get_database_service)
) -> Dict[str, Any]:
    """Perform competitive analysis around a hexagon"""
    
    if not h3.is_valid_cell(h3_index):
        raise HTTPException(status_code=400, detail="Invalid H3 index")
    
    try:
        # Get analysis from database or generate mock
        analysis = {}
        if db:
            try:
                analysis = db.get_competitive_analysis(h3_index, radius_rings)
            except Exception as e:
                logger.warning(f"Database competitive analysis failed: {e}, using mock data")
        
        # Generate mock analysis if needed
        if not analysis:
            # Get all hexagons in the analysis area using H3
            area_hexagons = h3.grid_disk(h3_index, radius_rings)
            
            # Generate mock competitors
            competitors = []
            for i, hex_id in enumerate(list(area_hexagons)[:10]):  # Limit to 10
                if hex_id != h3_index:  # Exclude center
                    geo = h3.cell_to_latlng(hex_id)
                    competitor = {
                        "h3_index": hex_id,
                        "name": f"Competitor {i+1}",
                        "brand": f"Brand {(i % 3) + 1}",
                        "primary_category": "retail",
                        "latitude": geo[0],
                        "longitude": geo[1],
                        "competition_strength": round((hash(hex_id) % 100) / 100.0, 2),
                        "distance_rings": min(radius_rings, (hash(hex_id) % radius_rings) + 1)
                    }
                    competitors.append(competitor)
            
            # Generate analysis summary
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
        
        # Add additional metadata
        center_lat, center_lon = h3.cell_to_latlng(h3_index)
        total_area = get_area_coverage(resolution, radius_rings)
        
        response = {
            "analysis_metadata": {
                "h3_index": h3_index,
                "resolution": resolution,
                "center_coordinates": {
                    "latitude": center_lat,
                    "longitude": center_lon
                },
                "analysis_radius_rings": radius_rings,
                "total_area_analyzed_km2": total_area,
                "analysis_timestamp": datetime.now().isoformat()
            },
            "competitive_analysis": analysis
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error in competitive analysis for {h3_index}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# ===============================================
# ROUTER EXPORT
# ===============================================

# Експорт router для використання в main_safe.py
__all__ = ["router"]