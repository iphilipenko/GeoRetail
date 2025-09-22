"""
Territories Router for API v2
Handles Explorer Mode endpoints for UC1
Complete implementation with all endpoints
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.orm import Session
from datetime import datetime

# Імпорти з префіксом src. (коли main.py в корені)
from src.api.v2.core.dependencies import get_current_user, require_permission
from src.core.rbac_database import get_db as get_db_session
from src.api.v2.territories.schemas import (
    AdminUnitResponse,
    AdminMetricsResponse, 
    H3HexagonResponse,
    H3GridResponse,
    BivariateConfig,
    TerritorySearchRequest,
    TerritorySearchResponse,
    POIResponse,
    CompetitorResponse,
    TerritoryStatsResponse
)
from src.api.v2.territories.services import TerritoriesService

# ==========================================
# ROUTER SETUP
# ==========================================

router = APIRouter(
    prefix="/api/v2/territories",
    tags=["territories", "explorer"],
    responses={
        404: {"description": "Territory not found"},
        403: {"description": "Insufficient permissions"}
    }
)

# Ініціалізуємо сервіс
territories_service = TerritoriesService()


# ==========================================
# ADMIN UNITS ENDPOINTS
# ==========================================

@router.get(
    "/admin/geometries/all",
    response_model=List[AdminUnitResponse],
    summary="Get all admin unit geometries",
    description="Returns geometries of all oblasts, raions and hromadas of Ukraine"
)
async def get_all_admin_geometries(
    level: Optional[str] = Query("all", description="Level: oblast, raion, hromada, all"),
    simplified: bool = Query(True, description="Simplified geometries for faster loading"),
    bounds: Optional[str] = Query(None, description="Bounding box: minLon,minLat,maxLon,maxLat"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units")),
    db: Session = Depends(get_db_session)
) -> List[AdminUnitResponse]:
    """
    Loads all admin units at app startup.
    Used for initial map initialization.
    """
    try:
        return await territories_service.get_admin_geometries(
            db=db,
            level=level,
            simplified=simplified,
            bounds=bounds
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/admin/metrics/all",
    response_model=List[AdminMetricsResponse],
    summary="Get metrics for all admin units",
    description="Returns calculated metrics and bivariate bins from ClickHouse"
)
async def get_all_admin_metrics(
    metric_x: str = Query("population", description="Metric for X axis"),
    metric_y: str = Query("income_index", description="Metric for Y axis"),
    normalize: bool = Query(True, description="Normalize metrics"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units")),
    db: Session = Depends(get_db_session)
) -> List[AdminMetricsResponse]:
    """
    Loads all metrics for admin units.
    Includes pre-calculated bivariate bins for fast visualization.
    """
    try:
        return await territories_service.get_admin_metrics(
            db=db,
            metric_x=metric_x,
            metric_y=metric_y,
            normalize=normalize
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/admin/{koatuu_code}",
    response_model=AdminUnitResponse,
    summary="Get single admin unit details"
)
async def get_admin_unit(
    koatuu_code: str = Path(..., description="KOATUU code of admin unit"),
    include_children: bool = Query(False, description="Include child units"),
    include_stats: bool = Query(True, description="Include statistics"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units")),
    db: Session = Depends(get_db_session)
) -> AdminUnitResponse:
    """Get detailed information about specific admin unit."""
    try:
        result = await territories_service.get_admin_unit_by_code(
            db=db,
            koatuu_code=koatuu_code,
            include_children=include_children,
            include_stats=include_stats
        )
        if not result:
            raise HTTPException(status_code=404, detail="Admin unit not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# H3 HEXAGON ENDPOINTS
# ==========================================

@router.get(
    "/h3/grid",
    response_model=List[H3GridResponse],
    summary="Get H3 grid for area",
    description="Returns H3 hexagons for specified area and resolution"
)
async def get_h3_grid(
    bounds: str = Query(..., description="Bounding box: minLon,minLat,maxLon,maxLat"),
    resolution: int = Query(7, ge=4, le=10, description="H3 resolution (4-10)"),
    metric: Optional[str] = Query(None, description="Metric to include"),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db_session)
) -> List[H3GridResponse]:
    """
    Get H3 hexagons for specified area.
    Higher resolutions require additional permissions.
    """
    # Перевірка дозволів для високих резолюцій
    if resolution > 8:
        _ = await require_permission("core.view_h3_detailed")(
            current_user=current_user, 
            db=db
        )
    else:
        _ = await require_permission("core.view_h3_basic")(
            current_user=current_user,
            db=db
        )
    
    try:
        return await territories_service.get_h3_grid(
            db=db,
            bounds=bounds,
            resolution=resolution,
            metric=metric
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/h3/{h3_index}",
    response_model=H3HexagonResponse,
    summary="Get H3 hexagon details"
)
async def get_h3_details(
    h3_index: str = Path(..., description="H3 index"),
    include_neighbors: bool = Query(False, description="Include neighbor hexagons"),
    include_poi: bool = Query(False, description="Include POI data"),
    include_competition: bool = Query(False, description="Include competition data"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_h3_detailed")),
    db: Session = Depends(get_db_session)
) -> H3HexagonResponse:
    """
    Get detailed information about specific H3 hexagon.
    """
    try:
        result = await territories_service.get_h3_details(
            db=db,
            h3_index=h3_index,
            include_neighbors=include_neighbors,
            include_poi=include_poi,
            include_competition=include_competition
        )
        if not result:
            raise HTTPException(status_code=404, detail="Hexagon not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# POI & COMPETITION ENDPOINTS  
# ==========================================

@router.get(
    "/poi/search",
    response_model=List[POIResponse],
    summary="Search POIs in area"
)
async def search_poi(
    bounds: Optional[str] = Query(None, description="Bounding box"),
    h3_index: Optional[str] = Query(None, description="H3 hexagon"),
    category: Optional[str] = Query(None, description="POI category"),
    limit: int = Query(100, le=500, description="Max results"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_h3_basic")),
    db: Session = Depends(get_db_session)
) -> List[POIResponse]:
    """Search for Points of Interest in specified area."""
    if not bounds and not h3_index:
        raise HTTPException(
            status_code=400,
            detail="Either bounds or h3_index must be provided"
        )
    
    try:
        return await territories_service.search_poi(
            db=db,
            bounds=bounds,
            h3_index=h3_index,
            category=category,
            limit=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/competition/nearby",
    response_model=List[CompetitorResponse],
    summary="Find nearby competitors"
)
async def find_competitors(
    lat: float = Query(..., description="Latitude"),
    lon: float = Query(..., description="Longitude"),
    radius_km: float = Query(5.0, description="Search radius in km"),
    format_type: Optional[str] = Query(None, description="Store format filter"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("competition.view_competitors")),
    db: Session = Depends(get_db_session)
) -> List[CompetitorResponse]:
    """Find competitor stores within radius."""
    try:
        return await territories_service.find_nearby_competitors(
            db=db,
            latitude=lat,
            longitude=lon,
            radius_km=radius_km,
            format_type=format_type
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# ANALYTICS & INSIGHTS ENDPOINTS
# ==========================================

@router.post(
    "/analyze",
    response_model=TerritoryStatsResponse,
    summary="Analyze territory potential"
)
async def analyze_territory(
    request: TerritorySearchRequest,
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_h3_detailed")),
    db: Session = Depends(get_db_session)
) -> TerritoryStatsResponse:
    """
    Complex analysis of territory potential.
    Combines multiple data sources and metrics.
    """
    try:
        return await territories_service.analyze_territory(
            db=db,
            bounds=request.bounds,
            h3_resolution=request.resolution,
            metrics=request.metrics
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/statistics/{koatuu_code}",
    response_model=TerritoryStatsResponse,
    summary="Get territory statistics"
)
async def get_territory_stats(
    koatuu_code: str = Path(..., description="KOATUU code"),
    period: Optional[str] = Query("month", description="Time period"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units")),
    db: Session = Depends(get_db_session)
) -> TerritoryStatsResponse:
    """Get aggregated statistics for territory."""
    try:
        return await territories_service.get_territory_statistics(
            db=db,
            koatuu_code=koatuu_code,
            period=period
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# CONFIGURATION ENDPOINTS
# ==========================================

@router.get(
    "/bivariate/config",
    response_model=BivariateConfig,
    summary="Get bivariate choropleth configuration"
)
async def get_bivariate_config(
    metric_x: str = Query("population", description="X axis metric"),
    metric_y: str = Query("income_index", description="Y axis metric"),
    bins: int = Query(3, ge=2, le=5, description="Number of bins per axis")
) -> BivariateConfig:
    """
    Get configuration for bivariate choropleth visualization.
    Includes color matrix and bin thresholds.
    """
    try:
        return territories_service.get_bivariate_config(
            metric_x=metric_x,
            metric_y=metric_y,
            bins=bins
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/metrics/available",
    response_model=Dict[str, Any],
    summary="Get list of available metrics"
)
async def get_available_metrics(
    category: Optional[str] = Query(None, description="Metric category")
) -> Dict[str, Any]:
    """Get list of all available metrics for visualization."""
    try:
        return territories_service.get_available_metrics(category=category)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# SEARCH ENDPOINTS
# ==========================================

@router.post(
    "/search",
    response_model=TerritorySearchResponse,
    summary="Advanced territory search"
)
async def search_territories(
    request: TerritorySearchRequest,
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units")),
    db: Session = Depends(get_db_session)
) -> TerritorySearchResponse:
    """
    Advanced search with multiple filters and criteria.
    Supports text search, metric filters, and spatial queries.
    """
    try:
        return await territories_service.search_territories(
            db=db,
            query=request.query,
            filters=request.filters,
            bounds=request.bounds,
            limit=request.limit,
            offset=request.offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# EXPORT ENDPOINTS
# ==========================================

@router.get(
    "/export/{format}",
    summary="Export territory data"
)
async def export_data(
    format: str = Path(..., regex="^(geojson|csv|xlsx)$", description="Export format"),
    koatuu_code: Optional[str] = Query(None, description="Territory code"),
    bounds: Optional[str] = Query(None, description="Bounding box"),
    metrics: Optional[str] = Query(None, description="Comma-separated metrics"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.export_data")),
    db: Session = Depends(get_db_session)
):
    """Export territory data in various formats."""
    try:
        return await territories_service.export_territory_data(
            db=db,
            format=format,
            koatuu_code=koatuu_code,
            bounds=bounds,
            metrics=metrics.split(",") if metrics else None
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# HEALTH CHECK
# ==========================================

@router.get(
    "/health",
    summary="Territory service health check"
)
async def health_check() -> Dict[str, str]:
    """Check if territory services are operational."""
    return {
        "status": "healthy",
        "service": "territories",
        "timestamp": datetime.utcnow().isoformat()
    }