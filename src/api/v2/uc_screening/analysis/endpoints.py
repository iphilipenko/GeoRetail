"""
Analysis Endpoints
Analysis and visualization
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import HeatmapRequest, HeatmapResponse, TopLocationsRequest, TopLocationsResponse, FilterRequest, FilterResponse
from .services import AnalysisService

logger = logging.getLogger(__name__)

router = APIRouter()
service = AnalysisService()


@router.get("/heatmap")
async def get_heatmap(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.heatmap"))
) -> HeatmapResponse:
    """
    Get heatmap for analysis
    
    Required permission: screening.heatmap
    """
    try:
        result = await service.get_heatmap(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return HeatmapResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_heatmap: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/top_locations")
async def get_top_locations(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.top_locations"))
) -> TopLocationsResponse:
    """
    Get top locations for analysis
    
    Required permission: screening.top_locations
    """
    try:
        result = await service.get_top_locations(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return TopLocationsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_top_locations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/filter")
async def get_filter(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.filter"))
) -> FilterResponse:
    """
    Get filter for analysis
    
    Required permission: screening.filter
    """
    try:
        result = await service.get_filter(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return FilterResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_filter: {e}")
        raise HTTPException(status_code=500, detail=str(e))

