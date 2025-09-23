"""
Analysis Endpoints
Comparative analysis
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import SpiderChartRequest, SpiderChartResponse, SideBySideRequest, SideBySideResponse, CannibalizationRequest, CannibalizationResponse, RoiForecastRequest, RoiForecastResponse
from .services import AnalysisService

logger = logging.getLogger(__name__)

router = APIRouter()
service = AnalysisService()


@router.get("/spider_chart")
async def get_spider_chart(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.spider_chart"))
) -> SpiderChartResponse:
    """
    Get spider chart for analysis
    
    Required permission: comparison.spider_chart
    """
    try:
        result = await service.get_spider_chart(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return SpiderChartResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_spider_chart: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/side_by_side")
async def get_side_by_side(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.side_by_side"))
) -> SideBySideResponse:
    """
    Get side by side for analysis
    
    Required permission: comparison.side_by_side
    """
    try:
        result = await service.get_side_by_side(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return SideBySideResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_side_by_side: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/cannibalization")
async def get_cannibalization(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.cannibalization"))
) -> CannibalizationResponse:
    """
    Get cannibalization for analysis
    
    Required permission: comparison.cannibalization
    """
    try:
        result = await service.get_cannibalization(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return CannibalizationResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_cannibalization: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/roi_forecast")
async def get_roi_forecast(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.roi_forecast"))
) -> RoiForecastResponse:
    """
    Get roi forecast for analysis
    
    Required permission: comparison.roi_forecast
    """
    try:
        result = await service.get_roi_forecast(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return RoiForecastResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_roi_forecast: {e}")
        raise HTTPException(status_code=500, detail=str(e))

