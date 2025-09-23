"""
Metrics Endpoints
Metrics calculation and configuration
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import BivariateRequest, BivariateResponse, AvailableRequest, AvailableResponse, CalculateRequest, CalculateResponse
from .services import MetricsService

logger = logging.getLogger(__name__)

router = APIRouter()
service = MetricsService()


@router.get("/bivariate")
async def get_bivariate(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.bivariate"))
) -> BivariateResponse:
    """
    Get bivariate for metrics
    
    Required permission: explorer.bivariate
    """
    try:
        result = await service.get_bivariate(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return BivariateResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_bivariate: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/available")
async def get_available(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.available"))
) -> AvailableResponse:
    """
    Get available for metrics
    
    Required permission: explorer.available
    """
    try:
        result = await service.get_available(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return AvailableResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_available: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/calculate")
async def get_calculate(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.calculate"))
) -> CalculateResponse:
    """
    Get calculate for metrics
    
    Required permission: explorer.calculate
    """
    try:
        result = await service.get_calculate(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return CalculateResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_calculate: {e}")
        raise HTTPException(status_code=500, detail=str(e))

