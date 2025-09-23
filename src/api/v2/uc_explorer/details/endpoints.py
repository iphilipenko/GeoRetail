"""
Details Endpoints
Detailed information retrieval
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import TerritoryRequest, TerritoryResponse, HexagonRequest, HexagonResponse, StatisticsRequest, StatisticsResponse
from .services import DetailsService

logger = logging.getLogger(__name__)

router = APIRouter()
service = DetailsService()


@router.get("/territory")
async def get_territory(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.territory"))
) -> TerritoryResponse:
    """
    Get territory for details
    
    Required permission: explorer.territory
    """
    try:
        result = await service.get_territory(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return TerritoryResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_territory: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hexagon")
async def get_hexagon(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.hexagon"))
) -> HexagonResponse:
    """
    Get hexagon for details
    
    Required permission: explorer.hexagon
    """
    try:
        result = await service.get_hexagon(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return HexagonResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_hexagon: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/statistics")
async def get_statistics(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.statistics"))
) -> StatisticsResponse:
    """
    Get statistics for details
    
    Required permission: explorer.statistics
    """
    try:
        result = await service.get_statistics(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return StatisticsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))

