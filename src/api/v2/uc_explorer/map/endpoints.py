"""
Map Endpoints
Map data loading and navigation
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import InitialLoadRequest, InitialLoadResponse, ViewportRequest, ViewportResponse, DrillDownRequest, DrillDownResponse
from .services import MapService

logger = logging.getLogger(__name__)

router = APIRouter()
service = MapService()


@router.get("/initial_load")
async def get_initial_load(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.initial_load"))
) -> InitialLoadResponse:
    """
    Get initial load for map
    
    Required permission: explorer.initial_load
    """
    try:
        result = await service.get_initial_load(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return InitialLoadResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_initial_load: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/viewport")
async def get_viewport(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.viewport"))
) -> ViewportResponse:
    """
    Get viewport for map
    
    Required permission: explorer.viewport
    """
    try:
        result = await service.get_viewport(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ViewportResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_viewport: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/drill_down")
async def get_drill_down(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.drill_down"))
) -> DrillDownResponse:
    """
    Get drill down for map
    
    Required permission: explorer.drill_down
    """
    try:
        result = await service.get_drill_down(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return DrillDownResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_drill_down: {e}")
        raise HTTPException(status_code=500, detail=str(e))

