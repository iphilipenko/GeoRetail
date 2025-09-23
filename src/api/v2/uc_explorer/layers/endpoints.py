"""
Layers Endpoints
Map layers management
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import AdminUnitsRequest, AdminUnitsResponse, HexagonsRequest, HexagonsResponse, PoiRequest, PoiResponse, CompetitorsRequest, CompetitorsResponse
from .services import LayersService

logger = logging.getLogger(__name__)

router = APIRouter()
service = LayersService()


@router.get("/admin_units")
async def get_admin_units(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.admin_units"))
) -> AdminUnitsResponse:
    """
    Get admin units for layers
    
    Required permission: explorer.admin_units
    """
    try:
        result = await service.get_admin_units(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return AdminUnitsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_admin_units: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/hexagons")
async def get_hexagons(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.hexagons"))
) -> HexagonsResponse:
    """
    Get hexagons for layers
    
    Required permission: explorer.hexagons
    """
    try:
        result = await service.get_hexagons(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return HexagonsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_hexagons: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/poi")
async def get_poi(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.poi"))
) -> PoiResponse:
    """
    Get poi for layers
    
    Required permission: explorer.poi
    """
    try:
        result = await service.get_poi(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return PoiResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_poi: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/competitors")
async def get_competitors(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("explorer.competitors"))
) -> CompetitorsResponse:
    """
    Get competitors for layers
    
    Required permission: explorer.competitors
    """
    try:
        result = await service.get_competitors(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return CompetitorsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_competitors: {e}")
        raise HTTPException(status_code=500, detail=str(e))

