"""
Locations Endpoints
Location management for comparison
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import AddRequest, AddResponse, RemoveRequest, RemoveResponse, ListRequest, ListResponse
from .services import LocationsService

logger = logging.getLogger(__name__)

router = APIRouter()
service = LocationsService()


@router.get("/add")
async def get_add(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.add"))
) -> AddResponse:
    """
    Get add for locations
    
    Required permission: comparison.add
    """
    try:
        result = await service.get_add(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return AddResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_add: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/remove")
async def get_remove(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.remove"))
) -> RemoveResponse:
    """
    Get remove for locations
    
    Required permission: comparison.remove
    """
    try:
        result = await service.get_remove(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return RemoveResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_remove: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/list")
async def get_list(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.list"))
) -> ListResponse:
    """
    Get list for locations
    
    Required permission: comparison.list
    """
    try:
        result = await service.get_list(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ListResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_list: {e}")
        raise HTTPException(status_code=500, detail=str(e))

