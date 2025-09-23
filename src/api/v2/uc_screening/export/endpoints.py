"""
Export Endpoints
Export and project management
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import ShortlistRequest, ShortlistResponse, AddToProjectRequest, AddToProjectResponse
from .services import ExportService

logger = logging.getLogger(__name__)

router = APIRouter()
service = ExportService()


@router.get("/shortlist")
async def get_shortlist(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.shortlist"))
) -> ShortlistResponse:
    """
    Get shortlist for export
    
    Required permission: screening.shortlist
    """
    try:
        result = await service.get_shortlist(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ShortlistResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_shortlist: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/add_to_project")
async def get_add_to_project(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.add_to_project"))
) -> AddToProjectResponse:
    """
    Get add to project for export
    
    Required permission: screening.add_to_project
    """
    try:
        result = await service.get_add_to_project(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return AddToProjectResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_add_to_project: {e}")
        raise HTTPException(status_code=500, detail=str(e))

