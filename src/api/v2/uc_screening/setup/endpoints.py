"""
Setup Endpoints
Screening configuration
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import TemplatesRequest, TemplatesResponse, CriteriaRequest, CriteriaResponse, FiltersRequest, FiltersResponse
from .services import SetupService

logger = logging.getLogger(__name__)

router = APIRouter()
service = SetupService()


@router.get("/templates")
async def get_templates(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.templates"))
) -> TemplatesResponse:
    """
    Get templates for setup
    
    Required permission: screening.templates
    """
    try:
        result = await service.get_templates(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return TemplatesResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_templates: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/criteria")
async def get_criteria(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.criteria"))
) -> CriteriaResponse:
    """
    Get criteria for setup
    
    Required permission: screening.criteria
    """
    try:
        result = await service.get_criteria(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return CriteriaResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_criteria: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/filters")
async def get_filters(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.filters"))
) -> FiltersResponse:
    """
    Get filters for setup
    
    Required permission: screening.filters
    """
    try:
        result = await service.get_filters(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return FiltersResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_filters: {e}")
        raise HTTPException(status_code=500, detail=str(e))

