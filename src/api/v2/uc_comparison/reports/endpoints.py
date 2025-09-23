"""
Reports Endpoints
Report generation
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import GenerateRequest, GenerateResponse, DownloadRequest, DownloadResponse, TemplatesRequest, TemplatesResponse
from .services import ReportsService

logger = logging.getLogger(__name__)

router = APIRouter()
service = ReportsService()


@router.get("/generate")
async def get_generate(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.generate"))
) -> GenerateResponse:
    """
    Get generate for reports
    
    Required permission: comparison.generate
    """
    try:
        result = await service.get_generate(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return GenerateResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_generate: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/download")
async def get_download(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.download"))
) -> DownloadResponse:
    """
    Get download for reports
    
    Required permission: comparison.download
    """
    try:
        result = await service.get_download(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return DownloadResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_download: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/templates")
async def get_templates(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.templates"))
) -> TemplatesResponse:
    """
    Get templates for reports
    
    Required permission: comparison.templates
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

