"""
Batch Endpoints
Batch processing operations
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import ScoreRequest, ScoreResponse, ProgressRequest, ProgressResponse, ResultsRequest, ResultsResponse
from .services import BatchService

logger = logging.getLogger(__name__)

router = APIRouter()
service = BatchService()


@router.get("/score")
async def get_score(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.score"))
) -> ScoreResponse:
    """
    Get score for batch
    
    Required permission: screening.score
    """
    try:
        result = await service.get_score(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ScoreResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_score: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/progress")
async def get_progress(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.progress"))
) -> ProgressResponse:
    """
    Get progress for batch
    
    Required permission: screening.progress
    """
    try:
        result = await service.get_progress(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ProgressResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_progress: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/results")
async def get_results(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("screening.results"))
) -> ResultsResponse:
    """
    Get results for batch
    
    Required permission: screening.results
    """
    try:
        result = await service.get_results(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ResultsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_results: {e}")
        raise HTTPException(status_code=500, detail=str(e))

