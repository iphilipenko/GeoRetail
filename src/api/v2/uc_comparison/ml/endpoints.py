"""
Ml Endpoints
Machine learning predictions
"""

from fastapi import APIRouter, Depends, Query, Path, Body, HTTPException
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
import logging

from src.core.rbac_database import get_db
from src.api.v2.core.dependencies import get_current_user, require_permission
from ..schemas import PredictRevenueRequest, PredictRevenueResponse, ConfidenceScoresRequest, ConfidenceScoresResponse, SimilarLocationsRequest, SimilarLocationsResponse
from .services import MlService

logger = logging.getLogger(__name__)

router = APIRouter()
service = MlService()


@router.get("/predict_revenue")
async def get_predict_revenue(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.predict_revenue"))
) -> PredictRevenueResponse:
    """
    Get predict revenue for ml
    
    Required permission: comparison.predict_revenue
    """
    try:
        result = await service.get_predict_revenue(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return PredictRevenueResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_predict_revenue: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/confidence_scores")
async def get_confidence_scores(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.confidence_scores"))
) -> ConfidenceScoresResponse:
    """
    Get confidence scores for ml
    
    Required permission: comparison.confidence_scores
    """
    try:
        result = await service.get_confidence_scores(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return ConfidenceScoresResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_confidence_scores: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/similar_locations")
async def get_similar_locations(
    # Path and query parameters
    limit: int = Query(100, ge=1, le=1000, description="Maximum items to return"),
    offset: int = Query(0, ge=0, description="Number of items to skip"),
    
    # Dependencies
    db: Session = Depends(get_db),
    current_user = Depends(get_current_user),
    _: Any = Depends(require_permission("comparison.similar_locations"))
) -> SimilarLocationsResponse:
    """
    Get similar locations for ml
    
    Required permission: comparison.similar_locations
    """
    try:
        result = await service.get_similar_locations(
            db=db,
            user_id=current_user.id,
            limit=limit,
            offset=offset
        )
        return SimilarLocationsResponse(
            success=True,
            data=result,
            total=len(result)
        )
    except Exception as e:
        logger.error(f"Error in get_similar_locations: {e}")
        raise HTTPException(status_code=500, detail=str(e))

