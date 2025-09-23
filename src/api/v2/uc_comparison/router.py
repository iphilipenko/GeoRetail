"""
Uc Comparison Main Router
Aggregates all sub-routers for the use case
"""

from fastapi import APIRouter
import logging

from .locations import endpoints as locations_endpoints
from .analysis import endpoints as analysis_endpoints
from .ml import endpoints as ml_endpoints
from .reports import endpoints as reports_endpoints

logger = logging.getLogger(__name__)

# Create main router
router = APIRouter(
    prefix="/comparison",
    tags=['Comparison', 'Decision Support']
)

# Include sub-routers
router.include_router(locations_endpoints.router, prefix="/locations", tags=["locations"])
router.include_router(analysis_endpoints.router, prefix="/analysis", tags=["analysis"])
router.include_router(ml_endpoints.router, prefix="/ml", tags=["ml"])
router.include_router(reports_endpoints.router, prefix="/reports", tags=["reports"])

# Health check endpoint
@router.get("/health")
async def health_check():
    """Check if Uc Comparison endpoints are operational"""
    return {
        "status": "healthy",
        "use_case": "Uc Comparison",
        "version": "2.0.0"
    }
