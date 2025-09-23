"""
Uc Screening Main Router
Aggregates all sub-routers for the use case
"""

from fastapi import APIRouter
import logging

from .setup import endpoints as setup_endpoints
from .batch import endpoints as batch_endpoints
from .analysis import endpoints as analysis_endpoints
from .export import endpoints as export_endpoints

logger = logging.getLogger(__name__)

# Create main router
router = APIRouter(
    prefix="/screening",
    tags=['Screening', 'Batch Processing']
)

# Include sub-routers
router.include_router(setup_endpoints.router, prefix="/setup", tags=["setup"])
router.include_router(batch_endpoints.router, prefix="/batch", tags=["batch"])
router.include_router(analysis_endpoints.router, prefix="/analysis", tags=["analysis"])
router.include_router(export_endpoints.router, prefix="/export", tags=["export"])

# Health check endpoint
@router.get("/health")
async def health_check():
    """Check if Uc Screening endpoints are operational"""
    return {
        "status": "healthy",
        "use_case": "Uc Screening",
        "version": "2.0.0"
    }
