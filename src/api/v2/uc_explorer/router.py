"""
Uc Explorer Main Router
Aggregates all sub-routers for the use case
"""

from fastapi import APIRouter
import logging

from .map import endpoints as map_endpoints
from .layers import endpoints as layers_endpoints
from .metrics import endpoints as metrics_endpoints
from .details import endpoints as details_endpoints

logger = logging.getLogger(__name__)

# Create main router
router = APIRouter(
    prefix="/explorer",
    tags=['Explorer', 'Territory Discovery']
)

# Include sub-routers
router.include_router(map_endpoints.router, prefix="/map", tags=["map"])
router.include_router(layers_endpoints.router, prefix="/layers", tags=["layers"])
router.include_router(metrics_endpoints.router, prefix="/metrics", tags=["metrics"])
router.include_router(details_endpoints.router, prefix="/details", tags=["details"])

# Health check endpoint
@router.get("/health")
async def health_check():
    """Check if Uc Explorer endpoints are operational"""
    return {
        "status": "healthy",
        "use_case": "Uc Explorer",
        "version": "2.0.0"
    }
