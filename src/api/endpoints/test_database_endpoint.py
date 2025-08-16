# src/api/endpoints/test_database_endpoint.py
"""
🧪 Додатковий endpoint для тестування бази даних
"""

from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, Union, Optional
import logging

# Налаштування логування
logger = logging.getLogger(__name__)

# Безпечний імпорт database service
try:
    from ..services.database_service import get_database_service, DatabaseService
    DATABASE_SERVICE_AVAILABLE = True
    logger.info("✅ DatabaseService imported successfully")
except ImportError as e:
    logger.warning(f"⚠️ DatabaseService import failed: {e}")
    DATABASE_SERVICE_AVAILABLE = False
    
    # Fallback клас для type hints
    class DatabaseService:
        """Placeholder class for type hints when database service is unavailable"""
        def test_connection(self) -> Dict[str, Any]:
            return {"status": "error", "message": "Database service not available"}
        
        def check_h3_exists(self, h3_index: str) -> bool:
            return False
            
        def get_h3_analytics(self, h3_index: str) -> Dict[str, Any]:
            return {"error": "Database service not available", "h3_index": h3_index}
            
        def get_poi_in_hexagon(self, h3_index: str) -> list:
            return []
    
    def get_database_service() -> DatabaseService:
        """Fallback function when database service is unavailable"""
        return DatabaseService()

# Створюємо router
router = APIRouter(prefix="/api/v1/database", tags=["Database Testing"])

# ==========================================
# DATABASE TESTING ENDPOINTS
# ==========================================

@router.get("/test-connection")
async def test_database_connection() -> Dict[str, Any]:
    """Test database connection and get basic info"""
    if not DATABASE_SERVICE_AVAILABLE:
        return {
            "status": "error",
            "message": "Database service not available",
            "error": "DatabaseService import failed",
            "available_endpoints": ["This endpoint only"],
            "recommendations": [
                "Check if database_service.py exists",
                "Verify PostgreSQL connection string",
                "Ensure psycopg2 is installed"
            ]
        }
    
    try:
        db_service = get_database_service()
        result = db_service.test_connection()
        
        # Додаємо додаткову інформацію про тест
        result.update({
            "test_timestamp": result.get("connection_time", "unknown"),
            "test_source": "test_database_endpoint",
            "database_service_status": "available"
        })
        
        return result
        
    except Exception as e:
        logger.error(f"Error in test_database_connection: {e}")
        return {
            "status": "error",
            "error": str(e),
            "error_type": type(e).__name__,
            "test_source": "test_database_endpoint",
            "database_service_status": "error"
        }

@router.get("/test-h3/{h3_index}")
async def test_h3_data(h3_index: str, resolution: Optional[int] = 10) -> Dict[str, Any]:
    """Test H3 data retrieval for specific hexagon"""
    if not DATABASE_SERVICE_AVAILABLE:
        return {
            "status": "error",
            "message": "Database service not available",
            "h3_index": h3_index,
            "error": "DatabaseService import failed"
        }
    
    try:
        db_service = get_database_service()
        
        # Тестуємо всі H3 методи
        result = {
            "h3_index": h3_index,
            "resolution": resolution,
            "test_timestamp": str(logger),  # Placeholder for datetime
            "tests": {
                "h3_exists": db_service.check_h3_exists(h3_index),
                "analytics": db_service.get_h3_analytics(h3_index, resolution),
                "poi_data": db_service.get_poi_in_hexagon(h3_index, resolution),
                "poi_count": len(db_service.get_poi_in_hexagon(h3_index, resolution))
            },
            "status": "success"
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error in test_h3_data for {h3_index}: {e}")
        return {
            "status": "error",
            "h3_index": h3_index,
            "error": str(e),
            "error_type": type(e).__name__
        }

@router.get("/test-competitive-analysis/{h3_index}")
async def test_competitive_analysis(
    h3_index: str, 
    radius_rings: Optional[int] = 2,
    resolution: Optional[int] = 10
) -> Dict[str, Any]:
    """Test competitive analysis for H3 hexagon"""
    if not DATABASE_SERVICE_AVAILABLE:
        return {
            "status": "error",
            "message": "Database service not available",
            "h3_index": h3_index,
            "error": "DatabaseService import failed"
        }
    
    try:
        db_service = get_database_service()
        
        # Тестуємо конкурентний аналіз
        analysis = db_service.get_competitive_analysis(
            h3_index=h3_index,
            radius_rings=radius_rings,
            resolution=resolution
        )
        
        return {
            "status": "success",
            "test_type": "competitive_analysis",
            "parameters": {
                "h3_index": h3_index,
                "radius_rings": radius_rings,
                "resolution": resolution
            },
            "analysis": analysis
        }
        
    except Exception as e:
        logger.error(f"Error in competitive analysis for {h3_index}: {e}")
        return {
            "status": "error",
            "h3_index": h3_index,
            "error": str(e),
            "error_type": type(e).__name__
        }

@router.get("/status")
async def database_service_status() -> Dict[str, Any]:
    """Get status of database service and its capabilities"""
    return {
        "database_service_available": DATABASE_SERVICE_AVAILABLE,
        "endpoints": [
            "/api/v1/database/test-connection",
            "/api/v1/database/test-h3/{h3_index}",
            "/api/v1/database/test-competitive-analysis/{h3_index}",
            "/api/v1/database/status"
        ],
        "capabilities": {
            "connection_testing": DATABASE_SERVICE_AVAILABLE,
            "h3_analytics": DATABASE_SERVICE_AVAILABLE,
            "poi_retrieval": DATABASE_SERVICE_AVAILABLE,
            "competitive_analysis": DATABASE_SERVICE_AVAILABLE
        },
        "requirements": [
            "PostgreSQL database",
            "psycopg2 library",
            "h3 library",
            "Proper connection string"
        ]
    }

# ==========================================
# ROUTER EXPORT
# ==========================================

# Експорт router для використання в main файлах
__all__ = ["router"]