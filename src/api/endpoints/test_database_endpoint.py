# test_database_endpoint.py
"""
🧪 Додатковий endpoint для тестування бази даних
"""

from fastapi import APIRouter, Depends
from typing import Dict, Any, Union

# Безпечний імпорт database service
try:
    from api.services.database_service import get_database_service, DatabaseService
except ImportError:
    # Fallback якщо database service недоступний
    class DatabaseService:
        """Placeholder class for type hints when database service is unavailable"""
        pass
    
    def get_database_service():
        return None

router = APIRouter(prefix="/api/v1/database", tags=["Database Testing"])

@router.get("/test-connection")
async def test_database_connection(db: Union[DatabaseService, None] = Depends(get_database_service)) -> Dict[str, Any]:
    """Test database connection and get basic info"""
    if db is None:
        return {
            "status": "error",
            "message": "Database service not available",
            "error": "DatabaseService import failed"
        }
    return db.test_connection()

@router.get("/test-h3/{h3_index}")
async def test_h3_data(h3_index: str, db: Union[DatabaseService, None] = Depends(get_database_service)) -> Dict[str, Any]:
    """Test H3 data retrieval"""
    if db is None:
        return {
            "status": "error",
            "message": "Database service not available",
            "h3_index": h3_index
        }
    
    return {
        "h3_index": h3_index,
        "exists": db.check_h3_exists(h3_index),
        "analytics": db.get_h3_analytics(h3_index),
        "poi_count": len(db.get_poi_in_hexagon(h3_index))
    }

# Експорт router
__all__ = ["router"]
