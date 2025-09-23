"""
Response formatting utilities
"""

from typing import Any, Dict, Optional, List
from datetime import datetime


def success_response(
    data: Any,
    message: Optional[str] = None,
    metadata: Optional[Dict] = None,
    total: Optional[int] = None
) -> Dict:
    """Format success response"""
    response = {
        "success": True,
        "data": data,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if message:
        response["message"] = message
    
    if metadata:
        response["metadata"] = metadata
    
    if total is not None:
        response["total"] = total
    
    return response


def error_response(
    error: str,
    code: Optional[str] = None,
    details: Optional[Dict] = None
) -> Dict:
    """Format error response"""
    response = {
        "success": False,
        "error": error,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if code:
        response["code"] = code
    
    if details:
        response["details"] = details
    
    return response


def paginated_response(
    items: List[Any],
    page: int,
    limit: int,
    total: int
) -> Dict:
    """Format paginated response"""
    return {
        "success": True,
        "data": items,
        "pagination": {
            "page": page,
            "limit": limit,
            "total": total,
            "pages": (total + limit - 1) // limit
        },
        "timestamp": datetime.utcnow().isoformat()
    }
