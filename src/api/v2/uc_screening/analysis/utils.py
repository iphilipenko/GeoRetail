"""
Analysis Utility Functions
Helper functions for Analysis and visualization
"""

from typing import List, Dict, Any, Optional, Tuple
import logging
import hashlib
import json
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def calculate_cache_key(*args, **kwargs) -> str:
    """Generate cache key from arguments"""
    key_data = {
        "args": args,
        "kwargs": kwargs
    }
    key_string = json.dumps(key_data, sort_keys=True, default=str)
    return hashlib.md5(key_string.encode()).hexdigest()


def parse_bounds(bounds_str: str) -> Tuple[float, float, float, float]:
    """Parse boundary string 'minLon,minLat,maxLon,maxLat'"""
    try:
        parts = bounds_str.split(',')
        if len(parts) != 4:
            raise ValueError("Bounds must have 4 coordinates")
        return tuple(map(float, parts))
    except (ValueError, AttributeError) as e:
        logger.error(f"Invalid bounds format: {bounds_str}")
        raise ValueError(f"Invalid bounds format: {e}")


def format_response(
    data: Any,
    success: bool = True,
    message: Optional[str] = None,
    metadata: Optional[Dict] = None
) -> Dict[str, Any]:
    """Format standardized API response"""
    response = {
        "success": success,
        "data": data,
        "timestamp": datetime.utcnow().isoformat(),
    }
    
    if message:
        response["message"] = message
    
    if metadata:
        response["metadata"] = metadata
    
    return response


# TODO: Add more utility functions as needed
