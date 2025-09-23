"""
Common validators for UC modules
"""

from typing import Tuple
import re


def validate_bounds(bounds_str: str) -> Tuple[float, float, float, float]:
    """Validate and parse bounds string"""
    pattern = r'^-?\d+\.\d+,-?\d+\.\d+,-?\d+\.\d+,-?\d+\.\d+$'
    if not re.match(pattern, bounds_str):
        raise ValueError(f"Invalid bounds format: {bounds_str}")
    
    parts = list(map(float, bounds_str.split(',')))
    min_lon, min_lat, max_lon, max_lat = parts
    
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError(f"Longitude out of range: {min_lon}, {max_lon}")
    
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError(f"Latitude out of range: {min_lat}, {max_lat}")
    
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError(f"Invalid bounds: min >= max")
    
    return (min_lon, min_lat, max_lon, max_lat)


def validate_h3_index(h3_index: str) -> bool:
    """Validate H3 index format"""
    pattern = r'^[0-9a-f]{15}$'
    return bool(re.match(pattern, h3_index.lower()))


def validate_resolution(resolution: int) -> bool:
    """Validate H3 resolution"""
    return 4 <= resolution <= 15
