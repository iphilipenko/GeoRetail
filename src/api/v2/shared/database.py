"""
Database utilities for UC modules
Connection pooling and query helpers
"""

from typing import Dict, List, Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
import logging

logger = logging.getLogger(__name__)


class DatabaseHelper:
    """Database query helpers"""
    
    @staticmethod
    def execute_query(
        db: Session,
        query: str,
        params: Dict = None
    ) -> List[Dict]:
        """Execute query and return results as dict list"""
        try:
            result = db.execute(text(query), params or {})
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise
    
    @staticmethod
    def get_admin_units(
        db: Session,
        level: Optional[str] = None,
        parent_id: Optional[int] = None
    ) -> List[Dict]:
        """Get administrative units"""
        query = """
            SELECT 
                id,
                name_uk,
                name_en,
                admin_level,
                parent_id,
                ST_AsGeoJSON(geometry) as geometry
            FROM osm_ukraine.admin_boundaries
            WHERE 1=1
        """
        
        params = {}
        if level:
            query += " AND admin_level = :level"
            params["level"] = level
        
        if parent_id:
            query += " AND parent_id = :parent_id"
            params["parent_id"] = parent_id
        
        return DatabaseHelper.execute_query(db, query, params)
    
    @staticmethod
    def get_hexagons_in_bounds(
        db: Session,
        bounds: tuple,
        resolution: int
    ) -> List[Dict]:
        """Get H3 hexagons within bounds"""
        query = """
            SELECT 
                h3_index,
                resolution,
                ST_X(center_point) as lon,
                ST_Y(center_point) as lat
            FROM h3_data.h3_grid
            WHERE resolution = :resolution
            AND ST_Intersects(
                geom,
                ST_MakeEnvelope(:min_lon, :min_lat, :max_lon, :max_lat, 4326)
            )
        """
        
        params = {
            "resolution": resolution,
            "min_lon": bounds[0],
            "min_lat": bounds[1],
            "max_lon": bounds[2],
            "max_lat": bounds[3]
        }
        
        return DatabaseHelper.execute_query(db, query, params)
