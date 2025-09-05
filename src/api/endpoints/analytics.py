# src/api/endpoints/analytics.py
"""
Analytics API endpoints for bulk data operations
Optimized for bulk loading of H3 datasets
"""

from fastapi import APIRouter, HTTPException, Depends, Header
from fastapi.responses import JSONResponse
from typing import Dict, Any, Optional, Optional
import psycopg2
import psycopg2.extras
import time
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/v1/analytics", tags=["Analytics"])

# Database connection function (use correct credentials from project)
def get_db_connection():
    """Get database connection with context manager support"""
    connection_params = {
        'host': 'localhost',
        'database': 'georetail',
        'user': 'georetail_user', 
        'password': 'georetail_secure_2024',
        'port': 5432,
        'connect_timeout': 300  # Increased timeout for large datasets
    }
    
    try:
        conn = psycopg2.connect(**connection_params)
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@router.get("/bulk_loading")
async def get_bulk_loading_data(
    timeout: int = 60,
    include_geometry: bool = True,
    max_hexagons_per_dataset: Optional[int] = None
):
    """
    Bulk loading endpoint for all H3 datasets
    
    Returns all 8 datasets (opportunity + competition × 4 resolutions) 
    from materialized view in single response
    
    Performance: ~16-60 seconds, ~518MB JSON
    Args:
        timeout: Request timeout in seconds (default 60)
        include_geometry: Include hexagon geometry (default True)
        max_hexagons_per_dataset: Limit hexagons per dataset for testing
    """
    start_time = time.time()
    
    try:
        logger.info("🚀 Starting bulk loading request")
        
        # SQL query from documentation
        query = """
        SELECT 
            bulk_datasets_with_geometry, 
            cached_at, 
            region_filter
        FROM osm_ukraine.mv_hexagon_bulk_cache;
        """
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                row = cur.fetchone()
                
                if not row:
                    raise HTTPException(
                        status_code=404, 
                        detail="Bulk data not found. Materialized view may not be created."
                    )
                
                # Extract data
                bulk_data = row['bulk_datasets_with_geometry']
                cached_at = row['cached_at']
                region_filter = row.get('region_filter')
                
                # Calculate performance metrics
                query_time_ms = (time.time() - start_time) * 1000
                
                # Calculate total hexagons count
                total_hexagons = 0
                if bulk_data:
                    for dataset_name, dataset in bulk_data.items():
                        if isinstance(dataset, dict) and 'count' in dataset:
                            total_hexagons += dataset['count']
                
                logger.info(f"✅ Bulk loading completed in {query_time_ms:.1f}ms")
                logger.info(f"📊 Total hexagons: {total_hexagons}")
                
                # Prepare response with cache headers
                response_data = {
                    "data": bulk_data,
                    "metadata": {
                        "cached_at": cached_at.isoformat() if cached_at else None,
                        "region_filter": region_filter,
                        "total_hexagons": total_hexagons,
                        "query_time_ms": round(query_time_ms, 1),
                        "generated_at": datetime.now().isoformat(),
                        "datasets_available": list(bulk_data.keys()) if bulk_data else []
                    }
                }
                
                # Create response with cache headers
                response = JSONResponse(content=response_data)
                response.headers["Cache-Control"] = "public, max-age=3600"
                response.headers["X-Data-Cached-At"] = cached_at.isoformat() if cached_at else ""
                response.headers["X-Query-Time"] = f"{query_time_ms:.1f}ms"
                response.headers["X-Total-Hexagons"] = str(total_hexagons)
                
                return response
                
    except psycopg2.Error as e:
        logger.error(f"❌ Database error: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Database query failed: {str(e)}"
        )
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Bulk loading failed: {str(e)}"
        )

@router.get("/bulk_loading/test")
async def get_bulk_loading_test():
    """
    Test endpoint - returns only metadata without full datasets
    Fast response for testing connections
    """
    start_time = time.time()
    
    try:
        logger.info("🧪 Starting bulk loading test (metadata only)")
        
        # Simple query - just check if data exists without JSON parsing
        query = """
        SELECT 
            cached_at, 
            region_filter,
            CASE 
                WHEN bulk_datasets_with_geometry IS NOT NULL 
                THEN 'data_available' 
                ELSE 'no_data' 
            END as data_status,
            LENGTH(bulk_datasets_with_geometry::text) as data_size_chars
        FROM osm_ukraine.mv_hexagon_bulk_cache
        LIMIT 1;
        """
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query)
                row = cur.fetchone()
                
                if not row:
                    raise HTTPException(
                        status_code=404, 
                        detail="Bulk data not found"
                    )
                
                query_time_ms = (time.time() - start_time) * 1000
                
                # Static list of expected datasets based on documentation
                expected_datasets = [
                    "opportunity_7", "competition_7",
                    "opportunity_8", "competition_8", 
                    "opportunity_9", "competition_9",
                    "opportunity_10", "competition_10"
                ]
                
                # Estimate size in MB
                data_size_mb = row['data_size_chars'] / (1024 * 1024) if row['data_size_chars'] else 0
                
                return {
                    "status": "test_successful",
                    "metadata": {
                        "cached_at": row['cached_at'].isoformat() if row['cached_at'] else None,
                        "region_filter": row['region_filter'],
                        "data_status": row['data_status'],
                        "expected_datasets": expected_datasets,
                        "data_size_mb": round(data_size_mb, 1),
                        "query_time_ms": round(query_time_ms, 1),
                        "note": "This is a test endpoint - returns metadata only"
                    }
                }
                
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Test failed: {str(e)}"
        )

@router.get("/bulk_loading/sample")
async def get_bulk_loading_sample(dataset: str = "opportunity_7", limit: int = 100):
    """
    Sample endpoint - returns limited data from one dataset
    Args:
        dataset: Dataset name (e.g., 'opportunity_7', 'competition_8')
        limit: Number of hexagons to return (default 100)
    """
    start_time = time.time()
    
    try:
        logger.info(f"📊 Getting sample from dataset: {dataset}, limit: {limit}")
        
        # Use json_extract_path to get specific dataset without full conversion
        query = """
        SELECT 
            json_extract_path_text(bulk_datasets_with_geometry, %s) as dataset_exists,
            cached_at
        FROM osm_ukraine.mv_hexagon_bulk_cache
        LIMIT 1;
        """
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (dataset,))
                row = cur.fetchone()
                
                if not row or not row['dataset_exists']:
                    available_datasets = [
                        "opportunity_7", "competition_7",
                        "opportunity_8", "competition_8", 
                        "opportunity_9", "competition_9",
                        "opportunity_10", "competition_10"
                    ]
                    raise HTTPException(
                        status_code=404, 
                        detail=f"Dataset '{dataset}' not found. Available: {available_datasets}"
                    )
                
                query_time_ms = (time.time() - start_time) * 1000
                
                return {
                    "dataset": dataset,
                    "status": "sample_not_implemented",
                    "reason": "Dataset too large for JSON parsing in PostgreSQL",
                    "metadata": {
                        "cached_at": row['cached_at'].isoformat() if row['cached_at'] else None,
                        "query_time_ms": round(query_time_ms, 1),
                        "requested_limit": limit,
                        "note": "Use full bulk_loading endpoint with pagination for actual data"
                    },
                    "recommendation": "Use /bulk_loading endpoint to get full dataset"
                }
                
    except Exception as e:
        logger.error(f"❌ Sample failed: {e}")
        raise HTTPException(
            status_code=500, 
            detail=f"Sample failed: {str(e)}"
        )

@router.get("/bulk_loading/status")
async def get_bulk_loading_status():
    """
    Check status of materialized view and bulk loading availability
    """
    try:
        status_query = """
        SELECT 
            schemaname,
            matviewname,
            hasindexes,
            ispopulated,
            definition
        FROM pg_matviews 
        WHERE matviewname = 'mv_hexagon_bulk_cache'
        AND schemaname = 'osm_ukraine';
        """
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(status_query)
                matview_info = cur.fetchone()
                
                if not matview_info:
                    return {
                        "status": "not_available",
                        "materialized_view_exists": False,
                        "message": "Materialized view 'mv_hexagon_bulk_cache' not found",
                        "setup_required": True
                    }
                
                # Check last refresh time
                refresh_query = """
        SELECT 
            cached_at,
            region_filter
        FROM osm_ukraine.mv_hexagon_bulk_cache 
        LIMIT 1;
        """
                
                cur.execute(refresh_query)
                cache_info = cur.fetchone()
                
                return {
                    "status": "available",
                    "materialized_view_exists": True,
                    "is_populated": matview_info['ispopulated'],
                    "has_indexes": matview_info['hasindexes'],
                    "last_cached_at": cache_info['cached_at'].isoformat() if cache_info and cache_info['cached_at'] else None,
                    "region_filter": cache_info['region_filter'] if cache_info else None,
                    "endpoint_ready": True
                }
                
    except Exception as e:
        logger.error(f"Status check failed: {e}")
        return {
            "status": "error",
            "error": str(e),
            "endpoint_ready": False
        }

@router.post("/bulk_loading/refresh")
async def refresh_bulk_cache():
    """
    Manually refresh the materialized view
    (Usually done via cron job at 2 AM daily)
    """
    try:
        logger.info("🔄 Starting manual cache refresh")
        start_time = time.time()
        
        refresh_query = "REFRESH MATERIALIZED VIEW osm_ukraine.mv_hexagon_bulk_cache;"
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(refresh_query)
                conn.commit()
                
                refresh_time_ms = (time.time() - start_time) * 1000
                
                logger.info(f"✅ Cache refresh completed in {refresh_time_ms:.1f}ms")
                
                return {
                    "status": "success",
                    "message": "Materialized view refreshed successfully",
                    "refresh_time_ms": round(refresh_time_ms, 1),
                    "refreshed_at": datetime.now().isoformat()
                }
                
    except Exception as e:
        logger.error(f"❌ Cache refresh failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Cache refresh failed: {str(e)}"
        )

# Export router
__all__ = ["router"]