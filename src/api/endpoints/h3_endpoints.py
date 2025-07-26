"""
GeoRetail H3 Spatial Analysis API Endpoints
Provides H3-based location screening and spatial analysis
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import psycopg2
import os
from datetime import datetime
import json

# Pydantic models for API requests/responses
class LocationPoint(BaseModel):
    lat: float = Field(..., ge=-90, le=90, description="Latitude")
    lon: float = Field(..., ge=-180, le=180, description="Longitude")

class H3ScreeningRequest(BaseModel):
    location: LocationPoint
    radius_km: float = Field(default=1.0, ge=0.1, le=10.0, description="Analysis radius in kilometers")
    resolution: int = Field(default=8, ge=7, le=10, description="H3 resolution level")
    include_osm: bool = Field(default=True, description="Include OSM data analysis")
    include_demographics: bool = Field(default=True, description="Include demographics if available")

class H3GridRequest(BaseModel):
    min_lat: float = Field(..., ge=-90, le=90)
    min_lon: float = Field(..., ge=-180, le=180)
    max_lat: float = Field(..., ge=-90, le=90)
    max_lon: float = Field(..., ge=-180, le=180)
    resolution: int = Field(default=8, ge=7, le=10)

class StoreImportData(BaseModel):
    shop_number: int
    shop_name: str
    region: str
    locality: str
    opening_date: str
    lat: float
    lon: float
    format: str
    square_trade: Optional[float] = None
    square_total: Optional[float] = None
    monthly_revenue: Optional[float] = None
    # Add other fields as needed

# Router for H3 endpoints
router = APIRouter(prefix="/api/v1/h3", tags=["H3 Spatial Analysis"])

# Database connection helper
def get_db_connection():
    """Get PostgreSQL database connection"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            database=os.getenv("POSTGRES_DB", "georetail"),
            user=os.getenv("POSTGRES_USER", "georetail_user"),
            password=os.getenv("POSTGRES_PASSWORD", "georetail_secure_2024")
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

@router.get("/info")
async def h3_system_info():
    """Get H3 system information and capabilities"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check H3 extension
        cursor.execute("SELECT h3_get_extension_version();")
        h3_version = cursor.fetchone()[0]
        
        # Check available resolutions info
        resolutions_info = {}
        for res in [7, 8, 9, 10]:
            cursor.execute("SELECT h3_get_hexagon_area_avg(%s, 'km^2');", (res,))
            area = cursor.fetchone()[0]
            resolutions_info[res] = {
                "area_km2": round(area, 4),
                "description": {
                    7: "Strategic level - Regional analysis",
                    8: "Tactical level - Site selection", 
                    9: "Operational level - Micro-location",
                    10: "Detailed level - Precise analysis"
                }[res]
            }
        
        cursor.close()
        conn.close()
        
        return {
            "status": "available",
            "h3_version": h3_version,
            "resolutions": resolutions_info,
            "capabilities": [
                "Location to H3 conversion",
                "H3 grid generation",
                "Spatial screening",
                "Distance calculations",
                "Boundary analysis"
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"H3 system check failed: {str(e)}")

@router.post("/location-to-h3")
async def convert_location_to_h3(location: LocationPoint, resolution: int = 8):
    """Convert lat/lon coordinates to H3 index"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert to H3 for multiple resolutions
        h3_data = {}
        for res in [7, 8, 9]:
            cursor.execute(
                "SELECT h3_lat_lng_to_cell(POINT(%s, %s), %s);",
                (location.lon, location.lat, res)
            )
            h3_index = cursor.fetchone()[0]
            h3_data[f"resolution_{res}"] = h3_index
        
        # Get cell area for requested resolution
        cursor.execute("SELECT h3_cell_area(h3_lat_lng_to_cell(POINT(%s, %s), %s), 'km^2');", 
                      (location.lon, location.lat, resolution))
        cell_area = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "location": location.dict(),
            "h3_indices": h3_data,
            "primary_resolution": resolution,
            "primary_h3": h3_data[f"resolution_{resolution}"],
            "cell_area_km2": round(cell_area, 4)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"H3 conversion failed: {str(e)}")

@router.post("/screen-location")
async def screen_location(request: H3ScreeningRequest):
    """Comprehensive location screening using H3 spatial analysis"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get H3 index for location
        cursor.execute(
            "SELECT h3_lat_lng_to_cell(POINT(%s, %s), %s);",
            (request.location.lon, request.location.lat, request.resolution)
        )
        h3_index = cursor.fetchone()[0]
        
        # Get surrounding H3 cells within radius
        # Calculate k parameter for grid_disk based on radius
        cursor.execute("SELECT h3_get_hexagon_edge_length_avg(%s, 'km');", (request.resolution,))
        edge_length = cursor.fetchone()[0]
        k = max(1, int(request.radius_km / edge_length))
        
        cursor.execute(
            "SELECT h3_grid_disk(%s, %s);",
            (h3_index, k)
        )
        surrounding_cells = [row[0] for row in cursor.fetchall()]
        
        # Basic spatial analysis
        analysis_result = {
            "location": request.location.dict(),
            "analysis_parameters": {
                "radius_km": request.radius_km,
                "resolution": request.resolution,
                "h3_primary": h3_index,
                "surrounding_cells_count": len(surrounding_cells),
                "analysis_area_km2": len(surrounding_cells) * edge_length * edge_length * 2.598  # hexagon area factor
            },
            "h3_analysis": {
                "primary_cell": h3_index,
                "surrounding_cells": surrounding_cells[:10],  # Limit for response size
                "total_cells_analyzed": len(surrounding_cells)
            },
            "spatial_metrics": {
                "cell_edge_length_km": round(edge_length, 4),
                "coverage_radius_actual_km": round(k * edge_length, 2)
            }
        }
        
        # Add demographic analysis if requested
        if request.include_demographics:
            try:
                cursor.execute(
                    "SELECT COUNT(*), SUM(population_count) FROM demographics.h3_population WHERE hex_id = ANY(%s);",
                    (surrounding_cells,)
                )
                demo_result = cursor.fetchone()
                analysis_result["demographics"] = {
                    "cells_with_data": demo_result[0] or 0,
                    "total_population": demo_result[1] or 0,
                    "data_available": demo_result[0] > 0
                }
            except Exception:
                analysis_result["demographics"] = {
                    "status": "no_data_available",
                    "message": "Demographics data not yet imported"
                }
        
        # Add store analysis if data exists
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM georetail.stores WHERE h3_res%s = ANY(%s);",
                (request.resolution, surrounding_cells)
            )
            store_count = cursor.fetchone()[0]
            analysis_result["existing_stores"] = {
                "count_in_area": store_count,
                "data_available": store_count > 0
            }
        except Exception:
            analysis_result["existing_stores"] = {
                "status": "no_data_available",
                "message": "Store data not yet imported"
            }
        
        analysis_result["analysis_timestamp"] = datetime.now().isoformat()
        analysis_result["recommendations"] = [
            "Import demographics data for population analysis",
            "Import existing store data for competitive analysis", 
            "Integrate OSM data for POI analysis",
            "Setup TomTom traffic data for accessibility analysis"
        ]
        
        cursor.close()
        conn.close()
        
        return analysis_result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Location screening failed: {str(e)}")

@router.post("/generate-grid")
async def generate_h3_grid(request: H3GridRequest):
    """Generate H3 grid for specified bounding box"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Generate grid points
        grid_cells = []
        lat_step = 0.01  # Approximately 1km
        lon_step = 0.01
        
        lat = request.min_lat
        while lat <= request.max_lat:
            lon = request.min_lon
            while lon <= request.max_lon:
                cursor.execute(
                    "SELECT h3_lat_lng_to_cell(POINT(%s, %s), %s);",
                    (lon, lat, request.resolution)
                )
                h3_index = cursor.fetchone()[0]
                
                # Get cell center
                cursor.execute("SELECT h3_cell_to_lat_lng(%s);", (h3_index,))
                center = cursor.fetchone()[0]
                
                grid_cells.append({
                    "h3_index": h3_index,
                    "center_lat": float(center.x),
                    "center_lon": float(center.y),
                    "input_lat": lat,
                    "input_lon": lon
                })
                
                lon += lon_step
            lat += lat_step
        
        # Remove duplicates based on h3_index
        unique_cells = {}
        for cell in grid_cells:
            unique_cells[cell["h3_index"]] = cell
        
        cursor.close()
        conn.close()
        
        return {
            "bounding_box": request.dict(),
            "resolution": request.resolution,
            "total_unique_cells": len(unique_cells),
            "cells": list(unique_cells.values())[:100],  # Limit response size
            "note": f"Showing first 100 of {len(unique_cells)} unique H3 cells"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Grid generation failed: {str(e)}")

@router.get("/database-status")
async def get_database_status():
    """Get status of H3 database tables and data"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        status = {}
        
        # Check table counts
        tables = [
            ("h3_data.ukraine_grid", "H3 Grid"),
            ("h3_data.hexagon_metrics", "H3 Metrics"), 
            ("georetail.stores", "Stores"),
            ("demographics.h3_population", "Demographics"),
            ("osm_cache.osm_extracts", "OSM Cache"),
            ("traffic.tomtom_traffic", "Traffic Data")
        ]
        
        for table, name in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = cursor.fetchone()[0]
                status[name] = {"table": table, "count": count, "status": "available"}
            except Exception as e:
                status[name] = {"table": table, "count": 0, "status": "not_available", "error": str(e)}
        
        cursor.close()
        conn.close()
        
        return {
            "database_status": "connected",
            "tables": status,
            "ready_for_analysis": any(s["count"] > 0 for s in status.values()),
            "next_steps": [
                "Import demographics data (.gpkg files)",
                "Import store network data", 
                "Generate Ukraine H3 grid",
                "Setup OSM data integration"
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database status check failed: {str(e)}")

# Data import router
data_router = APIRouter(prefix="/api/v1/data", tags=["Data Management"])

@data_router.post("/stores/import")
async def import_store_data(stores: List[StoreImportData]):
    """Import store data with automatic H3 assignment"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        imported_count = 0
        errors = []
        
        for store in stores:
            try:
                # Calculate H3 indices for store location
                cursor.execute("""
                    INSERT INTO georetail.stores 
                    (shop_number, shop_name, region, locality, opening_date, lat, lon, format, 
                     square_trade, square_total, monthly_revenue, h3_res7, h3_res8, h3_res9)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            h3_lat_lng_to_cell(POINT(%s, %s), 7),
                            h3_lat_lng_to_cell(POINT(%s, %s), 8), 
                            h3_lat_lng_to_cell(POINT(%s, %s), 9))
                    ON CONFLICT (shop_number) DO UPDATE SET
                        shop_name = EXCLUDED.shop_name,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        h3_res7 = EXCLUDED.h3_res7,
                        h3_res8 = EXCLUDED.h3_res8,
                        h3_res9 = EXCLUDED.h3_res9,
                        updated_at = NOW();
                """, (
                    store.shop_number, store.shop_name, store.region, store.locality,
                    store.opening_date, store.lat, store.lon, store.format,
                    store.square_trade, store.square_total, store.monthly_revenue,
                    store.lon, store.lat,  # H3 res 7
                    store.lon, store.lat,  # H3 res 8
                    store.lon, store.lat   # H3 res 9
                ))
                imported_count += 1
                
            except Exception as e:
                errors.append(f"Store {store.shop_number}: {str(e)}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "status": "completed",
            "imported_count": imported_count,
            "total_submitted": len(stores),
            "errors": errors,
            "success_rate": f"{imported_count/len(stores)*100:.1f}%" if stores else "0%"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Store import failed: {str(e)}")

# Include both routers
def get_h3_router():
    """Get combined H3 and data management router"""
    combined_router = APIRouter()
    combined_router.include_router(router)
    combined_router.include_router(data_router)
    return combined_router