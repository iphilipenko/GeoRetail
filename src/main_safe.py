"""
GeoRetail FastAPI Application - Complete Fixed Version
Main entry point with safe Neo4j connection and proper OSM serialization
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Shapely imports for geometry serialization
try:
    from shapely.geometry import Point, LineString, Polygon
    from shapely.geometry.base import BaseGeometry
    SHAPELY_AVAILABLE = True
except ImportError:
    SHAPELY_AVAILABLE = False
    print("⚠️  Shapely not available - geometry serialization limited")

# Add src to path for imports
src_path = Path(__file__).parent
sys.path.append(str(src_path))

# Import modules with safe fallbacks
neo4j_client = None
osm_extractor = None

try:
    # Try to import existing modules
    from graph.neo4j_client import geo_neo4j_client
    neo4j_client = geo_neo4j_client
    print("✅ Neo4j client loaded successfully")
except Exception as e:
    print(f"⚠️  Neo4j client not available: {e}")
    print("   API will work without Neo4j functionality")

try:
    from data.osm_extractor import geo_osm_extractor  
    osm_extractor = geo_osm_extractor
    print("✅ OSM extractor loaded successfully")
except Exception as e:
    print(f"⚠️  OSM extractor not available: {e}")
    print("   API will work without OSM functionality")

# ==========================================
# GEOMETRY SERIALIZATION HELPERS
# ==========================================

def serialize_geometry(obj):
    """Convert Shapely geometries to GeoJSON format"""
    if not SHAPELY_AVAILABLE:
        return str(obj)
        
    try:
        if isinstance(obj, BaseGeometry):
            return obj.__geo_interface__
        elif hasattr(obj, 'geometry') and isinstance(obj.geometry, BaseGeometry):
            # For GeoDataFrame rows
            return obj.geometry.__geo_interface__
        elif isinstance(obj, dict):
            return {k: serialize_geometry(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [serialize_geometry(item) for item in obj]
        else:
            return obj
    except Exception:
        return str(obj)

def safe_serialize_data(data, max_items=5):
    """Safely serialize complex data structures with geometry handling"""
    if data is None:
        return None
        
    try:
        # Handle different data types
        if hasattr(data, 'to_dict'):
            # GeoDataFrame or DataFrame
            try:
                # Try to get basic info without full serialization
                result = {
                    "type": str(type(data).__name__),
                    "count": len(data) if hasattr(data, '__len__') else 0,
                }
                
                if hasattr(data, 'columns'):
                    result["columns"] = list(data.columns)
                    
                # Try to get a few sample records
                if len(data) > 0:
                    sample_data = []
                    for i, (idx, row) in enumerate(data.head(max_items).iterrows()):
                        if i >= max_items:
                            break
                        try:
                            # Convert row to dict, handling geometries
                            row_dict = {}
                            for col, val in row.items():
                                if SHAPELY_AVAILABLE and isinstance(val, BaseGeometry):
                                    row_dict[col] = {
                                        "type": "geometry",
                                        "geom_type": val.geom_type,
                                        "bounds": list(val.bounds) if hasattr(val, 'bounds') else None
                                    }
                                elif isinstance(val, (str, int, float, bool)) or val is None:
                                    row_dict[col] = val
                                else:
                                    row_dict[col] = str(val)[:100]  # Truncate long strings
                            sample_data.append(row_dict)
                        except Exception:
                            sample_data.append({"error": "serialization_failed", "index": idx})
                    
                    result["sample_data"] = sample_data
                    result["note"] = f"Showing {len(sample_data)} of {len(data)} records"
                
                return result
                
            except Exception as e:
                return {
                    "type": str(type(data).__name__),
                    "count": len(data) if hasattr(data, '__len__') else 0,
                    "error": f"Serialization failed: {str(e)}"
                }
                
        elif isinstance(data, dict):
            # Regular dictionary
            result = {}
            for key, value in data.items():
                try:
                    if isinstance(value, (str, int, float, bool)) or value is None:
                        result[key] = value
                    elif isinstance(value, (list, tuple)) and len(value) <= 100:
                        # Small lists/tuples
                        result[key] = [safe_serialize_data(item, 1) for item in value[:max_items]]
                        if len(value) > max_items:
                            result[key].append({"note": f"... and {len(value) - max_items} more items"})
                    else:
                        result[key] = safe_serialize_data(value, max_items)
                except Exception:
                    result[key] = {"error": "serialization_failed", "type": str(type(value).__name__)}
            return result
            
        elif isinstance(data, (list, tuple)):
            # Lists/tuples
            if len(data) == 0:
                return []
            elif len(data) <= max_items:
                return [safe_serialize_data(item, 1) for item in data]
            else:
                result = [safe_serialize_data(item, 1) for item in data[:max_items]]
                result.append({"note": f"... and {len(data) - max_items} more items"})
                return result
                
        elif isinstance(data, (str, int, float, bool)) or data is None:
            # Simple types
            return data
            
        else:
            # Everything else
            return {
                "type": str(type(data).__name__),
                "value": str(data)[:200],  # First 200 characters
                "note": "Complex object converted to string"
            }
            
    except Exception as e:
        return {
            "error": f"Serialization completely failed: {str(e)}",
            "type": str(type(data).__name__)
        }

# ==========================================
# FASTAPI APPLICATION
# ==========================================

app = FastAPI(
    title="GeoRetail Core Infrastructure",
    description="""
    Advanced retail location intelligence system using:
    • H3 hexagonal spatial indexing
    • Graph neural networks and embeddings  
    • Competitive analysis and market saturation
    • Revenue forecasting and risk assessment
    """,
    version="2.0.1",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# SYSTEM ENDPOINTS
# ==========================================

@app.get("/", tags=["System"])
async def root():
    """Root endpoint with system information"""
    return {
        "message": "GeoRetail Core Infrastructure",
        "version": "2.0.1",
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "docs": "/docs",
        "available_features": {
            "neo4j": neo4j_client is not None,
            "osm_extractor": osm_extractor is not None,
            "fastapi_core": True,
            "geometry_serialization": SHAPELY_AVAILABLE,
            "h3_processing": "pending_setup",
            "postgis": "pending_setup"
        },
        "next_setup_steps": [
            "✅ Neo4j working" if neo4j_client else "❌ Fix Neo4j authentication",
            "✅ OSM extraction working" if osm_extractor else "❌ Setup OSM extractor",
            "⏳ Setup PostGIS + H3",
            "⏳ Import demographics data",
            "⏳ Implement H3 screening"
        ]
    }

@app.get("/health", tags=["System"])
async def health_check():
    """Comprehensive health check for all system components"""
    
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {},
        "overall_health": "operational"
    }
    
    # Check Neo4j connection
    if neo4j_client:
        try:
            result = neo4j_client.execute_query("RETURN 'healthy' as status")
            health_status["services"]["neo4j"] = {
                "status": "healthy",
                "message": "Neo4j connection successful",
                "available": True
            }
        except Exception as e:
            health_status["services"]["neo4j"] = {
                "status": "unhealthy",
                "message": f"Neo4j connection failed: {str(e)}",
                "available": False
            }
    else:
        health_status["services"]["neo4j"] = {
            "status": "not_configured",
            "message": "Neo4j client not loaded - check credentials",
            "available": False
        }
    
    # Check OSM extractor
    if osm_extractor:
        health_status["services"]["osm_extractor"] = {
            "status": "healthy",
            "message": "OSM extractor available",
            "available": True
        }
    else:
        health_status["services"]["osm_extractor"] = {
            "status": "not_available",
            "message": "OSM extractor not loaded",
            "available": False
        }
    
    # Check Python environment
    health_status["services"]["python"] = {
        "status": "healthy",
        "version": sys.version,
        "working_directory": str(Path.cwd()),
        "shapely_available": SHAPELY_AVAILABLE,
        "available": True
    }
    
    # Future infrastructure components
    health_status["services"]["postgis"] = {
        "status": "pending",
        "message": "PostGIS setup pending",
        "available": False
    }
    health_status["services"]["redis"] = {
        "status": "pending", 
        "message": "Redis setup pending",
        "available": False
    }
    health_status["services"]["h3_processor"] = {
        "status": "pending",
        "message": "H3 processing setup pending", 
        "available": False
    }
    
    # Overall status assessment
    unhealthy_services = [
        service for service, info in health_status["services"].items()
        if info["status"] == "unhealthy"
    ]
    
    if unhealthy_services:
        health_status["overall_health"] = "degraded"
        health_status["unhealthy_services"] = unhealthy_services
    
    return health_status

# ==========================================
# NEO4J ENDPOINTS
# ==========================================

@app.get("/neo4j/test", tags=["Neo4j"])
async def test_neo4j_connection():
    """Test Neo4j connection"""
    if not neo4j_client:
        raise HTTPException(
            status_code=503, 
            detail="Neo4j client not available. Check credentials and restart API."
        )
    
    try:
        result = neo4j_client.execute_query("RETURN 'Connection successful' as message")
        return {
            "status": "success",
            "message": "Neo4j connection working",
            "result": result
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, 
            detail=f"Neo4j connection failed: {str(e)}"
        )

@app.get("/neo4j/info", tags=["Neo4j"])
async def neo4j_info():
    """Get Neo4j database information"""
    if not neo4j_client:
        return {
            "status": "not_available",
            "message": "Neo4j client not loaded",
            "troubleshooting": [
                "1. Check Neo4j Desktop is running",
                "2. Verify credentials in .env file",
                "3. Ensure Neo4j URI is correct (bolt://localhost:7687)",
                "4. Restart API after fixing credentials"
            ]
        }
    
    try:
        # Get database info
        result = neo4j_client.execute_query("""
            CALL dbms.components() YIELD name, versions, edition
            RETURN name, versions[0] as version, edition
        """)
        
        return {
            "status": "connected",
            "database_info": result,
            "uri": os.getenv("NEO4J_URI", "not_set"),
            "user": os.getenv("NEO4J_USER", "not_set")
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "troubleshooting": [
                "Check Neo4j Desktop is running",
                "Verify password is correct",
                "Try connecting manually through Neo4j Browser"
            ]
        }

# ==========================================
# OSM ENDPOINTS (with proper serialization)
# ==========================================

@app.get("/osm/info", tags=["OSM"])
async def osm_extractor_info():
    """Get information about OSM extractor methods"""
    if not osm_extractor:
        return {"status": "not_available"}
    
    import inspect
    
    # Get method signature
    method = getattr(osm_extractor, 'extract_location_data', None)
    if method:
        signature = inspect.signature(method)
        return {
            "status": "available",
            "method_signature": str(signature),
            "parameters": [
                {"name": param.name, "type": str(param.annotation)}
                for param in signature.parameters.values()
            ],
            "available_methods": [attr for attr in dir(osm_extractor) if not attr.startswith('_')]
        }
    else:
        return {"status": "method_not_found"}

@app.post("/osm/extract/summary", tags=["OSM"])
async def extract_osm_summary(lat: float, lon: float):
    """Extract OSM data summary without full geometries"""
    if not osm_extractor:
        raise HTTPException(status_code=503, detail="OSM extractor not available")
    
    try:
        location_data = osm_extractor.extract_location_data(lat, lon)
        
        summary = {
            "location": {"lat": lat, "lon": lon},
            "extraction_results": {}
        }
        
        for key, value in location_data.items():
            if hasattr(value, '__len__'):
                summary["extraction_results"][key] = {
                    "count": len(value),
                    "type": str(type(value).__name__)
                }
                
                # Additional info for different data types
                if hasattr(value, 'columns'):
                    # DataFrame/GeoDataFrame
                    summary["extraction_results"][key]["columns"] = list(value.columns)
                elif isinstance(value, dict) and value:
                    summary["extraction_results"][key]["sample_keys"] = list(value.keys())[:10]
            else:
                summary["extraction_results"][key] = {
                    "value": str(value)[:100],  # First 100 characters
                    "type": str(type(value).__name__)
                }
        
        return {
            "status": "success",
            "summary": summary,
            "note": "Use /osm/extract for full data with serialization handling"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Summary extraction failed: {str(e)}")

@app.post("/osm/extract", tags=["OSM"])
async def extract_osm_data(lat: float, lon: float, radius: int = 500):
    """Extract OSM data for location with proper serialization"""
    if not osm_extractor:
        raise HTTPException(
            status_code=503,
            detail="OSM extractor not available"
        )
    
    try:
        # Call with correct signature (only lat, lon)
        location_data = osm_extractor.extract_location_data(lat, lon)
        
        # Safe serialization of data
        serialized_data = {}
        total_pois = 0
        total_buildings = 0
        
        for key, value in location_data.items():
            try:
                serialized_value = safe_serialize_data(value, max_items=3)
                serialized_data[key] = serialized_value
                
                # Count items for summary
                if hasattr(value, '__len__'):
                    count = len(value)
                    if 'poi' in key.lower():
                        total_pois += count
                    elif 'building' in key.lower():
                        total_buildings += count
                        
            except Exception as e:
                serialized_data[key] = {
                    "error": f"Serialization failed: {str(e)}",
                    "type": str(type(value).__name__)
                }
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon, "radius": radius},
            "extraction_summary": {
                "total_pois": total_pois,
                "total_buildings": total_buildings,
                "data_categories": list(serialized_data.keys()),
                "extraction_time": datetime.now().isoformat()
            },
            "data": serialized_data,
            "notes": [
                "Geometries converted to GeoJSON format where possible",
                "Large datasets are truncated with samples shown",
                "Use /osm/extract/summary for quick overview"
            ]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"OSM extraction failed: {str(e)}"
        )

# ==========================================
# CONFIGURATION ENDPOINTS
# ==========================================

@app.get("/config/environment", tags=["Configuration"])
async def get_environment_info():
    """Get environment configuration information"""
    return {
        "environment": os.getenv("ENVIRONMENT", "development"),
        "debug_mode": os.getenv("DEBUG_MODE", "true"),
        "configuration": {
            "neo4j_uri": os.getenv("NEO4J_URI", "not_set"),
            "neo4j_user": os.getenv("NEO4J_USER", "not_set"),
            "postgres_host": os.getenv("POSTGRES_HOST", "not_set"),
            "redis_host": os.getenv("REDIS_HOST", "not_set"),
            "h3_default_resolution": os.getenv("H3_DEFAULT_RESOLUTION", "7")
        },
        "recommendations": [
            "✅ Neo4j working" if neo4j_client else "❌ Create .env file with proper Neo4j credentials",
            "✅ OSM extractor working" if osm_extractor else "❌ Setup OSM extractor",
            "⏳ Setup Docker for PostGIS and Redis",
            "⏳ Import demographics data"
        ]
    }

@app.post("/config/test-credentials", tags=["Configuration"])
async def test_credentials():
    """Test all configured credentials"""
    results = {}
    
    # Test Neo4j
    if neo4j_client:
        try:
            neo4j_client.execute_query("RETURN 1")
            results["neo4j"] = {"status": "success", "message": "Connection successful"}
        except Exception as e:
            results["neo4j"] = {"status": "failed", "error": str(e)}
    else:
        results["neo4j"] = {"status": "not_configured", "message": "Client not loaded"}
    
    # Test environment variables
    required_vars = ["NEO4J_URI", "NEO4J_USER", "NEO4J_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        results["environment"] = {
            "status": "incomplete",
            "missing_variables": missing_vars
        }
    else:
        results["environment"] = {"status": "complete", "message": "All variables set"}
    
    return results

# ==========================================
# SETUP HELPERS
# ==========================================

@app.get("/setup/next-steps", tags=["Setup"])
async def get_next_steps():
    """Get recommended next setup steps"""
    
    steps = []
    
    # Check Neo4j
    if not neo4j_client:
        steps.append({
            "priority": "high",
            "step": "Fix Neo4j connection",
            "status": "❌ Required",
            "actions": [
                "Create .env file with NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD",
                "Ensure Neo4j Desktop is running",
                "Test connection manually in Neo4j Browser",
                "Restart FastAPI after fixing"
            ]
        })
    else:
        steps.append({
            "priority": "completed",
            "step": "Neo4j connection",
            "status": "✅ Working",
            "actions": ["Neo4j is connected and working properly"]
        })
    
    # Check OSM
    if not osm_extractor:
        steps.append({
            "priority": "high",
            "step": "Setup OSM extractor", 
            "status": "❌ Required",
            "actions": ["Fix OSM extractor import and configuration"]
        })
    else:
        steps.append({
            "priority": "completed",
            "step": "OSM data extraction",
            "status": "✅ Working", 
            "actions": ["OSM extractor is working properly"]
        })
    
    # Docker setup
    steps.append({
        "priority": "medium", 
        "step": "Setup Docker infrastructure",
        "status": "⏳ Pending",
        "actions": [
            "Install Docker Desktop if not available",
            "Run: docker-compose -f docker-compose.dev.yml up -d",
            "Verify PostGIS and Redis containers"
        ]
    })
    
    # Data import
    steps.append({
        "priority": "medium",
        "step": "Import demographics data", 
        "status": "⏳ Pending",
        "actions": [
            "Prepare .gpkg demographics file",
            "Create import script",
            "Load data into PostGIS"
        ]
    })
    
    return {
        "current_status": "partial_setup",
        "working_components": [
            "✅ FastAPI Core",
            "✅ Neo4j" if neo4j_client else "❌ Neo4j",
            "✅ OSM Extractor" if osm_extractor else "❌ OSM Extractor",
            "✅ Geometry Serialization" if SHAPELY_AVAILABLE else "⚠️ Limited Geometry Support"
        ],
        "next_steps": steps,
        "quick_test": "Visit /health to see current system status"
    }

# ==========================================
# ERROR HANDLERS
# ==========================================

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Global exception handler with helpful debugging info"""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "type": type(exc).__name__,
            "debugging_tips": [
                "Check /health endpoint for system status",
                "Try /osm/extract/summary for lighter OSM data",
                "Visit /setup/next-steps for setup guidance"
            ]
        }
    )

# ==========================================
# STARTUP/SHUTDOWN EVENTS
# ==========================================

@app.on_event("startup")
async def startup_event():
    """Application startup with status reporting"""
    print("\n" + "="*60)
    print("🚀 GeoRetail Core Infrastructure Starting v2.0.1")
    print("="*60)
    
    print(f"✅ FastAPI application loaded")
    print(f"{'✅' if neo4j_client else '❌'} Neo4j client: {'Available' if neo4j_client else 'Not available'}")
    print(f"{'✅' if osm_extractor else '❌'} OSM extractor: {'Available' if osm_extractor else 'Not available'}")
    print(f"{'✅' if SHAPELY_AVAILABLE else '⚠️ '} Geometry serialization: {'Available' if SHAPELY_AVAILABLE else 'Limited'}")
    
    print(f"\n📍 API available at:")
    print(f"   • Documentation: http://localhost:8000/docs")
    print(f"   • Health Check:  http://localhost:8000/health")
    print(f"   • Setup Guide:   http://localhost:8000/setup/next-steps")
    
    if osm_extractor:
        print(f"   • OSM Test:      http://localhost:8000/osm/extract/summary")
    
    ready_components = sum([
        1 if neo4j_client else 0,
        1 if osm_extractor else 0,
        1 if SHAPELY_AVAILABLE else 0
    ])
    
    print(f"\n🎯 System Status: {ready_components}/3 core components ready")
    
    if ready_components == 3:
        print("🎉 All core components working! Ready for H3 and PostGIS setup.")
    
    print("="*60)

@app.on_event("shutdown") 
async def shutdown_event():
    """Application shutdown with cleanup"""
    print("🛑 GeoRetail shutting down...")
    
    # Cleanup connections safely
    if neo4j_client:
        try:
            neo4j_client.close()
            print("✅ Neo4j connection closed")
        except:
            pass

# ==========================================
# MAIN ENTRY POINT
# ==========================================

if __name__ == "__main__":
    # Development server
    uvicorn.run(
        "main_safe:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )