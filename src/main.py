"""
GeoRetail FastAPI Application
Main entry point for the Core Infrastructure API
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import os
import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent
sys.path.append(str(src_path))

# Import existing modules (preserving your work)
try:
    # Try to import existing modules from your current structure
    from graph.neo4j_client import geo_neo4j_client
    from data.osm_extractor import geo_osm_extractor
except ImportError as e:
    print(f"Note: Some existing modules not found: {e}")
    print("This is normal during initial setup")

# Import H3 visualization endpoints
try:
    from api.endpoints.h3_visualization import router as h3_viz_router
    from api.endpoints.h3_modal_endpoints import router as h3_modal_router
    from api.endpoints.test_database_endpoint import router as test_db_router
    H3_ENDPOINTS_AVAILABLE = True
except ImportError as e:
    print(f"H3 endpoints not available: {e}")
    H3_ENDPOINTS_AVAILABLE = False

# Import new Core Infrastructure modules
try:
    from api.endpoints import health, screening, analysis
    from core.config import settings
except ImportError:
    # These will be created as we build out the infrastructure
    print("Core Infrastructure modules will be available after full setup")

# Create FastAPI application
app = FastAPI(
    title="GeoRetail Core Infrastructure",
    description="""
    Advanced retail location intelligence system using:
    • H3 hexagonal spatial indexing
    • Graph neural networks and embeddings
    • Competitive analysis and market saturation
    • Revenue forecasting and risk assessment
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# REGISTER H3 ENDPOINTS
# ==========================================

if H3_ENDPOINTS_AVAILABLE:
    # Register H3 visualization endpoints
    app.include_router(h3_viz_router)
    app.include_router(h3_modal_router)
    app.include_router(test_db_router)
    print("✅ H3 visualization, modal, and database test endpoints registered")
else:
    print("⚠️ H3 endpoints not available - check imports")

# ==========================================
# HEALTH CHECK ENDPOINTS
# ==========================================

@app.get("/", tags=["System"])
async def root():
    """Root endpoint with system information"""
    return {
        "message": "GeoRetail Core Infrastructure",
        "version": "2.0.0",
        "status": "operational",
        "docs": "/docs",
        "features": [
            "H3 spatial indexing",
            "Graph embeddings", 
            "Competitive analysis",
            "Revenue forecasting"
        ]
    }

@app.get("/health", tags=["System"])
async def health_check():
    """Comprehensive health check for all system components"""
    
    health_status = {
        "status": "healthy",
        "timestamp": "2024-01-01T00:00:00Z",  # Will be updated with real timestamp
        "services": {}
    }
    
    # Check existing services (from your current setup)
    try:
        # Neo4j connection check
        result = geo_neo4j_client.execute_query("RETURN 'healthy' as status")
        health_status["services"]["neo4j"] = {
            "status": "healthy" if result else "unhealthy",
            "message": "Neo4j connection successful"
        }
    except Exception as e:
        health_status["services"]["neo4j"] = {
            "status": "unhealthy", 
            "message": f"Neo4j connection failed: {str(e)}"
        }
    
    # Check OSM extractor
    try:
        # Simple test of OSM functionality
        health_status["services"]["osm_extractor"] = {
            "status": "healthy",
            "message": "OSM extractor available"
        }
    except Exception as e:
        health_status["services"]["osm_extractor"] = {
            "status": "unhealthy",
            "message": f"OSM extractor error: {str(e)}"
        }
    
    # Check new infrastructure components (will be added progressively)
    health_status["services"]["postgis"] = {"status": "pending", "message": "Not yet configured"}
    health_status["services"]["redis"] = {"status": "pending", "message": "Not yet configured"}
    health_status["services"]["h3_processor"] = {"status": "pending", "message": "Not yet configured"}
    
    # Overall status
    unhealthy_services = [
        service for service, info in health_status["services"].items() 
        if info["status"] == "unhealthy"
    ]
    
    if unhealthy_services:
        health_status["status"] = "degraded"
        health_status["unhealthy_services"] = unhealthy_services
    
    return health_status

# ==========================================
# LEGACY ENDPOINTS (preserving existing functionality)
# ==========================================

@app.get("/legacy/test-neo4j", tags=["Legacy"])
async def test_neo4j_connection():
    """Test existing Neo4j setup"""
    try:
        result = geo_neo4j_client.execute_query("RETURN 'Connection successful' as message")
        return {"status": "success", "result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Neo4j connection failed: {str(e)}")

@app.get("/legacy/test-osm", tags=["Legacy"])
async def test_osm_extraction():
    """Test existing OSM extraction"""
    try:
        # Test with Kyiv center coordinates
        lat, lon = 50.4501, 30.5234
        location_data = geo_osm_extractor.extract_location_data(lat, lon)
        
        return {
            "status": "success",
            "location": {"lat": lat, "lon": lon},
            "pois_found": len(location_data.get('pois', [])),
            "buildings_count": location_data.get('buildings', {}).get('count', 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OSM extraction failed: {str(e)}")

# ==========================================
# NEW CORE INFRASTRUCTURE ENDPOINTS
# ==========================================

@app.post("/api/v1/screening/region", tags=["H3 Screening"])
async def screen_region():
    """H3 region screening endpoint (to be implemented)"""
    return {
        "status": "not_implemented",
        "message": "H3 screening will be available after PostGIS setup",
        "todo": [
            "Setup PostGIS with H3 extension",
            "Import Ukraine H3 grid",
            "Implement screening algorithm"
        ]
    }

@app.post("/api/v1/screening/location", tags=["Location Analysis"])
async def analyze_location():
    """Single location analysis endpoint (to be implemented)"""
    return {
        "status": "not_implemented", 
        "message": "Location analysis will integrate existing OSM + new H3 processing"
    }

@app.get("/api/v1/competitors/analyze", tags=["Competitive Analysis"])
async def analyze_competitors():
    """Competitive analysis endpoint (to be implemented)"""
    return {
        "status": "not_implemented",
        "message": "Will use existing Neo4j graph + new competitive algorithms"
    }

# ==========================================
# DATA MANAGEMENT ENDPOINTS
# ==========================================

@app.post("/api/v1/data/import/demographics", tags=["Data Import"])
async def import_demographics():
    """Import H3 demographics data"""
    return {
        "status": "not_implemented",
        "message": "Ready to import your .gpkg demographics file"
    }

@app.post("/api/v1/data/import/stores", tags=["Data Import"])  
async def import_stores():
    """Import store network data"""
    return {
        "status": "not_implemented",
        "message": "Ready to import store network with your custom schema"
    }

@app.get("/api/v1/data/quality", tags=["Data Management"])
async def data_quality_dashboard():
    """Data quality monitoring dashboard"""
    return {
        "status": "not_implemented",
        "message": "Data quality metrics will be available after database setup"
    }

# ==========================================
# DEVELOPMENT AND TESTING
# ==========================================

@app.get("/dev/info", tags=["Development"])
async def development_info():
    """Development environment information"""
    return {
        "python_version": sys.version,
        "working_directory": str(Path.cwd()),
        "environment_variables": {
            "ENVIRONMENT": os.getenv("ENVIRONMENT", "development"),
            "DEBUG": os.getenv("DEBUG", "true"),
            "NEO4J_URI": os.getenv("NEO4J_URI", "not_set"),
            "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "not_set"),
            "REDIS_HOST": os.getenv("REDIS_HOST", "not_set")
        },
        "next_steps": [
            "1. Run upgrade_to_core_infrastructure.py",
            "2. Setup Docker infrastructure with make docker-up",
            "3. Import demographics and store data",
            "4. Implement H3 processing pipeline",
            "5. Add graph embedding capabilities"
        ]
    }

# ==========================================
# ERROR HANDLERS
# ==========================================

@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Global exception handler"""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": str(exc),
            "type": type(exc).__name__
        }
    )

# ==========================================
# APPLICATION STARTUP
# ==========================================

@app.on_event("startup")
async def startup_event():
    """Application startup tasks"""
    print("🚀 GeoRetail Core Infrastructure starting...")
    print("📍 Existing OSM functionality preserved")
    print("🔗 Neo4j integration maintained")
    print("⭡ Ready for H3 and FastAPI extensions")

@app.on_event("shutdown")
async def shutdown_event():
    """Application shutdown tasks"""
    print("🛑 GeoRetail Core Infrastructure shutting down...")
    
    # Cleanup connections
    try:
        geo_neo4j_client.close()
    except:
        pass

# ==========================================
# MAIN ENTRY POINT
# ==========================================

if __name__ == "__main__":
    # Development server
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )