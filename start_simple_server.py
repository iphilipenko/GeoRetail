# start_simple_server.py
"""
🚀 Простий запуск H3 Modal API без складних залежностей
Для швидкого тестування API endpoints
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime
import uvicorn
import sys
from pathlib import Path

# Додаємо src до path
src_path = Path(__file__).parent / "src"
sys.path.append(str(src_path))

print(f"📁 Source path: {src_path}")
print(f"🐍 Python version: {sys.version}")

# Створюємо FastAPI app
app = FastAPI(
    title="GeoRetail H3 Modal API",
    description="H3 Hexagonal Spatial Analysis API",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Імпортуємо H3 endpoints
H3_AVAILABLE = False
try:
    from api.endpoints.h3_modal_endpoints import router as h3_router
    from api.endpoints.test_database_endpoint import router as db_router
    app.include_router(h3_router)
    app.include_router(db_router)
    H3_AVAILABLE = True
    print("✅ H3 Modal endpoints imported successfully")
except Exception as e:
    print(f"❌ Failed to import H3 endpoints: {e}")
    H3_AVAILABLE = False

# Базові endpoints
@app.get("/")
async def root():
    return {
        "message": "GeoRetail H3 Modal API",
        "version": "2.1.0",
        "status": "operational",
        "h3_endpoints_available": H3_AVAILABLE,
        "timestamp": datetime.now().isoformat(),
        "docs": "/docs",
        "endpoints": {
            "coverage_calculator": "/api/v1/hexagon-details/coverage-calculator",
            "analysis_preview": "/api/v1/hexagon-details/analysis-preview/{h3_index}",
            "hexagon_details": "/api/v1/hexagon-details/details/{h3_index}",
            "poi_analysis": "/api/v1/hexagon-details/poi-in-hexagon/{h3_index}",
            "competitive_analysis": "/api/v1/hexagon-details/competitive-analysis/{h3_index}",
            "database_test": "/api/v1/database/test-connection"
        }
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "h3_endpoints": H3_AVAILABLE,
        "timestamp": datetime.now().isoformat(),
        "version": "2.1.0"
    }

# Fallback endpoints якщо H3 не працює
if not H3_AVAILABLE:
    @app.get("/api/v1/test")
    async def test_fallback():
        return {
            "message": "H3 endpoints not available, but server is running",
            "suggestions": [
                "Check h3 library installation: pip install h3",
                "Check file src/api/endpoints/h3_modal_endpoints.py exists",
                "Check for syntax errors in endpoints"
            ]
        }

if __name__ == "__main__":
    print("🚀 Starting simple H3 Modal API server...")
    print("📡 Server URL: http://localhost:8000")
    print("📚 Swagger UI: http://localhost:8000/docs")
    print("🛑 Stop: Ctrl+C")
    print("="*50)
    
    uvicorn.run(
        "start_simple_server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
