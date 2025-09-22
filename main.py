"""
GeoRetail FastAPI Application
Main entry point - UC1 Explorer Mode
Version: 2.0.0
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any

# Додаємо src до Python path для правильних імпортів
sys.path.insert(0, str(Path(__file__).parent / "src"))

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import uvicorn
import logging
from datetime import datetime

# На початку файлу, після імпортів
import io
import sys

# Fix для Windows консолі
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# ================== Logging Setup ==================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('georetail.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ================== Import Routers ==================

try:
    # Auth router (existing)
    from src.api.endpoints.auth_endpoints import router as auth_router
    logger.info("✅ Auth router imported")
except ImportError as e:
    logger.warning(f"⚠️ Auth router not found: {e}")
    auth_router = None

try:
    # Territories router (UC1)
    from src.api.v2.territories.router import router as territories_router
    logger.info("✅ Territories router imported")
except ImportError as e:
    logger.warning(f"⚠️ Territories router not found: {e}")
    territories_router = None

try:
    # Database initialization
    from src.core.rbac_database import init_database, close_database, db_manager
    logger.info("✅ Database module imported")
except ImportError as e:
    logger.warning(f"⚠️ Database module not found: {e}")
    init_database = None
    close_database = None
    db_manager = None

try:
    # Configuration
    from src.core.config import settings
    logger.info("✅ Configuration imported")
except ImportError:
    logger.warning("⚠️ Configuration not found, using defaults")
    # Default settings if config not found
    class Settings:
        APP_NAME = "GeoRetail Analytics"
        APP_VERSION = "2.0.0"
        DEBUG = True
        CORS_ORIGINS = ["http://localhost:3000", "http://localhost:5173"]
        API_PREFIX = "/api/v2"
    settings = Settings()


# ================== Startup/Shutdown Events ==================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle manager для FastAPI
    Handles startup and shutdown events
    """
    # ===== STARTUP =====
    logger.info("=" * 50)
    logger.info("🚀 Starting GeoRetail API...")
    logger.info(f"📅 Start time: {datetime.now()}")
    logger.info(f"🐍 Python version: {sys.version}")
    logger.info(f"📁 Working directory: {Path.cwd()}")
    logger.info("=" * 50)
    
    # Initialize database
    if init_database:
        try:
            init_database()
            logger.info("✅ Database initialized successfully")
            
            # Test connection
            if db_manager and hasattr(db_manager, 'test_connection'):
                if db_manager.test_connection():
                    logger.info("✅ Database connection verified")
                else:
                    logger.warning("⚠️ Database connection test failed")
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            logger.info("⚠️ Continuing without database...")
    else:
        logger.warning("⚠️ Database module not available")
    
    # Initialize other services if needed
    # - Redis cache
    # - ClickHouse analytics
    # - ML models loading
    
    logger.info("✅ Application startup complete")
    logger.info("=" * 50)
    
    yield  # Application runs here
    
    # ===== SHUTDOWN =====
    logger.info("=" * 50)
    logger.info("🛑 Shutting down GeoRetail API...")
    
    # Close database connections
    if close_database:
        try:
            close_database()
            logger.info("✅ Database connections closed")
        except Exception as e:
            logger.error(f"❌ Error closing database: {e}")
    
    # Cleanup other resources
    logger.info("✅ Cleanup completed")
    logger.info(f"📅 Shutdown time: {datetime.now()}")
    logger.info("=" * 50)


# ================== Create FastAPI App ==================

app = FastAPI(
    title="GeoRetail Analytics API",
    description="""
    🗺️ Геоаналітична платформа для роздрібної торгівлі
    
    ## Features
    - 🔍 UC1 Explorer Mode - візуальний аналіз територій
    - 📊 Bivariate choropleth maps
    - 🔷 H3 hexagon analytics
    - 🏪 Competition analysis
    - 📈 ML-powered predictions
    
    ## Modules
    - **Territories** - робота з адмінодиницями та H3 гексагонами
    - **Authentication** - RBAC-based авторизація
    - **Insights** - аналітика та прогнозування (coming soon)
    - **Decisions** - рекомендації по локаціях (coming soon)
    """,
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


# ================== Middleware Configuration ==================

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",    # React dev server
        "http://localhost:5173",    # Vite dev server
        "http://localhost:5174",    # Alternative Vite port
        "http://127.0.0.1:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:5174",
        "http://localhost:8000",    # Self for Swagger UI
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# Trusted Host Middleware (security)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["localhost", "127.0.0.1", "*.georetail.local"]
)


# ================== Exception Handlers ==================

@app.exception_handler(404)
async def not_found_handler(request: Request, exc: HTTPException):
    """Custom 404 handler"""
    return JSONResponse(
        status_code=404,
        content={
            "error": "Not Found",
            "message": f"The requested URL {request.url.path} was not found",
            "path": request.url.path,
            "timestamp": datetime.utcnow().isoformat()
        }
    )

@app.exception_handler(500)
async def internal_error_handler(request: Request, exc: Exception):
    """Custom 500 handler"""
    logger.error(f"Internal error: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred",
            "timestamp": datetime.utcnow().isoformat()
        }
    )

@app.exception_handler(403)
async def forbidden_handler(request: Request, exc: HTTPException):
    """Custom 403 handler"""
    return JSONResponse(
        status_code=403,
        content={
            "error": "Forbidden",
            "message": "You don't have permission to access this resource",
            "timestamp": datetime.utcnow().isoformat()
        }
    )


# ================== Root & Utility Endpoints ==================

@app.get("/", include_in_schema=False)
async def root():
    """Root endpoint - redirect to docs"""
    return RedirectResponse(url="/docs")

@app.get("/api", tags=["info"])
async def api_info():
    """API information endpoint"""
    return {
        "name": "GeoRetail Analytics API",
        "version": "2.0.0",
        "mode": "UC1 Explorer",
        "status": "operational",
        "documentation": {
            "swagger": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json"
        },
        "modules": {
            "territories": "/api/v2/territories",
            "auth": "/api/v2/auth",
            "insights": "coming soon",
            "decisions": "coming soon"
        },
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health", tags=["monitoring"])
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint
    Returns system health status
    """
    health_status = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "uptime": "calculating...",
        "services": {
            "api": "operational",
            "database": "checking...",
            "redis": "checking...",
            "clickhouse": "checking..."
        },
        "version": "2.0.0",
        "environment": os.getenv("ENVIRONMENT", "development")
    }
    
    # Check PostgreSQL
    if db_manager:
        try:
            if db_manager.test_connection():
                health_status["services"]["database"] = "healthy"
            else:
                health_status["services"]["database"] = "unhealthy"
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["services"]["database"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
    else:
        health_status["services"]["database"] = "not configured"
    
    # Check Redis (if available)
    try:
        from src.database.connections import redis_connection
        # Implement Redis health check
        health_status["services"]["redis"] = "not implemented"
    except ImportError:
        health_status["services"]["redis"] = "not configured"
    
    # Check ClickHouse (if available)
    try:
        from src.database.connections import clickhouse
        # Implement ClickHouse health check
        health_status["services"]["clickhouse"] = "not implemented"
    except ImportError:
        health_status["services"]["clickhouse"] = "not configured"
    
    return health_status

@app.get("/metrics", tags=["monitoring"])
async def metrics():
    """
    Prometheus-style metrics endpoint
    """
    return {
        "http_requests_total": 0,
        "http_request_duration_seconds": 0,
        "database_connections_active": 0,
        "database_connections_idle": 0,
        "cache_hits_total": 0,
        "cache_misses_total": 0,
        "timestamp": datetime.utcnow().isoformat()
    }


# ================== Register API Routers ==================

# Auth endpoints
if auth_router:
    app.include_router(
        auth_router,
        prefix="/api/v2",
        tags=["authentication"]
    )
    logger.info("✅ Auth router registered")
else:
    logger.warning("⚠️ Auth router not registered")

# Territories endpoints (UC1)
if territories_router:
    app.include_router(
        territories_router,
        tags=["territories", "explorer"]
    )
    logger.info("✅ Territories router registered")
else:
    logger.warning("⚠️ Territories router not registered")

# Future routers (placeholder)
# from src.api.v2.insights.router import router as insights_router
# app.include_router(insights_router, tags=["insights"])

# from src.api.v2.decisions.router import router as decisions_router
# app.include_router(decisions_router, tags=["decisions"])

# from src.api.v2.portfolio.router import router as portfolio_router
# app.include_router(portfolio_router, tags=["portfolio"])


# ================== Static Files (if needed) ==================

# Serve static files if frontend is built
static_path = Path("frontend/dist")
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
    logger.info(f"✅ Static files mounted from {static_path}")


# ================== Main Entry Point ==================

if __name__ == "__main__":
    """
    Run the application with uvicorn
    For production use: gunicorn or systemd service
    """
    
    # Get configuration from environment or defaults
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    reload = os.getenv("RELOAD", "true").lower() == "true"
    workers = int(os.getenv("WORKERS", "1"))
    log_level = os.getenv("LOG_LEVEL", "info")
    
    print("=" * 60)
    print(" 🚀 GeoRetail Analytics API")
    print("=" * 60)
    print(f" 📡 Server:   http://{host}:{port}")
    print(f" 📚 Docs:     http://{host}:{port}/docs")
    print(f" 📘 ReDoc:    http://{host}:{port}/redoc")
    print(f" 🔄 Reload:   {reload}")
    print(f" 👷 Workers:  {workers}")
    print(f" 📝 Log level: {log_level}")
    print("=" * 60)
    print(" Press CTRL+C to stop the server")
    print("=" * 60)
    
    # Run with uvicorn
    uvicorn.run(
        "main:app",  # app instance
        host=host,
        port=port,
        reload=reload,
        workers=workers if not reload else 1,  # Multiple workers only without reload
        log_level=log_level,
        access_log=True,
        use_colors=True
    )