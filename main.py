"""
GeoRetail FastAPI Application
Main entry point - UC1 Explorer Mode
Version: 2.0.0
"""

import os
import sys
import time
import argparse
from pathlib import Path
from typing import Dict, Any
from datetime import datetime
import logging

# Встановлюємо кодування для Windows
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Додаємо src до Python path для правильних імпортів
sys.path.insert(0, str(Path(__file__).parent / "src"))

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
import uvicorn

# ================== Logging Setup ==================

# Налаштування логування з UTF-8
def setup_logging():
    """Налаштування логування з підтримкою UTF-8"""
    # Отримуємо root logger
    root_logger = logging.getLogger()
    
    # Якщо вже є handlers, не переналаштовуємо
    if not root_logger.handlers:
        # Створюємо console handler
        console_handler = logging.StreamHandler()
        
        # Файловий handler з UTF-8
        try:
            file_handler = logging.FileHandler('georetail.log', encoding='utf-8', mode='a')
        except:
            file_handler = logging.FileHandler('georetail.log', mode='a')
        
        # Форматування
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)
        
        # Додаємо handlers
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)
        root_logger.setLevel(logging.INFO)
    
    return logging.getLogger(__name__)

# Ініціалізуємо логування
logger = setup_logging()

# ================== Diagnostic Functions ==================

def find_changing_files(seconds_ago: int = 10):
    """
    Знайти файли що змінювалися останні N секунд
    Для діагностики проблеми з watchfiles
    """
    print("\n" + "=" * 60)
    print(f"[DIAGNOSTIC] Checking for files modified in last {seconds_ago} seconds")
    print("=" * 60)
    
    current_time = time.time()
    base_path = Path.cwd()
    modified_files = []
    
    # Директорії які ігноруємо
    ignore_dirs = {'.git', '__pycache__', '.vscode', '.idea', 'node_modules', '.pytest_cache'}
    ignore_extensions = {'.pyc', '.pyo', '.log', '.tmp', '.db', '.sqlite', '.pid'}
    
    for root, dirs, files in os.walk(base_path):
        # Видаляємо ігноровані директорії зі списку для обходу
        dirs[:] = [d for d in dirs if d not in ignore_dirs]
        
        root_path = Path(root)
        
        # Пропускаємо якщо це ігнорована директорія
        if any(ignored in str(root_path) for ignored in ignore_dirs):
            continue
            
        for file in files:
            filepath = root_path / file
            
            # Пропускаємо файли з ігнорованими розширеннями
            if filepath.suffix in ignore_extensions:
                continue
                
            try:
                stat = filepath.stat()
                mtime = stat.st_mtime
                
                if current_time - mtime < seconds_ago:
                    relative_path = filepath.relative_to(base_path)
                    modified_files.append({
                        'path': str(relative_path),
                        'seconds_ago': int(current_time - mtime),
                        'size': stat.st_size
                    })
            except Exception as e:
                pass  # Ігноруємо помилки доступу
    
    if modified_files:
        print(f"Found {len(modified_files)} recently modified files:")
        for file_info in sorted(modified_files, key=lambda x: x['seconds_ago']):
            print(f"  - {file_info['path']} (modified {file_info['seconds_ago']}s ago, size: {file_info['size']} bytes)")
    else:
        print("No recently modified files found")
    
    print("=" * 60 + "\n")
    return modified_files

# ================== Import Routers ==================

try:
    # Auth router (existing)
    from src.api.endpoints.auth_endpoints import router as auth_router
    logger.info("[OK] Auth router imported")
except ImportError as e:
    logger.warning(f"[WARNING] Auth router not found: {e}")
    auth_router = None

try:
    # Territories router (UC1)
    from src.api.v2.territories.router import router as territories_router
    logger.info("[OK] Territories router imported")
except ImportError as e:
    logger.warning(f"[WARNING] Territories router not found: {e}")
    territories_router = None

try:
    # Database initialization
    from src.core.rbac_database import init_database, close_database, db_manager
    logger.info("[OK] Database module imported")
except ImportError as e:
    logger.warning(f"[WARNING] Database module not found: {e}")
    init_database = None
    close_database = None
    db_manager = None

try:
    # Configuration
    from src.core.config import settings
    logger.info("[OK] Configuration imported")
except ImportError:
    logger.warning("[WARNING] Configuration not found, using defaults")
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
    logger.info("[START] Starting GeoRetail API...")
    logger.info(f"[TIME] Start time: {datetime.now()}")
    logger.info(f"[PYTHON] Version: {sys.version}")
    logger.info(f"[PATH] Working directory: {Path.cwd()}")
    logger.info("=" * 50)
    
    # Initialize database
    if init_database:
        try:
            init_database()
            logger.info("[OK] Database initialized successfully")
            
            # Test connection
            if db_manager and hasattr(db_manager, 'test_connection'):
                if db_manager.test_connection():
                    logger.info("[OK] Database connection verified")
                else:
                    logger.warning("[WARNING] Database connection test failed")
        except Exception as e:
            logger.error(f"[ERROR] Database initialization failed: {e}")
            logger.info("[WARNING] Continuing without database...")
    else:
        logger.warning("[WARNING] Database module not available")
    
    logger.info("[OK] Application startup complete")
    logger.info("=" * 50)
    
    yield  # Application runs here
    
    # ===== SHUTDOWN =====
    logger.info("=" * 50)
    logger.info("[STOP] Shutting down GeoRetail API...")
    
    # Close database connections
    if close_database:
        try:
            close_database()
            logger.info("[OK] Database connections closed")
        except Exception as e:
            logger.error(f"[ERROR] Error closing database: {e}")
    
    logger.info("[OK] Cleanup completed")
    logger.info(f"[TIME] Shutdown time: {datetime.now()}")
    logger.info("=" * 50)

# ================== Create FastAPI App ==================

app = FastAPI(
    title="GeoRetail Analytics API",
    description="""
    Геоаналітична платформа для роздрібної торгівлі
    
    ## Features
    - UC1 Explorer Mode - візуальний аналіз територій
    - Bivariate choropleth maps
    - H3 hexagon analytics
    - Competition analysis
    - ML-powered predictions
    
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
    
    # Check ClickHouse (critical for analytics)
    try:
        from clickhouse_driver import Client
        from src.core.config import settings
        
        # Спробуємо різні варіанти підключення
        connections_to_try = [
            # Спочатку з config
            {
                'host': settings.CLICKHOUSE_HOST,
                'port': settings.CLICKHOUSE_PORT,
                'database': settings.CLICKHOUSE_DB,
                'user': getattr(settings, 'CLICKHOUSE_USER', 'webuser'),
                'password': getattr(settings, 'CLICKHOUSE_PASSWORD', 'password123')
            },
            # Потім default без пароля
            {
                'host': settings.CLICKHOUSE_HOST,
                'port': settings.CLICKHOUSE_PORT,
                'database': settings.CLICKHOUSE_DB,
                'user': 'default',
                'password': ''
            }
        ]
        
        client = None
        for conn_params in connections_to_try:
            try:
                client = Client(**conn_params)
                result = client.execute('SELECT 1')
                # Якщо дійшли сюди - підключення успішне
                health_status["services"]["clickhouse"] = "healthy"
                
                # Додаткова перевірка таблиць
                try:
                    tables = client.execute('SHOW TABLES')
                    if tables:
                        health_status["services"]["clickhouse"] = f"healthy ({len(tables)} tables, user: {conn_params['user']})"
                    else:
                        health_status["services"]["clickhouse"] = f"healthy (no tables yet, user: {conn_params['user']})"
                except:
                    health_status["services"]["clickhouse"] = f"healthy (connected as {conn_params['user']})"
                break
            except Exception as e:
                continue  # Спробуємо наступний варіант
                
        if not client:
            # Якщо жоден варіант не спрацював
            health_status["services"]["clickhouse"] = "auth failed (try default user with no password)"
            health_status["status"] = "degraded"
            logger.error("[ERROR] ClickHouse: не вдалося підключитися. Спробуйте default користувача")
            
    except ImportError:
        health_status["services"]["clickhouse"] = "driver not installed"
        health_status["status"] = "degraded"
        logger.warning("[WARNING] clickhouse-driver not installed. Run: pip install clickhouse-driver")
    except Exception as e:
        health_status["services"]["clickhouse"] = f"error: {str(e)[:100]}"
        health_status["status"] = "degraded"
        logger.error(f"[ERROR] ClickHouse connection failed: {e}")
    
    # Redis check (not critical)
    health_status["services"]["redis"] = "not configured"
    
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
        
        tags=["authentication"]
    )
    logger.info("[OK] Auth router registered")
else:
    logger.warning("[WARNING] Auth router not registered")

# Territories endpoints (UC1)
if territories_router:
    app.include_router(
        territories_router,
        tags=["territories", "explorer"]
    )
    logger.info("[OK] Territories router registered")
else:
    logger.warning("[WARNING] Territories router not registered")

# ================== Static Files (if needed) ==================

static_path = Path("frontend/dist")
if static_path.exists():
    app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
    logger.info(f"[OK] Static files mounted from {static_path}")

# ================== Main Entry Point ==================

if __name__ == "__main__":
    """
    Run the application with uvicorn
    For production use: gunicorn or systemd service
    """
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='GeoRetail API Server')
    parser.add_argument('--no-reload', action='store_true', 
                       help='Disable auto-reload')
    parser.add_argument('--diagnose', action='store_true',
                       help='Run diagnostics for file changes')
    parser.add_argument('--port', type=int, default=8000,
                       help='Port to run server on')
    parser.add_argument('--host', default='0.0.0.0',
                       help='Host to run server on')
    args = parser.parse_args()
    
    # Get configuration
    host = args.host
    port = args.port
    reload = not args.no_reload and os.getenv("RELOAD", "true").lower() == "true"
    workers = int(os.getenv("WORKERS", "1"))
    log_level = os.getenv("LOG_LEVEL", "info")
    
    # Run diagnostics if requested
    if args.diagnose:
        print("[DIAGNOSTIC MODE]")
        print("Checking for file changes...")
        
        # Check initial state
        find_changing_files(5)
        
        # Wait and check again
        print("Waiting 5 seconds...")
        time.sleep(5)
        find_changing_files(5)
        
        print("Waiting another 5 seconds...")
        time.sleep(5)
        find_changing_files(5)
        
        print("\n[DIAGNOSTIC COMPLETE]")
        print("Now starting server with reload disabled for testing...")
        reload = False
    
    print("=" * 60)
    print(" [GEORETAIL] Analytics API Server")
    print("=" * 60)
    print(f" [SERVER]   http://{host}:{port}")
    print(f" [DOCS]     http://{host}:{port}/docs")
    print(f" [REDOC]    http://{host}:{port}/redoc")
    print(f" [RELOAD]   {reload}")
    print(f" [WORKERS]  {workers}")
    print(f" [LOG]      {log_level}")
    print("=" * 60)
    print(" Press CTRL+C to stop the server")
    print("=" * 60)
    
    try:
        # Configure uvicorn based on reload setting
        if reload:
            # З reload - обмежуємо директорії для спостереження
            uvicorn.run(
                "main:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=["src", "api"],  # Тільки ці папки
                workers=1,  # reload працює тільки з 1 воркером
                log_level=log_level,
                access_log=True,
                use_colors=True
            )
        else:
            # Без reload - можемо використовувати кілька воркерів
            uvicorn.run(
                "main:app",
                host=host,
                port=port,
                reload=False,
                workers=workers,
                log_level=log_level,
                access_log=True,
                use_colors=True
            )
    except KeyboardInterrupt:
        logger.info("[STOP] Server stopped by user")
    except Exception as e:
        logger.error(f"[ERROR] Server error: {e}")
        raise