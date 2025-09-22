"""
GeoRetail API v2
Main FastAPI application with territories and auth endpoints
"""

import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

# Import routers
from api.endpoints.auth_endpoints import router as auth_router
from api.v2.territories.router import router as territories_router

# Import database connections
from database.connections import init_databases, close_databases

# Import configuration
from core.config import settings


# ================== Application Lifespan ==================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управління життєвим циклом додатку
    """
    # Startup
    print("🚀 Starting GeoRetail API v2...")
    
    # Ініціалізація баз даних
    await init_databases()
    
    print("✅ Application started successfully!")
    print(f"📍 API available at http://localhost:{settings.PORT}")
    print(f"📚 Documentation at http://localhost:{settings.PORT}/docs")
    
    yield
    
    # Shutdown
    print("🔄 Shutting down GeoRetail API...")
    
    # Закриття підключень
    await close_databases()
    
    print("✅ Application shut down successfully!")


# ================== Application Instance ==================

app = FastAPI(
    title="GeoRetail API v2",
    description="Territory Intelligence Platform for Retail Analytics",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan
)


# ================== Middleware ==================

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],  # React dev servers
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Total-Count", "X-Request-ID"]
)

# Gzip compression for large responses
app.add_middleware(
    GZipMiddleware,
    minimum_size=1000  # Compress responses larger than 1KB
)

# Trusted host middleware (security)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["localhost", "127.0.0.1", "*.georetail.com"]
)


# ================== Request/Response Middleware ==================

@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """Додає час обробки запиту в headers"""
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = f"{process_time:.3f}"
    return response


@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Додає унікальний ID запиту для трекінгу"""
    import uuid
    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


# ================== Exception Handlers ==================

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Обробка HTTP помилок"""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": "http_error",
            "detail": exc.detail,
            "status_code": exc.status_code,
            "request_id": getattr(request.state, "request_id", None)
        }
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Обробка помилок валідації"""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "validation_error",
            "detail": "Invalid request data",
            "field_errors": exc.errors(),
            "request_id": getattr(request.state, "request_id", None)
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Обробка всіх інших помилок"""
    import traceback
    
    # Логуємо помилку (в production використовуйте proper logging)
    print(f"Unhandled exception: {exc}")
    print(traceback.format_exc())
    
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "internal_server_error",
            "detail": "An unexpected error occurred",
            "request_id": getattr(request.state, "request_id", None)
        }
    )


# ================== Routes ==================

# Root endpoint
@app.get("/", tags=["root"])
async def root() -> Dict[str, Any]:
    """Root endpoint with API info"""
    return {
        "name": "GeoRetail API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "auth": "/api/v2/auth",
            "territories": "/api/v2/territories",
            "docs": "/docs",
            "redoc": "/redoc",
            "openapi": "/openapi.json",
            "health": "/health"
        }
    }


# Health check
@app.get("/health", tags=["health"])
async def health_check() -> Dict[str, Any]:
    """Comprehensive health check"""
    
    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "services": {
            "api": "running",
            "postgres": "unknown",
            "clickhouse": "unknown",
            "redis": "unknown"
        }
    }
    
    # Перевірка PostgreSQL
    try:
        from database.connections import postgres_engine
        from sqlalchemy import text
        async with postgres_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        health_status["services"]["postgres"] = "healthy"
    except Exception as e:
        health_status["services"]["postgres"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Перевірка ClickHouse
    try:
        from database.connections import clickhouse
        ch_client = clickhouse.get_client()
        ch_client.execute("SELECT 1")
        health_status["services"]["clickhouse"] = "healthy"
    except Exception as e:
        health_status["services"]["clickhouse"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    # Перевірка Redis
    try:
        from database.connections import redis_connection
        redis_client = await redis_connection.get_redis()
        await redis_client.ping()
        health_status["services"]["redis"] = "healthy"
    except Exception as e:
        health_status["services"]["redis"] = f"unhealthy: {str(e)}"
        health_status["status"] = "degraded"
    
    return health_status


# ================== Register Routers ==================

# Auth endpoints (existing)
app.include_router(
    auth_router,
    prefix="/api/v2",
    tags=["authentication"]
)

# Territories endpoints (new for UC1)
app.include_router(
    territories_router,
    tags=["territories", "explorer"]
)

# Admin endpoints (якщо існує)
# app.include_router(admin_router, prefix="/api/v2/admin", tags=["admin"])

# Insights endpoints (майбутнє)
# app.include_router(insights_router, prefix="/api/v2/insights", tags=["insights"])

# Decisions endpoints (майбутнє)
# app.include_router(decisions_router, prefix="/api/v2/decisions", tags=["decisions"])


# ================== Run Application ==================

if __name__ == "__main__":
    import uvicorn
    
    # Конфігурація для development
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload при змінах коду
        log_level="info",
        access_log=True,
        workers=1  # Для development, в production використовуйте більше
    )