"""
# Standard response builders
Created for API v2 Domain-Driven Architecture
"""
"""
File: src/api/v2/core/responses.py
Path: C:\projects\AA AI Assistance\GeoRetail_git\georetail\src\api\v2\core\responses.py

Purpose: Стандартні response builders для консистентних відповідей API
- Успішні відповіді з data/meta/links
- Error responses з детальною інформацією
- Pagination metadata
- Permission tracking в responses
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import time
import logging

logger = logging.getLogger(__name__)

# ==========================================
# RESPONSE SCHEMAS
# ==========================================

class MetaData(BaseModel):
    """Метадані для всіх responses"""
    timestamp: str
    version: str = "2.0"
    execution_time_ms: Optional[int] = None
    permissions_used: Optional[List[str]] = None
    cache_hit: bool = False
    request_id: Optional[str] = None

class PaginationMeta(BaseModel):
    """Метадані пагінації"""
    page: int
    limit: int
    total_items: int
    total_pages: int
    has_next: bool
    has_prev: bool

class Links(BaseModel):
    """Корисні посилання"""
    self: str
    next: Optional[str] = None
    prev: Optional[str] = None
    first: Optional[str] = None
    last: Optional[str] = None
    related: Optional[Dict[str, str]] = None

class ErrorDetail(BaseModel):
    """Деталі помилки"""
    code: str
    message: str
    field: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

# ==========================================
# SUCCESS RESPONSES
# ==========================================

def create_response(
    data: Any,
    meta: Optional[Dict[str, Any]] = None,
    links: Optional[Dict[str, Any]] = None,
    permissions_used: Optional[List[str]] = None,
    execution_start: Optional[float] = None,
    cache_hit: bool = False,
    status_code: int = 200
) -> JSONResponse:
    """
    Створення стандартної успішної відповіді
    
    Args:
        data: Основні дані відповіді
        meta: Додаткові метадані
        links: Корисні посилання
        permissions_used: Використані permissions
        execution_start: Час початку виконання (для розрахунку execution_time)
        cache_hit: Чи дані з кешу
        status_code: HTTP status code
    
    Returns:
        JSONResponse з правильною структурою
    """
    
    # Розрахунок часу виконання
    execution_time_ms = None
    if execution_start:
        execution_time_ms = int((time.time() - execution_start) * 1000)
    
    # Базові метадані
    response_meta = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "version": "2.0",
        "cache_hit": cache_hit
    }
    
    if execution_time_ms is not None:
        response_meta["execution_time_ms"] = execution_time_ms
    
    if permissions_used:
        response_meta["permissions_used"] = permissions_used
    
    # Додаємо custom meta
    if meta:
        response_meta.update(meta)
    
    # Формуємо response body
    response_body = {
        "data": data,
        "meta": response_meta
    }
    
    if links:
        response_body["links"] = links
    
    return JSONResponse(
        content=response_body,
        status_code=status_code
    )

def create_list_response(
    items: List[Any],
    page: int,
    limit: int,
    total_items: int,
    base_url: str,
    permissions_used: Optional[List[str]] = None,
    execution_start: Optional[float] = None,
    extra_meta: Optional[Dict[str, Any]] = None
) -> JSONResponse:
    """
    Створення відповіді для списків з пагінацією
    
    Args:
        items: Список елементів
        page: Поточна сторінка
        limit: Елементів на сторінку
        total_items: Загальна кількість елементів
        base_url: Базовий URL для links
        permissions_used: Використані permissions
        execution_start: Час початку виконання
        extra_meta: Додаткові метадані
    """
    
    # Розрахунок пагінації
    total_pages = (total_items + limit - 1) // limit
    has_next = page < total_pages
    has_prev = page > 1
    
    # Метадані пагінації
    pagination_meta = {
        "page": page,
        "limit": limit,
        "total_items": total_items,
        "total_pages": total_pages,
        "has_next": has_next,
        "has_prev": has_prev
    }
    
    # Links для навігації
    links = {
        "self": f"{base_url}?page={page}&limit={limit}",
        "first": f"{base_url}?page=1&limit={limit}",
        "last": f"{base_url}?page={total_pages}&limit={limit}"
    }
    
    if has_next:
        links["next"] = f"{base_url}?page={page + 1}&limit={limit}"
    
    if has_prev:
        links["prev"] = f"{base_url}?page={page - 1}&limit={limit}"
    
    # Об'єднуємо метадані
    meta = {"pagination": pagination_meta}
    if extra_meta:
        meta.update(extra_meta)
    
    return create_response(
        data={"items": items, "count": len(items)},
        meta=meta,
        links=links,
        permissions_used=permissions_used,
        execution_start=execution_start
    )

# ==========================================
# ERROR RESPONSES
# ==========================================

def create_error_response(
    error_code: str,
    message: str,
    status_code: int = 400,
    details: Optional[Dict[str, Any]] = None,
    field: Optional[str] = None
) -> JSONResponse:
    """
    Створення стандартної error response
    
    Args:
        error_code: Код помилки (e.g., "VALIDATION_ERROR")
        message: Людинозрозуміле повідомлення
        status_code: HTTP status code
        details: Додаткові деталі помилки
        field: Поле що спричинило помилку (для валідації)
    """
    
    error_body = {
        "error": {
            "code": error_code,
            "message": message
        },
        "meta": {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "version": "2.0"
        }
    }
    
    if details:
        error_body["error"]["details"] = details
    
    if field:
        error_body["error"]["field"] = field
    
    return JSONResponse(
        content=error_body,
        status_code=status_code
    )

def validation_error_response(
    errors: List[Dict[str, Any]],
    message: str = "Validation failed"
) -> JSONResponse:
    """
    Спеціальна response для validation errors
    
    Args:
        errors: Список помилок валідації
        message: Загальне повідомлення
    """
    
    return create_error_response(
        error_code="VALIDATION_ERROR",
        message=message,
        status_code=status.HTTP_400_BAD_REQUEST,
        details={"validation_errors": errors}
    )

def permission_denied_response(
    required_permission: str,
    user_permissions: Optional[List[str]] = None
) -> JSONResponse:
    """
    Response для permission denied
    """
    
    details = {"required_permission": required_permission}
    if user_permissions:
        details["user_permissions"] = user_permissions
    
    return create_error_response(
        error_code="PERMISSION_DENIED",
        message=f"User does not have permission '{required_permission}'",
        status_code=status.HTTP_403_FORBIDDEN,
        details=details
    )

def not_found_response(
    resource_type: str,
    resource_id: Union[str, int],
    message: Optional[str] = None
) -> JSONResponse:
    """
    Response для not found (404)
    """
    
    if not message:
        message = f"{resource_type} with id '{resource_id}' not found"
    
    return create_error_response(
        error_code="NOT_FOUND",
        message=message,
        status_code=status.HTTP_404_NOT_FOUND,
        details={
            "resource_type": resource_type,
            "resource_id": resource_id
        }
    )

def rate_limit_exceeded_response(
    limit: int,
    window: str = "1 minute",
    retry_after: Optional[int] = None
) -> JSONResponse:
    """
    Response для rate limit exceeded
    """
    
    details = {
        "limit": limit,
        "window": window
    }
    
    if retry_after:
        details["retry_after_seconds"] = retry_after
    
    response = create_error_response(
        error_code="RATE_LIMIT_EXCEEDED",
        message=f"Rate limit exceeded: {limit} requests per {window}",
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        details=details
    )
    
    if retry_after:
        response.headers["Retry-After"] = str(retry_after)
    
    return response

# ==========================================
# STANDARD ERROR CODES
# ==========================================

class ErrorCodes:
    """Стандартні коди помилок"""
    
    # Authentication & Authorization
    UNAUTHORIZED = "UNAUTHORIZED"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    TOKEN_EXPIRED = "TOKEN_EXPIRED"
    INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
    
    # Validation
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INVALID_INPUT = "INVALID_INPUT"
    MISSING_FIELD = "MISSING_FIELD"
    
    # Resources
    NOT_FOUND = "NOT_FOUND"
    ALREADY_EXISTS = "ALREADY_EXISTS"
    CONFLICT = "CONFLICT"
    
    # Rate Limiting
    RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    
    # Server Errors
    INTERNAL_ERROR = "INTERNAL_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    DATABASE_ERROR = "DATABASE_ERROR"
    EXTERNAL_SERVICE_ERROR = "EXTERNAL_SERVICE_ERROR"

# ==========================================
# EXCEPTION HANDLERS
# ==========================================

def handle_database_error(e: Exception) -> JSONResponse:
    """Handler для database errors"""
    logger.error(f"Database error: {e}")
    
    return create_error_response(
        error_code=ErrorCodes.DATABASE_ERROR,
        message="Database operation failed",
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        details={"error": str(e) if logger.level <= logging.DEBUG else None}
    )

def handle_validation_error(e: Exception) -> JSONResponse:
    """Handler для validation errors"""
    logger.warning(f"Validation error: {e}")
    
    return create_error_response(
        error_code=ErrorCodes.VALIDATION_ERROR,
        message=str(e),
        status_code=status.HTTP_400_BAD_REQUEST
    )

# ==========================================
# RESPONSE HELPERS
# ==========================================

def success_response(
    message: str = "Operation successful",
    data: Optional[Any] = None,
    status_code: int = 200
) -> JSONResponse:
    """
    Простий success response
    """
    
    response_data = {"message": message}
    if data is not None:
        response_data.update(data)
    
    return create_response(
        data=response_data,
        status_code=status_code
    )

def created_response(
    resource_type: str,
    resource_id: Union[str, int],
    data: Optional[Any] = None,
    location: Optional[str] = None
) -> JSONResponse:
    """
    Response для створених ресурсів (201)
    """
    
    response_data = {
        "message": f"{resource_type} created successfully",
        "id": resource_id
    }
    
    if data:
        response_data["data"] = data
    
    response = create_response(
        data=response_data,
        status_code=status.HTTP_201_CREATED
    )
    
    if location:
        response.headers["Location"] = location
    
    return response

def deleted_response(
    resource_type: str,
    resource_id: Union[str, int]
) -> JSONResponse:
    """
    Response для видалених ресурсів (204 або 200)
    """
    
    return success_response(
        message=f"{resource_type} with id '{resource_id}' deleted successfully",
        status_code=status.HTTP_200_OK
    )

# ==========================================
# EXPORTS
# ==========================================

__all__ = [
    # Main response builders
    "create_response",
    "create_list_response",
    
    # Error responses
    "create_error_response",
    "validation_error_response",
    "permission_denied_response",
    "not_found_response",
    "rate_limit_exceeded_response",
    
    # Success responses
    "success_response",
    "created_response",
    "deleted_response",
    
    # Error codes
    "ErrorCodes",
    
    # Handlers
    "handle_database_error",
    "handle_validation_error",
    
    # Schemas
    "MetaData",
    "PaginationMeta",
    "Links",
    "ErrorDetail",
]