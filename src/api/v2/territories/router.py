"""
# Territory endpoints router
Created for API v2 Domain-Driven Architecture
"""
"""
Territories Router for API v2
Handles Explorer Mode endpoints for UC1
"""

from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth_service import get_current_user
from core.rbac_service import require_permission
from database.connections import get_db_session
from api.v2.territories.schemas import (
    AdminUnitResponse,
    AdminMetricsResponse, 
    H3HexagonResponse,
    BivariateConfig
)
from src.api.v2.territories.services import TerritoriesService

# Створюємо router з префіксом та тегами
router = APIRouter(
    prefix="/api/v2/territories",
    tags=["territories", "explorer"],
    responses={404: {"description": "Not found"}}
)

# Ініціалізуємо сервіс
territories_service = TerritoriesService()


@router.get(
    "/admin/geometries/all",
    response_model=List[AdminUnitResponse],
    summary="Отримати всі геометрії адмінодиниць",
    description="Повертає геометрії всіх областей, районів та громад України"
)
async def get_all_admin_geometries(
    level: Optional[str] = Query("all", description="Рівень: oblast, raion, gromada, all"),
    simplified: bool = Query(True, description="Спрощені геометрії для швидшого завантаження"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units")),
    db: AsyncSession = Depends(get_db_session)
) -> List[AdminUnitResponse]:
    """
    Завантажує всі адмінодиниці при старті додатку.
    Використовується для початкової ініціалізації карти.
    
    Parameters:
    - level: Фільтр по рівню адмінодиниці
    - simplified: Якщо True, повертає спрощені геометрії (ST_Simplify)
    
    Returns:
    - Список адмінодиниць з геометріями в GeoJSON форматі
    """
    try:
        return await territories_service.get_admin_geometries(
            db=db,
            level=level,
            simplified=simplified
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/admin/metrics/all",
    response_model=List[AdminMetricsResponse],
    summary="Отримати метрики для всіх адмінодиниць",
    description="Повертає розраховані метрики та bivariate bins з ClickHouse"
)
async def get_all_admin_metrics(
    metric_x: str = Query("population", description="Метрика для осі X"),
    metric_y: str = Query("income_index", description="Метрика для осі Y"),
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_admin_units"))
) -> List[AdminMetricsResponse]:
    """
    Завантажує всі метрики для адмінодиниць.
    Включає pre-calculated bivariate bins для швидкої візуалізації.
    
    Parameters:
    - metric_x: Основна метрика для bivariate map
    - metric_y: Додаткова метрика для bivariate map
    
    Returns:
    - Список метрик з bivariate codes
    """
    try:
        return await territories_service.get_admin_metrics(
            metric_x=metric_x,
            metric_y=metric_y
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/hexagons/{admin_id}",
    response_model=List[H3HexagonResponse],
    summary="Отримати H3 гексагони для адмінодиниці",
    description="Повертає H3 гексагони з метриками для вибраної території"
)
async def get_hexagons_for_admin(
    admin_id: str,
    resolution: int = Query(7, ge=7, le=10, description="H3 резолюція (7-10)"),
    include_metrics: bool = Query(True, description="Включити метрики"),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session)
) -> List[H3HexagonResponse]:
    """
    Завантажує H3 гексагони для конкретної адмінодиниці.
    Без геометрій - тільки h3_index, Deck.gl сам згенерує полігони.
    
    Parameters:
    - admin_id: ID адмінодиниці (KOATUU код)
    - resolution: H3 резолюція (7=області, 10=квартали)
    - include_metrics: Чи включати метрики
    
    Returns:
    - Список H3 гексагонів з метриками
    """
    # Перевірка доступу до детальних H3 даних
    if resolution >= 9:
        await require_permission("core.view_h3_detailed")(current_user)
    else:
        await require_permission("core.view_h3_basic")(current_user)
    
    try:
        return await territories_service.get_h3_hexagons(
            db=db,
            admin_id=admin_id,
            resolution=resolution,
            include_metrics=include_metrics
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/bivariate/config",
    response_model=BivariateConfig,
    summary="Отримати конфігурацію bivariate map",
    description="Повертає налаштування для bivariate візуалізації"
)
async def get_bivariate_config() -> BivariateConfig:
    """
    Повертає конфігурацію кольорової схеми для bivariate map.
    Використовується для синхронізації backend та frontend.
    
    Returns:
    - Конфігурація з bins та кольорами
    """
    return territories_service.get_bivariate_config()


@router.post(
    "/hexagons/details",
    response_model=Dict[str, Any],
    summary="Отримати детальну інформацію про гексагони",
    description="Batch запит для отримання деталей по декількох H3 гексагонах"
)
async def get_hexagon_details(
    h3_indexes: List[str],
    current_user: dict = Depends(get_current_user),
    _: Any = Depends(require_permission("core.view_h3_detailed")),
    db: AsyncSession = Depends(get_db_session)
) -> Dict[str, Any]:
    """
    Повертає детальну інформацію для вибраних H3 гексагонів.
    Використовується для hover cards та side panels.
    
    Parameters:
    - h3_indexes: Список H3 індексів
    
    Returns:
    - Словник з детальною інформацією по кожному гексагону
    """
    if len(h3_indexes) > 100:
        raise HTTPException(
            status_code=400,
            detail="Maximum 100 hexagons per request"
        )
    
    try:
        return await territories_service.get_hexagon_details(
            db=db,
            h3_indexes=h3_indexes
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Health check для territories domain
@router.get(
    "/health",
    summary="Health check для territories API",
    tags=["health"]
)
async def health_check():
    """Перевірка працездатності territories endpoints"""
    return {
        "status": "healthy",
        "domain": "territories",
        "version": "2.0",
        "endpoints": [
            "/admin/geometries/all",
            "/admin/metrics/all",
            "/hexagons/{admin_id}",
            "/bivariate/config"
        ]
    }