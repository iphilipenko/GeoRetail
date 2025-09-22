"""
# Pydantic models for territories
Created for API v2 Domain-Driven Architecture
"""
"""
Pydantic Schemas for Territories Domain
Data validation and serialization models
"""

from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, ConfigDict, validator
from datetime import datetime
from decimal import Decimal


# ================== Request Models ==================

class AdminUnitFilter(BaseModel):
    """Фільтри для адмінодиниць"""
    level: Optional[str] = Field(None, description="oblast, raion, gromada")
    region_id: Optional[str] = Field(None, description="KOATUU код регіону")
    name_contains: Optional[str] = Field(None, description="Пошук по назві")
    
    model_config = ConfigDict(from_attributes=True)


class H3QueryParams(BaseModel):
    """Параметри запиту для H3 гексагонів"""
    resolution: int = Field(7, ge=7, le=10, description="H3 резолюція")
    bbox: Optional[List[float]] = Field(None, description="[minLng, minLat, maxLng, maxLat]")
    limit: int = Field(10000, le=50000, description="Максимум гексагонів")
    
    @validator('bbox')
    def validate_bbox(cls, v):
        if v and len(v) != 4:
            raise ValueError('bbox must have exactly 4 coordinates')
        return v


# ================== Response Models ==================

class GeoJSONGeometry(BaseModel):
    """GeoJSON геометрія"""
    type: str = Field(..., description="Тип геометрії: Polygon, MultiPolygon")
    coordinates: List = Field(..., description="Координати геометрії")


class AdminUnitResponse(BaseModel):
    """Відповідь з геометрією адмінодиниці"""
    id: str = Field(..., description="KOATUU код")
    name_uk: str = Field(..., description="Назва українською")
    name_en: Optional[str] = Field(None, description="Назва англійською")
    level: str = Field(..., description="Рівень: oblast, raion, gromada")
    parent_id: Optional[str] = Field(None, description="ID батьківської одиниці")
    
    # Геометрія
    geometry: GeoJSONGeometry = Field(..., description="GeoJSON геометрія")
    center_lat: float = Field(..., description="Широта центроїду")
    center_lon: float = Field(..., description="Довгота центроїду")
    area_km2: float = Field(..., description="Площа в км²")
    
    # Базові метрики
    population: Optional[int] = Field(None, description="Населення")
    settlement_count: Optional[int] = Field(None, description="Кількість населених пунктів")
    
    model_config = ConfigDict(from_attributes=True)


class AdminMetricsResponse(BaseModel):
    """Відповідь з метриками адмінодиниці"""
    id: str = Field(..., description="KOATUU код")
    name_uk: str = Field(..., description="Назва")
    level: str = Field(..., description="Рівень адмінодиниці")
    
    # Core метрики (8 основних)
    population: int = Field(..., description="Населення")
    population_density: float = Field(..., description="Щільність населення")
    income_index: float = Field(..., description="Індекс доходів (0-100)")
    retail_density: float = Field(..., description="Щільність роздрібної торгівлі")
    competitor_count: int = Field(..., description="Кількість конкурентів")
    traffic_index: float = Field(..., description="Індекс трафіку")
    accessibility_score: float = Field(..., description="Оцінка доступності")
    growth_potential: float = Field(..., description="Потенціал зростання")
    
    # Bivariate bins (pre-calculated)
    bivar_code: str = Field(..., description="Код bivariate bin: 11-33")
    bivar_x_bin: int = Field(..., ge=1, le=3, description="Bin для X метрики")
    bivar_y_bin: int = Field(..., ge=1, le=3, description="Bin для Y метрики")
    
    # Додаткові метрики
    poi_total: Optional[int] = None
    road_density_km: Optional[float] = None
    public_transport_stops: Optional[int] = None
    average_speed_kmh: Optional[float] = None
    
    # Метадані
    last_updated: datetime = Field(..., description="Остання дата оновлення")
    data_quality_score: float = Field(1.0, description="Якість даних (0-1)")
    
    model_config = ConfigDict(from_attributes=True)


class H3HexagonResponse(BaseModel):
    """H3 гексагон без геометрії (Deck.gl згенерує)"""
    h3_index: str = Field(..., description="H3 індекс гексагону")
    resolution: int = Field(..., ge=7, le=10, description="H3 резолюція")
    
    # Прив'язка до адмінодиниць
    oblast_id: Optional[str] = None
    raion_id: Optional[str] = None
    gromada_id: Optional[str] = None
    
    # Core метрики
    population: Optional[float] = None
    income_level: Optional[float] = None
    retail_count: Optional[int] = None
    competitor_intensity: Optional[float] = None
    traffic_flow: Optional[float] = None
    
    # Bivariate (розраховано на backend)
    bivar_code: str = Field("22", description="Bivariate bin code")
    
    # ML прогнози (якщо є)
    revenue_potential: Optional[float] = None
    risk_score: Optional[float] = None
    recommendation_score: Optional[float] = None
    
    model_config = ConfigDict(from_attributes=True)


class BivariateConfig(BaseModel):
    """Конфігурація для bivariate map"""
    
    class ColorScheme(BaseModel):
        """Схема кольорів"""
        name: str = Field("NeonGlow", description="Назва теми")
        colors: Dict[str, str] = Field(..., description="Mapping: code -> hex color")
        
    class BinConfig(BaseModel):
        """Налаштування bins"""
        x_breaks: List[float] = Field(..., description="Границі для X метрики")
        y_breaks: List[float] = Field(..., description="Границі для Y метрики")
        labels: Dict[str, str] = Field(..., description="Підписи для bins")
    
    color_scheme: ColorScheme
    bin_config: BinConfig
    current_metrics: Dict[str, str] = Field(
        default={
            "x": "population",
            "y": "income_index"
        }
    )
    
    model_config = ConfigDict(from_attributes=True)


class HexagonDetailsResponse(BaseModel):
    """Детальна інформація про H3 гексагон"""
    h3_index: str
    resolution: int
    
    # Локація
    center_lat: float
    center_lon: float
    address: Optional[str] = None
    
    # Метрики з percentiles
    metrics: Dict[str, Dict[str, Union[float, int]]] = Field(
        ...,
        description="Метрики з value та percentile"
    )
    
    # POI аналіз
    poi_summary: Dict[str, int] = Field(..., description="POI по категоріях")
    nearby_competitors: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Доступність
    transport_access: Dict[str, Any] = Field(default_factory=dict)
    walkability_score: Optional[float] = None
    
    # Демографія
    demographics: Dict[str, Any] = Field(default_factory=dict)
    
    # ML insights
    insights: Dict[str, Any] = Field(default_factory=dict)
    recommendations: List[str] = Field(default_factory=list)
    
    model_config = ConfigDict(from_attributes=True)


# ================== Aggregate Models ==================

class TerritoryComparison(BaseModel):
    """Модель для порівняння територій"""
    territories: List[str] = Field(..., min_items=2, max_items=5)
    metrics: List[str] = Field(..., description="Метрики для порівняння")
    comparison_type: str = Field("radar", description="radar, table, chart")
    
    model_config = ConfigDict(from_attributes=True)


class TerritorySearchRequest(BaseModel):
    """Запит на пошук територій"""
    query: str = Field(..., min_length=2, description="Пошуковий запит")
    filters: Optional[AdminUnitFilter] = None
    limit: int = Field(20, le=100)
    offset: int = Field(0, ge=0)
    
    model_config = ConfigDict(from_attributes=True)


class TerritorySearchResponse(BaseModel):
    """Відповідь пошуку територій"""
    total: int
    items: List[AdminUnitResponse]
    query: str
    execution_time_ms: float
    
    model_config = ConfigDict(from_attributes=True)


# ================== Error Models ==================

class ErrorResponse(BaseModel):
    """Стандартна модель помилки"""
    error: str = Field(..., description="Тип помилки")
    detail: str = Field(..., description="Опис помилки")
    status_code: int = Field(..., description="HTTP status code")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)


class ValidationErrorResponse(BaseModel):
    """Помилка валідації даних"""
    error: str = Field("validation_error")
    detail: str
    field_errors: Dict[str, List[str]]
    status_code: int = Field(422)
    
    model_config = ConfigDict(from_attributes=True)