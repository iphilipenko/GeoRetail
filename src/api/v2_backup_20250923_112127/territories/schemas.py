"""
Pydantic Schemas for Territories Domain v2
Complete data validation and serialization models
"""

from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, ConfigDict, validator
from datetime import datetime
from decimal import Decimal
from enum import Enum


# ================== Enums ==================

class AdminLevel(str, Enum):
    """Рівні адміністративних одиниць"""
    OBLAST = "oblast"
    RAION = "raion"
    GROMADA = "gromada"
    ALL = "all"


class POICategory(str, Enum):
    """Категорії точок інтересу"""
    RETAIL = "retail"
    FOOD = "food"
    TRANSPORT = "transport"
    EDUCATION = "education"
    HEALTH = "health"
    FINANCE = "finance"
    ENTERTAINMENT = "entertainment"
    SERVICES = "services"
    OTHER = "other"


class StoreFormat(str, Enum):
    """Формати магазинів"""
    HYPERMARKET = "hypermarket"
    SUPERMARKET = "supermarket"
    MINIMARKET = "minimarket"
    CONVENIENCE = "convenience"
    DISCOUNTER = "discounter"
    SPECIALTY = "specialty"


# ================== Request Models ==================

class AdminUnitFilter(BaseModel):
    """Фільтри для адмінодиниць"""
    level: Optional[AdminLevel] = Field(None, description="Рівень: oblast, raion, gromada")
    region_id: Optional[str] = Field(None, description="KOATUU код регіону")
    name_contains: Optional[str] = Field(None, description="Пошук по назві")
    population_min: Optional[int] = Field(None, description="Мінімальне населення")
    population_max: Optional[int] = Field(None, description="Максимальне населення")
    
    model_config = ConfigDict(from_attributes=True)


class H3QueryParams(BaseModel):
    """Параметри запиту для H3 гексагонів"""
    resolution: int = Field(7, ge=4, le=10, description="H3 резолюція")
    bounds: Optional[str] = Field(None, description="minLng,minLat,maxLng,maxLat")
    limit: int = Field(10000, le=50000, description="Максимум гексагонів")
    
    @validator('bounds')
    def validate_bounds(cls, v):
        if v:
            coords = v.split(',')
            if len(coords) != 4:
                raise ValueError('bounds must have exactly 4 coordinates')
            try:
                [float(c) for c in coords]
            except ValueError:
                raise ValueError('bounds must contain valid numbers')
        return v


class TerritorySearchRequest(BaseModel):
    """Запит на пошук територій"""
    query: Optional[str] = Field(None, min_length=2, description="Пошуковий запит")
    filters: Optional[AdminUnitFilter] = None
    bounds: Optional[str] = Field(None, description="Bounding box")
    resolution: int = Field(7, ge=4, le=10, description="H3 resolution для аналізу")
    metrics: List[str] = Field(default_factory=list, description="Метрики для аналізу")
    limit: int = Field(20, le=100)
    offset: int = Field(0, ge=0)
    
    model_config = ConfigDict(from_attributes=True)


# ================== Response Models - Core ==================

class GeoJSONGeometry(BaseModel):
    """GeoJSON геометрія"""
    type: str = Field(..., description="Тип геометрії: Polygon, MultiPolygon, Point")
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
    
    # Додаткові дані
    children: Optional[List['AdminUnitResponse']] = Field(None, description="Дочірні одиниці")
    stats: Optional[Dict[str, Any]] = Field(None, description="Статистика")
    
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


# ================== H3 Hexagon Models ==================

class H3HexagonResponse(BaseModel):
    """H3 гексагон без геометрії (Deck.gl згенерує)"""
    h3_index: str = Field(..., description="H3 індекс гексагону")
    resolution: int = Field(..., ge=4, le=10, description="H3 резолюція")
    
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
    
    # Додаткові дані
    neighbors: Optional[List[str]] = Field(None, description="H3 індекси сусідів")
    poi_data: Optional[Dict[str, int]] = Field(None, description="POI statistics")
    competition_data: Optional[Dict[str, Any]] = Field(None, description="Competition analysis")
    
    model_config = ConfigDict(from_attributes=True)


class H3GridResponse(BaseModel):
    """Відповідь для масового завантаження H3 гексагонів"""
    h3_index: str = Field(..., description="H3 індекс")
    resolution: int = Field(..., description="H3 резолюція")
    
    # Компактні метрики для швидкого рендерингу
    value: float = Field(..., description="Основна метрика для візуалізації")
    bivar_code: str = Field(..., description="Bivariate bin code")
    
    # Опціональні метрики
    population: Optional[float] = None
    income: Optional[float] = None
    retail_density: Optional[float] = None
    
    model_config = ConfigDict(from_attributes=True, extra="allow")


class HexagonDetailsResponse(BaseModel):
    """Детальна інформація про H3 гексагон"""
    h3_index: str = Field(..., description="H3 індекс")
    resolution: int = Field(..., description="H3 резолюція")
    
    # Локація
    center_lat: float = Field(..., description="Широта центру")
    center_lon: float = Field(..., description="Довгота центру")
    address: Optional[str] = Field(None, description="Адреса")
    
    # Метрики з percentiles
    metrics: Dict[str, Dict[str, Union[float, int]]] = Field(
        ...,
        description="Метрики з value та percentile"
    )
    
    # POI аналіз
    poi_summary: Dict[str, int] = Field(
        default_factory=dict, 
        description="POI по категоріях"
    )
    nearby_competitors: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Список найближчих конкурентів"
    )
    
    # Доступність
    transport_access: Dict[str, Any] = Field(
        default_factory=dict,
        description="Доступність транспорту"
    )
    walkability_score: Optional[float] = Field(
        None,
        ge=0, 
        le=1,
        description="Оцінка пішохідної доступності"
    )
    
    # Демографія
    demographics: Dict[str, Any] = Field(
        default_factory=dict,
        description="Демографічні показники"
    )
    
    # ML insights
    insights: Dict[str, Any] = Field(
        default_factory=dict,
        description="ML аналітика та прогнози"
    )
    recommendations: List[str] = Field(
        default_factory=list,
        description="Рекомендації на основі аналізу"
    )
    
    model_config = ConfigDict(from_attributes=True)


# ================== POI Models ==================

class POIResponse(BaseModel):
    """Відповідь з даними точки інтересу"""
    id: str = Field(..., description="Унікальний ID POI")
    name: str = Field(..., description="Назва")
    category: POICategory = Field(..., description="Категорія")
    subcategory: Optional[str] = Field(None, description="Підкатегорія")
    
    # Локація
    lat: float = Field(..., description="Широта")
    lon: float = Field(..., description="Довгота")
    address: Optional[str] = Field(None, description="Адреса")
    h3_index: Optional[str] = Field(None, description="H3 індекс локації")
    
    # Додаткова інформація
    brand: Optional[str] = Field(None, description="Бренд")
    opening_hours: Optional[str] = Field(None, description="Години роботи")
    phone: Optional[str] = Field(None, description="Телефон")
    website: Optional[str] = Field(None, description="Вебсайт")
    
    # Метрики
    rating: Optional[float] = Field(None, ge=0, le=5, description="Рейтинг")
    reviews_count: Optional[int] = Field(None, description="Кількість відгуків")
    popularity_score: Optional[float] = Field(None, description="Популярність")
    
    # OSM tags
    tags: Dict[str, Any] = Field(default_factory=dict, description="OSM tags")
    
    # Метадані
    source: str = Field("osm", description="Джерело даних")
    last_updated: Optional[datetime] = None
    
    model_config = ConfigDict(from_attributes=True)


# ================== Competition Models ==================

class CompetitorResponse(BaseModel):
    """Відповідь з даними про конкурента"""
    store_id: str = Field(..., description="ID магазину")
    brand: str = Field(..., description="Бренд/мережа")
    name: str = Field(..., description="Назва магазину")
    format: StoreFormat = Field(..., description="Формат магазину")
    
    # Локація
    lat: float = Field(..., description="Широта")
    lon: float = Field(..., description="Довгота")
    address: str = Field(..., description="Адреса")
    distance_km: float = Field(..., description="Відстань в км")
    
    # Характеристики магазину
    store_size_m2: Optional[float] = Field(None, description="Площа магазину")
    sku_count: Optional[int] = Field(None, description="Кількість SKU")
    employees_count: Optional[int] = Field(None, description="Кількість працівників")
    parking_spaces: Optional[int] = Field(None, description="Парковочні місця")
    
    # Фінансові показники
    estimated_revenue: Optional[float] = Field(None, description="Оцінка виторгу")
    market_share: Optional[float] = Field(None, description="Частка ринку %")
    price_index: Optional[float] = Field(None, description="Індекс цін")
    
    # Конкурентна оцінка
    threat_level: str = Field("medium", description="Рівень загрози: low/medium/high")
    overlap_score: float = Field(0.5, description="Перетин аудиторії (0-1)")
    cannibalization_risk: float = Field(0.0, description="Ризик канібалізації (0-1)")
    
    # Додаткові дані
    opening_hours: Optional[str] = None
    last_renovation: Optional[datetime] = None
    services: List[str] = Field(default_factory=list, description="Додаткові послуги")
    
    model_config = ConfigDict(from_attributes=True)


# ================== Analytics Models ==================

class TerritoryStatsResponse(BaseModel):
    """Агрегована статистика території"""
    territory_id: str = Field(..., description="ID території (KOATUU або H3)")
    territory_name: Optional[str] = Field(None, description="Назва території")
    analysis_period: str = Field(..., description="Період аналізу")
    
    # Демографія
    demographics: Dict[str, Any] = Field(..., description="Демографічні показники")
    population_total: int
    households_count: int
    average_income: float
    age_distribution: Dict[str, float]
    
    # Роздрібна торгівля
    retail_metrics: Dict[str, Any] = Field(..., description="Метрики роздрібної торгівлі")
    total_stores: int
    stores_by_format: Dict[str, int]
    retail_density_per_1000: float
    average_store_size: float
    
    # Конкуренція
    competition_analysis: Dict[str, Any] = Field(..., description="Аналіз конкуренції")
    main_competitors: List[str]
    market_concentration: float
    competition_intensity: float
    market_gaps: List[str]
    
    # Доступність
    accessibility_metrics: Dict[str, Any] = Field(..., description="Показники доступності")
    public_transport_coverage: float
    road_density: float
    walkability_score: float
    average_commute_time: float
    
    # Потенціал
    potential_assessment: Dict[str, Any] = Field(..., description="Оцінка потенціалу")
    revenue_potential_score: float
    growth_rate_forecast: float
    market_saturation: float
    expansion_opportunities: int
    
    # ML прогнози
    ml_predictions: Optional[Dict[str, Any]] = Field(None, description="ML прогнози")
    predicted_revenue: Optional[float] = None
    confidence_interval: Optional[List[float]] = None
    risk_factors: Optional[List[str]] = None
    
    # Рекомендації
    recommendations: List[str] = Field(default_factory=list, description="Рекомендації")
    optimal_store_format: Optional[str] = None
    suggested_location_types: List[str] = Field(default_factory=list)
    
    # Метадані
    calculation_timestamp: datetime = Field(default_factory=datetime.utcnow)
    data_quality_score: float = Field(1.0, ge=0, le=1)
    data_sources: List[str] = Field(default_factory=list)
    
    model_config = ConfigDict(from_attributes=True)


class TerritorySearchResponse(BaseModel):
    """Відповідь пошуку територій"""
    total: int
    items: List[AdminUnitResponse]
    query: Optional[str] = None
    filters_applied: Optional[Dict[str, Any]] = None
    execution_time_ms: float
    
    model_config = ConfigDict(from_attributes=True)


# ================== Configuration Models ==================

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


# ================== Comparison Models ==================

class TerritoryComparison(BaseModel):
    """Модель для порівняння територій"""
    territories: List[str] = Field(..., min_length=2, max_length=5)
    metrics: List[str] = Field(..., description="Метрики для порівняння")
    comparison_type: str = Field("radar", description="radar, table, chart")
    
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


# ================== Update model references ==================

# For circular references
AdminUnitResponse.model_rebuild()