"""
scripts/etl/clickhouse/__init__.py
Ініціалізація модуля ClickHouse ETL
Робить папку Python пакетом та експортує основні компоненти
"""

# Версія модуля
__version__ = "1.0.0"

# Опис модуля
__description__ = "ETL pipeline for PostGIS to ClickHouse data migration"

# Автор
__author__ = "GeoRetail Team"

# Імпорт основних компонентів (додамо пізніше)
# from .config import PG_CONFIG, CH_CONFIG
# from .utils import PostgresConnector, ClickHouseConnector

# Список експортованих об'єктів
__all__ = [
    "__version__",
    "__description__",
]