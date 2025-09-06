"""
scripts/etl/clickhouse/config.py
Конфігурація для ETL PostGIS → ClickHouse
Містить параметри підключення до обох баз даних
"""

import os
from dotenv import load_dotenv

# Завантажуємо змінні середовища з .env файлу
load_dotenv()

# PostGIS source - джерело даних
PG_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'georetail'),
    'user': os.getenv('POSTGRES_USER', 'georetail_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'georetail_secure_2024')
}

# ClickHouse target - цільова база даних
# Використовуємо webuser з паролем замість default
CH_CONFIG = {
    'host': os.getenv('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.getenv('CLICKHOUSE_NATIVE_PORT', 32769)),  # Native port для Python
    'database': os.getenv('CLICKHOUSE_DATABASE', 'geo_analytics'),
    'user': os.getenv('CLICKHOUSE_USER', 'webuser'),  # Змінено на webuser
    'password': os.getenv('CLICKHOUSE_PASSWORD', 'password123')  # Додано пароль
}

# ETL налаштування
BATCH_SIZE = int(os.getenv('ETL_BATCH_SIZE', 10000))  # Розмір батчу для обробки
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')  # Рівень логування
MAX_RETRIES = 3  # Максимальна кількість спроб при помилках
RETRY_DELAY = 5  # Затримка між спробами (секунди)

# Додаткові налаштування для великих таблиць
LARGE_TABLE_BATCH_SIZE = 50000  # Більший батч для великих таблиць
PARALLEL_WORKERS = 4  # Кількість паралельних процесів

# Налаштування для ClickHouse оптимізації
CH_INSERT_SETTINGS = {
    'insert_quorum': 0,
    'insert_quorum_parallel': 1,
    'max_threads': 8,
    'max_insert_threads': 8
}

# Список таблиць для ETL з пріоритетами
ETL_TABLES = {
    'admin_analytics': {
        'priority': 1,
        'source_schema': 'osm_ukraine',
        'source_table': 'admin_boundaries',
        'target_table': 'admin_analytics',
        'batch_size': BATCH_SIZE
    },
    'h3_analytics': {
        'priority': 3,
        'source_schema': 'osm_ukraine',
        'source_table': 'h3_admin_mapping',
        'target_table': 'h3_analytics',
        'batch_size': LARGE_TABLE_BATCH_SIZE
    }
}

# Конфігурація для розрахунку bins
BINS_CONFIG = {
    'admin_levels': [4, 5, 6, 7, 8, 9],  # Рівні адмінодиниць для обробки
    'terciles': 3,  # Кількість bins для terciles
    'quintiles': 5,  # Кількість bins для quintiles
    'metrics_for_bins': [
        'population_density',
        'economic_activity_index',
        'competitor_density',
        'transport_accessibility_score'
    ]
}

# Bivariate комбінації для візуалізації
BIVARIATE_COMBINATIONS = [
    ('population_density', 'economic_activity_index'),
    ('competitor_density', 'transport_accessibility_score'),
    ('poi_density', 'retail_potential_score'),
    ('residential_coverage', 'commercial_activity_score')
]

def get_connection_string(config_type='pg'):
    """
    Повертає рядок підключення для логування (без пароля)
    
    Args:
        config_type: 'pg' для PostGIS або 'ch' для ClickHouse
    
    Returns:
        Безпечний рядок підключення
    """
    if config_type == 'pg':
        return f"postgresql://{PG_CONFIG['user']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['database']}"
    else:
        return f"clickhouse://{CH_CONFIG['user']}@{CH_CONFIG['host']}:{CH_CONFIG['port']}/{CH_CONFIG['database']}"

# Експортуємо конфігурації
__all__ = [
    'PG_CONFIG',
    'CH_CONFIG',
    'BATCH_SIZE',
    'LOG_LEVEL',
    'ETL_TABLES',
    'BINS_CONFIG',
    'BIVARIATE_COMBINATIONS',
    'get_connection_string'
]