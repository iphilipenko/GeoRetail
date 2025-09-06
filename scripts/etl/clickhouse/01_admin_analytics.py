"""
scripts/etl/clickhouse/01_admin_analytics.py
ETL для міграції адміністративних одиниць з PostGIS в ClickHouse
ВИПРАВЛЕНА ВЕРСІЯ - з коректною обробкою None значень
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config import PG_CONFIG, CH_CONFIG, BATCH_SIZE
from utils import PostgresConnector, ClickHouseConnector, ETLProgress

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdminAnalyticsETL:
    """
    ETL клас для міграції адміністративних одиниць
    Обробляє дані з admin_boundaries та агрегує метрики
    """
    
    def __init__(self):
        """Ініціалізація ETL процесу"""
        self.pg_conn = PostgresConnector(PG_CONFIG)
        self.ch_conn = ClickHouseConnector(CH_CONFIG)
        self.start_time = datetime.now()
        self.records_processed = 0
        
    def extract_admin_units(self) -> List[Dict]:
        """
        Витягує адміністративні одиниці з PostGIS
        
        Returns:
            Список словників з даними адмінодиниць
        """
        logger.info("📥 Витягування адміністративних одиниць з PostGIS...")
        
        query = """
        WITH admin_data AS (
            SELECT 
                -- Ідентифікація
                ab.id as admin_id,
                ab.admin_level,
                ab.name as admin_name,
                ab.name_uk as admin_name_uk,
                ab.parent_id,
                ab.osm_id,
                
                -- Базові характеристики
                COALESCE(ab.area_km2, ST_Area(ab.geometry::geography) / 1000000) as area_km2,
                ST_Perimeter(ab.geometry::geography) / 1000 as perimeter_km,
                
                -- Геометрія для подальших розрахунків
                ab.geometry
                
            FROM osm_ukraine.admin_boundaries ab
            WHERE ab.admin_level IN (4, 5, 6, 7, 8, 9)
        )
        SELECT * FROM admin_data
        ORDER BY admin_level, admin_id
        """
        
        with self.pg_conn.connect():
            results = self.pg_conn.execute_query(query)
            logger.info(f"✅ Витягнуто {len(results)} адмінодиниць")
            return results
    
    def enrich_with_h3_counts(self, admin_units: List[Dict]) -> List[Dict]:
        """
        Додає підрахунок H3 гексагонів для кожної адмінодиниці
        """
        logger.info("🔢 Розрахунок H3 гексагонів для адмінодиниць...")
        
        with self.pg_conn.connect(autocommit=True):
            progress = ETLProgress(len(admin_units), "H3 підрахунок")
            
            for unit in admin_units:
                try:
                    h3_query = """
                    SELECT 
                        h3_resolution,
                        COUNT(*) as hex_count
                    FROM osm_ukraine.h3_admin_mapping
                    WHERE 
                        CASE %s
                            WHEN 4 THEN oblast_id = %s
                            WHEN 5 THEN raion_id = %s
                            WHEN 6 THEN gromada_id = %s
                            WHEN 7 THEN settlement_id = %s
                            WHEN 8 THEN settlement_id = %s
                            WHEN 9 THEN settlement_id = %s
                        END
                    GROUP BY h3_resolution
                    """
                    
                    self.pg_conn.cursor.execute(
                        h3_query,
                        (unit['admin_level'], unit['admin_id'], unit['admin_id'], 
                         unit['admin_id'], unit['admin_id'], unit['admin_id'], unit['admin_id'])
                    )
                    
                    h3_counts = self.pg_conn.cursor.fetchall()
                    
                    # Ініціалізуємо нулями
                    unit['hex_count_r7'] = 0
                    unit['hex_count_r8'] = 0
                    unit['hex_count_r9'] = 0
                    unit['hex_count_r10'] = 0
                    
                    # Заповнюємо реальними значеннями
                    for row in h3_counts:
                        if row['h3_resolution'] == 7:
                            unit['hex_count_r7'] = row['hex_count']
                        elif row['h3_resolution'] == 8:
                            unit['hex_count_r8'] = row['hex_count']
                        elif row['h3_resolution'] == 9:
                            unit['hex_count_r9'] = row['hex_count']
                        elif row['h3_resolution'] == 10:
                            unit['hex_count_r10'] = row['hex_count']
                            
                except Exception as e:
                    logger.warning(f"⚠️ Помилка H3 для {unit.get('admin_name', 'unknown')}: {str(e)[:100]}")
                    unit['hex_count_r7'] = 0
                    unit['hex_count_r8'] = 0
                    unit['hex_count_r9'] = 0
                    unit['hex_count_r10'] = 0
                
                progress.update(1)
        
        return admin_units
    
    def enrich_with_population(self, admin_units: List[Dict]) -> List[Dict]:
        """
        Додає точні дані про населення з demographics.h3_population
        """
        logger.info("👥 Розрахунок населення для адмінодиниць...")
        
        with self.pg_conn.connect(autocommit=True):
            progress = ETLProgress(len(admin_units), "Населення")
            
            for unit in admin_units:
                try:
                    pop_query = """
                    SELECT 
                        SUM(hp.population) as total_population,
                        AVG(hp.population_density) as avg_density,
                        AVG(hp.population) as avg_population,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hp.population) as median_population,
                        STDDEV(hp.population) as std_population
                    FROM demographics.h3_population hp
                    JOIN osm_ukraine.h3_admin_mapping ham ON ham.h3_index = hp.hex_id
                    WHERE ham.h3_resolution = 8
                        AND CASE %s
                            WHEN 4 THEN ham.oblast_id = %s
                            WHEN 5 THEN ham.raion_id = %s
                            WHEN 6 THEN ham.gromada_id = %s
                            WHEN 7 THEN ham.settlement_id = %s
                            WHEN 8 THEN ham.settlement_id = %s
                            WHEN 9 THEN ham.settlement_id = %s
                        END
                    """
                    
                    self.pg_conn.cursor.execute(
                        pop_query,
                        (unit['admin_level'], unit['admin_id'], unit['admin_id'],
                         unit['admin_id'], unit['admin_id'], unit['admin_id'], unit['admin_id'])
                    )
                    
                    result = self.pg_conn.cursor.fetchone()
                    
                    if result and result['total_population']:
                        unit['population_estimated'] = float(result['total_population'])
                        unit['h3_population_avg'] = float(result['avg_population']) if result['avg_population'] else None
                        unit['h3_population_median'] = float(result['median_population']) if result['median_population'] else None
                        unit['h3_population_std'] = float(result['std_population']) if result['std_population'] else None
                        
                        if unit['area_km2'] and unit['area_km2'] > 0:
                            unit['population_density'] = unit['population_estimated'] / unit['area_km2']
                        elif result['avg_density']:
                            unit['population_density'] = float(result['avg_density'])
                        else:
                            unit['population_density'] = None
                    else:
                        unit['population_estimated'] = None
                        unit['population_density'] = None
                        unit['h3_population_avg'] = None
                        unit['h3_population_median'] = None
                        unit['h3_population_std'] = None
                        
                except Exception as e:
                    logger.warning(f"⚠️ Помилка населення для {unit.get('admin_name', 'unknown')}: {str(e)[:100]}")
                    unit['population_estimated'] = None
                    unit['population_density'] = None
                    unit['h3_population_avg'] = None
                    unit['h3_population_median'] = None
                    unit['h3_population_std'] = None
                
                progress.update(1)
        
        return admin_units
    
    def enrich_with_poi_metrics(self, admin_units: List[Dict]) -> List[Dict]:
        """
        Додає метрики POI для кожної адмінодиниці
        ВИПРАВЛЕНО: використовує secondary_category замість shop_type
        """
        logger.info("📍 Розрахунок POI метрик для адмінодиниць...")
        
        with self.pg_conn.connect(autocommit=True):
            progress = ETLProgress(len(admin_units), "POI метрики")
            
            for unit in admin_units:
                try:
                    # ВИПРАВЛЕНО: використовуємо secondary_category замість shop_type
                    poi_query = """
                    SELECT 
                        COUNT(*) as poi_total,
                        COUNT(*) FILTER (WHERE functional_group = 'competitor') as competitors_total,
                        COUNT(*) FILTER (WHERE functional_group = 'competitor' AND secondary_category = 'supermarket') as comp_supermarket,
                        COUNT(*) FILTER (WHERE functional_group = 'competitor' AND secondary_category = 'convenience') as comp_convenience,
                        COUNT(*) FILTER (WHERE functional_group = 'competitor' AND secondary_category IN ('kiosk', 'variety_store')) as comp_minimarket,
                        COUNT(*) FILTER (WHERE primary_category = 'retail') as retail_count,
                        COUNT(*) FILTER (WHERE primary_category = 'food_service') as food_count,
                        COUNT(*) FILTER (WHERE primary_category IN ('amenity', 'healthcare')) as services_count,
                        COUNT(*) FILTER (WHERE primary_category = 'transport') as transport_count,
                        COUNT(*) FILTER (WHERE primary_category = 'education') as social_count,
                        COUNT(DISTINCT brand_normalized) FILTER (WHERE brand_normalized IS NOT NULL) as unique_brands
                    FROM osm_ukraine.poi_processed pp
                    WHERE ST_Contains(
                        (SELECT geometry FROM osm_ukraine.admin_boundaries WHERE id = %s),
                        pp.geom
                    )
                    """
                    
                    self.pg_conn.cursor.execute(poi_query, (unit['admin_id'],))
                    poi_stats = self.pg_conn.cursor.fetchone()
                    
                    if poi_stats:
                        # Основні POI метрики
                        unit['poi_total_count'] = poi_stats['poi_total'] or 0
                        unit['competitors_total'] = poi_stats['competitors_total'] or 0
                        unit['competitors_supermarket'] = poi_stats['comp_supermarket'] or 0
                        unit['competitors_convenience'] = poi_stats['comp_convenience'] or 0
                        unit['competitors_minimarket'] = poi_stats['comp_minimarket'] or 0
                        
                        # Категорії POI
                        unit['retail_count'] = poi_stats['retail_count'] or 0
                        unit['food_count'] = poi_stats['food_count'] or 0
                        unit['services_count'] = poi_stats['services_count'] or 0
                        unit['transport_count'] = poi_stats['transport_count'] or 0
                        unit['poi_retail_count'] = poi_stats['retail_count'] or 0
                        unit['poi_service_count'] = poi_stats['services_count'] or 0
                        unit['poi_social_count'] = poi_stats['social_count'] or 0
                        unit['poi_food_count'] = poi_stats['food_count'] or 0
                        unit['unique_brands_count'] = poi_stats['unique_brands'] or 0
                        
                        # Розраховуємо щільності та індекси
                        if unit['area_km2'] and unit['area_km2'] > 0:
                            unit['poi_density'] = unit['poi_total_count'] / unit['area_km2']
                            unit['competitor_density'] = unit['competitors_total'] / unit['area_km2']
                            unit['competitor_coverage'] = min(1.0, unit['competitor_density'] / 5)
                            
                            # Індекс різноманітності POI
                            categories = [unit['retail_count'], unit['food_count'], 
                                        unit['services_count'], unit['transport_count']]
                            total = sum(categories)
                            if total > 0:
                                diversity = 0
                                for cat in categories:
                                    if cat > 0:
                                        p = cat / total
                                        diversity -= p * (p if p > 0 else 0)
                                unit['poi_diversity_index'] = diversity
                            else:
                                unit['poi_diversity_index'] = 0
                            
                            unit['poi_retail_ratio'] = unit['retail_count'] / unit['poi_total_count'] if unit['poi_total_count'] > 0 else 0
                        else:
                            unit['poi_density'] = 0
                            unit['competitor_density'] = 0
                            unit['competitor_coverage'] = 0
                            unit['poi_diversity_index'] = 0
                            unit['poi_retail_ratio'] = 0
                    else:
                        self._set_default_poi_metrics(unit)
                        
                except Exception as e:
                    logger.warning(f"⚠️ Помилка POI для {unit.get('admin_name', 'unknown')}: {str(e)[:100]}")
                    self._set_default_poi_metrics(unit)
                
                progress.update(1)
        
        return admin_units
    
    def _set_default_poi_metrics(self, unit: Dict):
        """Встановлює значення за замовчуванням для POI метрик"""
        poi_fields = [
            'poi_total_count', 'competitors_total', 'competitors_supermarket',
            'competitors_convenience', 'competitors_minimarket', 'retail_count',
            'food_count', 'services_count', 'transport_count', 'poi_retail_count',
            'poi_service_count', 'poi_social_count', 'poi_food_count',
            'unique_brands_count'
        ]
        for field in poi_fields:
            unit[field] = 0
        
        density_fields = [
            'poi_density', 'competitor_density', 'competitor_coverage',
            'poi_diversity_index', 'poi_retail_ratio'
        ]
        for field in density_fields:
            unit[field] = 0.0
    
    def enrich_with_economic_metrics(self, admin_units: List[Dict]) -> List[Dict]:
        """
        Додає економічні метрики з таблиці rbc_h3_data
        ВИПРАВЛЕНО: використовує правильні поля avg_check_per_client замість avg_check_p50
        """
        logger.info("💰 Розрахунок економічних метрик...")
        
        progress = ETLProgress(len(admin_units), "Економічні метрики")
        success_count = 0
        error_count = 0
        
        for unit in admin_units:
            try:
                # Створюємо окреме підключення для кожного запиту
                conn = psycopg2.connect(
                    host=PG_CONFIG['host'],
                    port=PG_CONFIG['port'],
                    database=PG_CONFIG['database'],
                    user=PG_CONFIG['user'],
                    password=PG_CONFIG['password']
                )
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                cursor = conn.cursor(cursor_factory=RealDictCursor)
                
                # ВИПРАВЛЕНО: використовуємо avg_check_per_client або середнє p25 та p75
                econ_query = """
                SELECT 
                    COUNT(DISTINCT rbc.h3_index) as h3_with_transactions,
                    SUM(rbc.cnt_transaction) as total_transactions,
                    AVG(rbc.avg_check_per_client) as avg_check,
                    AVG((rbc.avg_check_p25 + rbc.avg_check_p75) / 2) as avg_check_median,
                    SUM(rbc.total_sum) as total_sum
                FROM osm_ukraine.rbc_h3_data rbc
                JOIN osm_ukraine.h3_admin_mapping ham ON ham.h3_index = rbc.h3_index
                WHERE ham.h3_resolution = 8
                    AND CASE %s
                        WHEN 4 THEN ham.oblast_id = %s
                        WHEN 5 THEN ham.raion_id = %s
                        WHEN 6 THEN ham.gromada_id = %s
                        WHEN 7 THEN ham.settlement_id = %s
                        WHEN 8 THEN ham.settlement_id = %s
                        WHEN 9 THEN ham.settlement_id = %s
                    END
                """
                
                cursor.execute(
                    econ_query,
                    (unit['admin_level'], unit['admin_id'], unit['admin_id'],
                     unit['admin_id'], unit['admin_id'], unit['admin_id'], unit['admin_id'])
                )
                
                result = cursor.fetchone()
                
                if result and result['total_transactions']:
                    # Транзакційна щільність
                    if unit['area_km2'] and unit['area_km2'] > 0:
                        unit['mcc_transaction_density'] = float(result['total_transactions']) / unit['area_km2']
                    else:
                        unit['mcc_transaction_density'] = None
                    
                    # Використовуємо avg_check_per_client як основний показник
                    unit['mcc_avg_transaction_value'] = float(result['avg_check']) if result['avg_check'] else None
                    
                    # Економічний індекс
                    if result['total_sum'] and unit.get('population_estimated') and unit['population_estimated'] > 0:
                        income_per_capita = float(result['total_sum']) / unit['population_estimated']
                        unit['economic_activity_index'] = min(1.0, income_per_capita / 100000)
                    else:
                        unit['economic_activity_index'] = None
                    
                    success_count += 1
                else:
                    unit['mcc_transaction_density'] = None
                    unit['mcc_avg_transaction_value'] = None
                    unit['economic_activity_index'] = None
                
                # Закриваємо підключення
                cursor.close()
                conn.close()
                
            except Exception as e:
                error_count += 1
                if error_count <= 10:  # Логуємо тільки перші 10 помилок
                    logger.warning(f"⚠️ Помилка економіки для {unit.get('admin_name', 'unknown')}: {str(e)[:100]}")
                
                unit['mcc_transaction_density'] = None
                unit['mcc_avg_transaction_value'] = None
                unit['economic_activity_index'] = None
                
                # Спробуємо закрити підключення якщо воно відкрите
                try:
                    if 'cursor' in locals():
                        cursor.close()
                    if 'conn' in locals():
                        conn.close()
                except:
                    pass
            
            progress.update(1)
        
        if error_count > 0:
            logger.warning(f"⚠️ Всього {error_count} помилок при розрахунку економічних метрик")
            logger.info(f"✅ Успішно розраховано економічні метрики для {success_count} адмінодиниць")
        else:
            logger.info(f"✅ Успішно розраховано економічні метрики для всіх {success_count} адмінодиниць")
        
        return admin_units
    
    def transform_for_clickhouse(self, admin_units: List[Dict]) -> List[Dict]:
        """
        Трансформує дані у формат для ClickHouse
        Додає всі необхідні поля відповідно до структури таблиці (87 полів)
        ВИПРАВЛЕНО: коректна обробка None значень при порівняннях
        """
        logger.info("🔄 Трансформація даних для ClickHouse...")
        
        transformed = []
        current_date = datetime.now()
        
        for unit in admin_units:
            # Копіюємо та очищаємо
            unit_copy = unit.copy()
            
            # Видаляємо поле geometry
            if 'geometry' in unit_copy:
                del unit_copy['geometry']
            
            # === Додаємо відсутні поля з дефолтними значеннями ===
            
            # Населення та будівлі
            unit_copy.setdefault('residential_coverage', 0.0)
            unit_copy.setdefault('residential_buildings_count', 0)
            unit_copy.setdefault('avg_building_floors', None)
            
            # Банківські дані
            unit_copy.setdefault('bank_terminals_count', None)
            unit_copy.setdefault('bank_branches_count', None)
            unit_copy.setdefault('mcc_top_category', None)
            unit_copy.setdefault('last_mcc_update', None)
            
            # Дорожня інфраструктура
            unit_copy.setdefault('road_density_km_per_km2', 0.0)
            unit_copy.setdefault('road_primary_km', 0.0)
            unit_copy.setdefault('road_secondary_km', 0.0)
            unit_copy.setdefault('public_transport_stops', 0)
            unit_copy.setdefault('railway_stations', 0)
            
            # Транспортні індекси (ВИПРАВЛЕНО: безпечне отримання значень)
            poi_density = unit_copy.get('poi_density')
            if poi_density is not None and poi_density > 0:
                connectivity = min(1.0, poi_density / 10)
            else:
                connectivity = 0.0
            unit_copy['connectivity_index'] = connectivity
            
            transport_count = unit_copy.get('transport_count')
            area_km2 = unit_copy.get('area_km2')
            transport_score = 0.0
            if transport_count is not None and transport_count > 0 and area_km2 is not None and area_km2 > 0:
                transport_density = transport_count / area_km2
                transport_score = min(1.0, transport_density / 5)
            unit_copy['transport_accessibility_score'] = transport_score
            
            # Комерційна активність (ВИПРАВЛЕНО: безпечне отримання значень)
            retail_count = unit_copy.get('retail_count')
            commercial_score = 0.0
            if retail_count is not None and retail_count > 0 and area_km2 is not None and area_km2 > 0:
                retail_density = retail_count / area_km2
                commercial_score = min(1.0, retail_density / 10)
            unit_copy['commercial_activity_score'] = commercial_score
            
            # Потенціал для ритейлу (ВИПРАВЛЕНО: безпечна обробка)
            factors = []
            pop_density = unit_copy.get('population_density')
            if pop_density is not None:
                factors.append(min(1.0, pop_density / 1000))
            
            economic_index = unit_copy.get('economic_activity_index')
            if economic_index is not None:
                factors.append(economic_index)
            
            if transport_score > 0:
                factors.append(transport_score)
            
            unit_copy['retail_potential_score'] = sum(factors) / len(factors) if factors else 0.0
            
            # H3 агреговані метрики
            unit_copy.setdefault('h3_competitor_avg', None)
            unit_copy.setdefault('h3_poi_avg', None)
            unit_copy.setdefault('h3_income_avg', None)
            unit_copy.setdefault('h3_income_median', None)
            
            # Туристичні індекси
            unit_copy.setdefault('tourist_index', None)
            unit_copy.setdefault('agricultural_index', None)
            unit_copy.setdefault('tourist_zone_type', None)
            unit_copy.setdefault('tourist_attractions_count', None)
            unit_copy.setdefault('hotels_count', None)
            unit_copy.setdefault('seasonal_factor', None)
            
            # Ринкова концентрація
            unit_copy.setdefault('market_concentration_hhi', None)
            unit_copy.setdefault('dominant_chain', None)
            
            # Географічні характеристики
            unit_copy.setdefault('distance_to_regional_center', 0.0)
            unit_copy.setdefault('border_distance_km', None)
            unit_copy.setdefault('industrial_specialization', None)
            unit_copy.setdefault('regional_center_type', None)
            
            # Урбанізація (ВИПРАВЛЕНО: безпечне порівняння)
            pop_density = unit_copy.get('population_density')
            if pop_density is not None and pop_density > 100:
                urbanization = 'urban'
            else:
                urbanization = 'rural'
            unit_copy['urbanization_level'] = urbanization
            
            # Bins (заглушки - будуть розраховані в наступному скрипті)
            unit_copy.setdefault('population_bin', 0)
            unit_copy.setdefault('population_quintile', 0)
            unit_copy.setdefault('economic_bin', 0)
            unit_copy.setdefault('economic_quintile', 0)
            unit_copy.setdefault('competitor_bin', 0)
            unit_copy.setdefault('infrastructure_bin', 0)
            unit_copy.setdefault('accessibility_bin', 0)
            
            # Bivariate комбінації (заглушки)
            unit_copy.setdefault('bivar_pop_economic', '')
            unit_copy.setdefault('bivar_access_economic', '')
            unit_copy.setdefault('bivar_urban_competitor', '')
            unit_copy.setdefault('bivar_comp_infrastructure', '')
            
            # Метадані
            unit_copy['data_completeness'] = self._calculate_completeness(unit_copy)
            unit_copy['last_osm_update'] = current_date.date()
            unit_copy['calculated_at'] = current_date
            unit_copy['updated_at'] = current_date
            
            transformed.append(unit_copy)
        
        return transformed
    
    def _calculate_completeness(self, unit: Dict) -> float:
        """
        Розраховує коефіцієнт повноти даних
        ВИПРАВЛЕНО: безпечна перевірка на None і 0
        """
        important_fields = [
            'population_estimated', 'poi_total_count', 'competitors_total',
            'economic_activity_index', 'transport_count'
        ]
        
        filled = 0
        for field in important_fields:
            value = unit.get(field)
            if value is not None and value != 0:
                filled += 1
        
        return filled / len(important_fields)
    
    def load_to_clickhouse(self, data: List[Dict]) -> int:
        """
        Завантажує дані в ClickHouse
        Автоматично фільтрує поля, які існують в таблиці
        """
        logger.info("📤 Завантаження даних в ClickHouse...")
        
        if not data:
            logger.warning("Немає даних для завантаження")
            return 0
        
        with self.ch_conn.connect():
            # Отримуємо список колонок з таблиці
            existing_columns = self.ch_conn.get_table_columns('geo_analytics.admin_analytics')
            
            if not existing_columns:
                logger.error("❌ Не вдалося отримати структуру таблиці")
                return 0
            
            logger.info(f"📋 Знайдено {len(existing_columns)} колонок в таблиці")
            
            # Очищаємо таблицю перед завантаженням
            self.ch_conn.client.execute("TRUNCATE TABLE geo_analytics.admin_analytics")
            logger.info("🗑️ Таблиця admin_analytics очищена")
            
            # Завантажуємо батчами
            batch_size = BATCH_SIZE
            total_loaded = 0
            progress = ETLProgress(len(data), "Завантаження в ClickHouse")
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                # insert_data автоматично фільтрує колонки
                loaded = self.ch_conn.insert_data(
                    'geo_analytics.admin_analytics',
                    batch,
                    existing_columns
                )
                total_loaded += loaded
                progress.update(len(batch))
            
            logger.info(f"✅ Завантажено {total_loaded} записів в ClickHouse")
            return total_loaded
    
    def verify_migration(self) -> bool:
        """
        Перевіряє успішність міграції
        """
        logger.info("🔍 Перевірка результатів міграції...")
        
        with self.ch_conn.connect():
            # Перевірка кількості записів
            result = self.ch_conn.client.execute(
                "SELECT COUNT(*) as cnt FROM geo_analytics.admin_analytics"
            )
            ch_count = result[0][0]
            
            # Перевірка по рівнях
            level_stats = self.ch_conn.client.execute("""
                SELECT 
                    admin_level,
                    COUNT(*) as cnt,
                    AVG(population_density) as avg_pop_density,
                    AVG(poi_density) as avg_poi_density,
                    AVG(economic_activity_index) as avg_economic,
                    AVG(competitor_density) as avg_competitor,
                    SUM(CASE WHEN economic_activity_index IS NOT NULL THEN 1 ELSE 0 END) as with_economics
                FROM geo_analytics.admin_analytics
                GROUP BY admin_level
                ORDER BY admin_level
            """)
            
            logger.info(f"\n📊 Статистика міграції:")
            logger.info(f"Всього записів: {ch_count:,}")
            logger.info(f"\nПо рівнях:")
            
            level_names = {
                4: "Області",
                5: "Райони", 
                6: "Громади",
                7: "Міста обласного значення",
                8: "Міста районного значення",
                9: "Села та селища"
            }
            
            total_with_economics = 0
            for row in level_stats:
                level, count, avg_pop, avg_poi, avg_econ, avg_comp, with_econ = row
                name = level_names.get(level, f"Рівень {level}")
                total_with_economics += with_econ
                
                logger.info(f"\n  {name}: {count:,} од.")
                
                if avg_pop:
                    logger.info(f"    - Щільність населення: {avg_pop:.1f} чол/км²")
                if avg_poi:
                    logger.info(f"    - Щільність POI: {avg_poi:.1f}/км²")
                if avg_comp:
                    logger.info(f"    - Щільність конкурентів: {avg_comp:.2f}/км²")
                if avg_econ:
                    logger.info(f"    - Економічний індекс: {avg_econ:.3f}")
                logger.info(f"    - З економічними даними: {with_econ}/{count} ({with_econ*100/count:.1f}%)")
            
            # Загальна статистика економічних даних
            logger.info(f"\n📈 Економічні метрики розраховані для {total_with_economics}/{ch_count} "
                       f"({total_with_economics*100/ch_count:.1f}%) адмінодиниць")
            
            if total_with_economics < ch_count:
                missing = ch_count - total_with_economics
                logger.warning(f"⚠️ {missing} адмінодиниць без економічних даних (це нормально)")
            
            # Топ конкурентів
            top_competitors = self.ch_conn.client.execute("""
                SELECT 
                    admin_name,
                    admin_level,
                    competitors_total,
                    competitors_supermarket,
                    competitors_convenience
                FROM geo_analytics.admin_analytics
                WHERE competitors_total > 0
                ORDER BY competitors_total DESC
                LIMIT 5
            """)
            
            logger.info(f"\n🏪 ТОП-5 за кількістю конкурентів:")
            for row in top_competitors:
                name, level, total, supermarket, convenience = row
                logger.info(f"  - {name}: {total} (супермаркети: {supermarket}, convenience: {convenience})")
            
            return ch_count > 0
    
    def run(self) -> bool:
        """
        Запускає повний ETL процес
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ПОЧАТОК ETL: Адміністративні одиниці → ClickHouse")
            logger.info("=" * 60)
            
            # 1. Витягування
            admin_units = self.extract_admin_units()
            
            if not admin_units:
                logger.error("❌ Не знайдено адмінодиниць для обробки")
                return False
            
            # 2. Збагачення даними
            admin_units = self.enrich_with_h3_counts(admin_units)
            admin_units = self.enrich_with_population(admin_units)
            admin_units = self.enrich_with_poi_metrics(admin_units)
            admin_units = self.enrich_with_economic_metrics(admin_units)
            
            # 3. Трансформація
            transformed = self.transform_for_clickhouse(admin_units)
            
            # 4. Завантаження
            loaded = self.load_to_clickhouse(transformed)
            
            # 5. Верифікація
            success = self.verify_migration()
            
            # Підсумок
            elapsed = datetime.now() - self.start_time
            logger.info("\n" + "=" * 60)
            if success:
                logger.info(f"✅ ETL ЗАВЕРШЕНО УСПІШНО")
                logger.info(f"⏱️ Час виконання: {elapsed}")
                logger.info(f"📊 Оброблено записів: {loaded:,}")
                logger.info(f"\n🎯 Наступний крок: розробка та запуск 02_admin_bins.py для розрахунку bins")
            else:
                logger.error(f"❌ ETL ЗАВЕРШЕНО З ПОМИЛКАМИ")
            logger.info("=" * 60)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Критична помилка ETL: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Головна функція"""
    etl = AdminAnalyticsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()