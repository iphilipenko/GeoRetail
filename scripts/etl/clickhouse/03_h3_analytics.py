"""
scripts/etl/clickhouse/03_h3_analytics.py
ETL для міграції H3 гексагонів з PostGIS в ClickHouse
Обробляє всі резолюції (7, 8, 9, 10) з батчуванням по областях
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import h3
from config import PG_CONFIG, CH_CONFIG, BATCH_SIZE, LARGE_TABLE_BATCH_SIZE
from utils import PostgresConnector, ClickHouseConnector, ETLProgress

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class H3AnalyticsETL:
    """
    ETL клас для міграції H3 гексагонів всіх резолюцій
    Використовує h3_admin_mapping як основне джерело
    """
    
    # Конфігурація резолюцій та очікувані обсяги
    RESOLUTIONS = {
        7: {'expected_count': 136000, 'batch_size': 50000},
        8: {'expected_count': 760000, 'batch_size': 30000},
        9: {'expected_count': 2500000, 'batch_size': 20000},
        10: {'expected_count': 5300000, 'batch_size': 10000}
    }
    
    def __init__(self):
        """Ініціалізація ETL процесу"""
        self.pg_conn = PostgresConnector(PG_CONFIG)
        self.ch_conn = ClickHouseConnector(CH_CONFIG)
        self.start_time = datetime.now()
        self.total_processed = 0
        
    def get_oblasts(self) -> List[Dict]:
        """
        Отримує список областей для батчування
        
        Returns:
            Список областей з id та назвами
        """
        logger.info("📍 Отримання списку областей...")
        
        query = """
        SELECT 
            id as oblast_id,
            name_uk as oblast_name,
            ST_Area(geometry::geography) / 1000000 as area_km2
        FROM osm_ukraine.admin_boundaries
        WHERE admin_level = 4
        ORDER BY id
        """
        
        with self.pg_conn.connect():
            oblasts = self.pg_conn.execute_query(query)
            logger.info(f"✅ Знайдено {len(oblasts)} областей")
            return oblasts
    
    def extract_h3_batch(self, resolution: int, oblast_id: int) -> List[Dict]:
        """
        Витягує батч H3 гексагонів для конкретної області та резолюції
        
        Args:
            resolution: H3 резолюція (7-10)
            oblast_id: ID області
            
        Returns:
            Список гексагонів з базовою інформацією
        """
        query = """
        SELECT DISTINCT
            ham.h3_index,
            ham.h3_resolution as resolution,
            ham.oblast_id,
            ham.raion_id,
            ham.gromada_id,
            -- Отримуємо центроїд гексагону для подальших розрахунків
            h3_cell_to_lat(ham.h3_index::h3index) as lat,
            h3_cell_to_lng(ham.h3_index::h3index) as lng
        FROM osm_ukraine.h3_admin_mapping ham
        WHERE ham.h3_resolution = %s
            AND ham.oblast_id = %s
        """
        
        with self.pg_conn.connect():
            self.pg_conn.cursor.execute(query, (resolution, oblast_id))
            results = self.pg_conn.cursor.fetchall()
            return results if results else []
    
    def enrich_with_population(self, hexagons: List[Dict]) -> List[Dict]:
        """
        Додає дані про населення з demographics.h3_population
        
        Args:
            hexagons: Список гексагонів
            
        Returns:
            Збагачені гексагони
        """
        if not hexagons:
            return hexagons
        
        # Створюємо мапу для швидкого доступу
        hex_map = {h['h3_index']: h for h in hexagons}
        h3_indices = list(hex_map.keys())
        
        # Батчами запитуємо дані про населення
        batch_size = 5000
        
        with self.pg_conn.connect():
            for i in range(0, len(h3_indices), batch_size):
                batch_indices = h3_indices[i:i+batch_size]
                
                query = """
                SELECT 
                    hex_id as h3_index,
                    population,
                    population_density
                FROM demographics.h3_population
                WHERE hex_id = ANY(%s)
                """
                
                self.pg_conn.cursor.execute(query, (batch_indices,))
                results = self.pg_conn.cursor.fetchall()
                
                # Оновлюємо дані
                for row in results:
                    if row['h3_index'] in hex_map:
                        hex_map[row['h3_index']]['population_count'] = row['population']
                        hex_map[row['h3_index']]['population_density'] = row['population_density']
        
        # Заповнюємо нулями відсутні значення
        for h in hexagons:
            h.setdefault('population_count', 0)
            h.setdefault('population_density', 0)
        
        return hexagons
    
    def enrich_with_economic_data(self, hexagons: List[Dict]) -> List[Dict]:
        """
        Додає економічні дані з rbc_h3_data
        
        Args:
            hexagons: Список гексагонів
            
        Returns:
            Збагачені гексагони
        """
        if not hexagons:
            return hexagons
        
        hex_map = {h['h3_index']: h for h in hexagons}
        h3_indices = list(hex_map.keys())
        
        batch_size = 5000
        
        with self.pg_conn.connect():
            for i in range(0, len(h3_indices), batch_size):
                batch_indices = h3_indices[i:i+batch_size]
                
                query = """
                SELECT 
                    h3_index,
                    cnt_transaction,
                    total_sum,
                    avg_check_per_client,
                    (avg_check_p25 + avg_check_p75) / 2 as avg_check_median
                FROM osm_ukraine.rbc_h3_data
                WHERE h3_index = ANY(%s)
                """
                
                self.pg_conn.cursor.execute(query, (batch_indices,))
                results = self.pg_conn.cursor.fetchall()
                
                for row in results:
                    if row['h3_index'] in hex_map:
                        h = hex_map[row['h3_index']]
                        h['transaction_count'] = row['cnt_transaction']
                        h['transaction_sum'] = float(row['total_sum']) if row['total_sum'] else 0
                        h['avg_check'] = float(row['avg_check_per_client']) if row['avg_check_per_client'] else 0
        
        # Заповнюємо нулями відсутні значення
        for h in hexagons:
            h.setdefault('transaction_count', 0)
            h.setdefault('transaction_sum', 0)
            h.setdefault('avg_check', 0)
        
        return hexagons
    
    def enrich_with_poi_metrics(self, hexagons: List[Dict], resolution: int) -> List[Dict]:
        """
        Додає POI метрики з poi_processed
        Використовує відповідне поле h3_res_X залежно від резолюції
        
        Args:
            hexagons: Список гексагонів
            resolution: H3 резолюція
            
        Returns:
            Збагачені гексагони
        """
        if not hexagons:
            return hexagons
        
        hex_map = {h['h3_index']: h for h in hexagons}
        h3_indices = list(hex_map.keys())
        
        # Визначаємо поле для JOIN залежно від резолюції
        h3_field = f'h3_res_{resolution}'
        
        batch_size = 5000
        
        with self.pg_conn.connect():
            for i in range(0, len(h3_indices), batch_size):
                batch_indices = h3_indices[i:i+batch_size]
                
                # Агрегуємо POI по гексагонах
                query = f"""
                SELECT 
                    {h3_field} as h3_index,
                    COUNT(*) as poi_total,
                    COUNT(*) FILTER (WHERE functional_group = 'competitor') as competitors,
                    COUNT(*) FILTER (WHERE primary_category = 'retail') as retail_count,
                    COUNT(*) FILTER (WHERE primary_category = 'food_service') as food_count,
                    COUNT(*) FILTER (WHERE primary_category = 'transport') as transport_count,
                    COUNT(*) FILTER (WHERE primary_category IN ('amenity', 'healthcare')) as services_count,
                    COUNT(*) FILTER (WHERE entity_type = 'transport_node') as transport_nodes,
                    COUNT(*) FILTER (WHERE entity_type = 'road_segment') as road_segments,
                    COUNT(DISTINCT brand_normalized) FILTER (WHERE brand_normalized IS NOT NULL) as unique_brands
                FROM osm_ukraine.poi_processed
                WHERE {h3_field} = ANY(%s)
                GROUP BY {h3_field}
                """
                
                self.pg_conn.cursor.execute(query, (batch_indices,))
                results = self.pg_conn.cursor.fetchall()
                
                for row in results:
                    if row['h3_index'] in hex_map:
                        h = hex_map[row['h3_index']]
                        h['poi_total'] = row['poi_total']
                        h['competitors'] = row['competitors']
                        h['retail_count'] = row['retail_count']
                        h['food_count'] = row['food_count']
                        h['transport_count'] = row['transport_count']
                        h['services_count'] = row['services_count']
                        h['transport_nodes'] = row['transport_nodes']
                        h['road_segments'] = row['road_segments']
                        h['unique_brands'] = row['unique_brands']
        
        # Заповнюємо нулями відсутні значення
        for h in hexagons:
            h.setdefault('poi_total', 0)
            h.setdefault('competitors', 0)
            h.setdefault('retail_count', 0)
            h.setdefault('food_count', 0)
            h.setdefault('transport_count', 0)
            h.setdefault('services_count', 0)
            h.setdefault('transport_nodes', 0)
            h.setdefault('road_segments', 0)
            h.setdefault('unique_brands', 0)
        
        return hexagons
    
    def calculate_metrics(self, hexagons: List[Dict], resolution: int) -> List[Dict]:
        """
        Розраховує всі необхідні метрики та індекси
        
        Args:
            hexagons: Список гексагонів з базовими даними
            resolution: H3 резолюція
            
        Returns:
            Гексагони з розрахованими метриками
        """
        # Отримуємо площу гексагону для даної резолюції (км²)
        hex_areas = {
            7: 5.161,    # ~5.16 км²
            8: 0.737,    # ~0.74 км²
            9: 0.105,    # ~0.11 км²
            10: 0.015    # ~0.015 км²
        }
        area_km2 = hex_areas.get(resolution, 1.0)
        
        for h in hexagons:
            # 1. Population density (вже є з demographics)
            # Якщо немає, розраховуємо
            if h.get('population_density') is None or h['population_density'] == 0:
                if h.get('population_count', 0) > 0:
                    h['population_density'] = h['population_count'] / area_km2
                else:
                    h['population_density'] = 0
            
            # 2. Income index (0-1) - на основі економічних даних
            if h.get('transaction_sum', 0) > 0 and h.get('population_count', 0) > 0:
                income_per_capita = h['transaction_sum'] / h['population_count']
                h['income_index'] = min(1.0, income_per_capita / 50000)  # Нормалізація
            else:
                h['income_index'] = 0
            
            # 3. Competitor intensity (0-1)
            competitors = h.get('competitors', 0)
            h['competitor_intensity'] = min(1.0, competitors / 10)  # 10+ конкурентів = максимум
            
            # 4. POI density (на км²)
            h['poi_density'] = h.get('poi_total', 0) / area_km2
            
            # 5. Accessibility score (0-1) - транспортна доступність
            transport_score = 0
            if h.get('transport_nodes', 0) > 0:
                transport_score += min(0.5, h['transport_nodes'] / 5)
            if h.get('road_segments', 0) > 0:
                transport_score += min(0.5, h['road_segments'] / 10)
            h['accessibility_score'] = transport_score
            
            # 6. Traffic index (0-1) - на основі POI що генерують трафік
            traffic_generators = (h.get('retail_count', 0) + 
                                h.get('food_count', 0) + 
                                h.get('services_count', 0))
            h['traffic_index'] = min(1.0, traffic_generators / 20)
            
            # 7. Retail potential (0-1) - композитний індекс
            factors = []
            
            # Фактор населення
            if h['population_density'] > 0:
                factors.append(min(1.0, h['population_density'] / 1000))
            
            # Фактор доходів
            if h['income_index'] > 0:
                factors.append(h['income_index'])
            
            # Фактор доступності
            if h['accessibility_score'] > 0:
                factors.append(h['accessibility_score'])
            
            # Фактор трафіку
            if h['traffic_index'] > 0:
                factors.append(h['traffic_index'])
            
            # Негативний фактор конкуренції
            competition_factor = 1.0 - (h['competitor_intensity'] * 0.5)
            
            if factors:
                h['retail_potential'] = (sum(factors) / len(factors)) * competition_factor
            else:
                h['retail_potential'] = 0
            
            # 8. Risk score (0-1) - ризики
            risk_factors = []
            
            # Ризик високої конкуренції
            if h['competitor_intensity'] > 0.5:
                risk_factors.append(h['competitor_intensity'])
            
            # Ризик низької щільності населення
            if h['population_density'] < 100:
                risk_factors.append(0.5)
            
            # Ризик низьких доходів
            if h['income_index'] < 0.3:
                risk_factors.append(0.6)
            
            # Ризик поганої доступності
            if h['accessibility_score'] < 0.3:
                risk_factors.append(0.4)
            
            if risk_factors:
                h['risk_score'] = sum(risk_factors) / len(risk_factors)
            else:
                h['risk_score'] = 0.2  # Базовий ризик
            
            # Pre-calculated bins (спрощена версія - детальний розрахунок в окремому скрипті)
            # Population bin (1-3)
            if h['population_density'] < 100:
                h['population_bin'] = 1
            elif h['population_density'] < 500:
                h['population_bin'] = 2
            else:
                h['population_bin'] = 3
            
            # Income bin (1-3)
            if h['income_index'] < 0.33:
                h['income_bin'] = 1
            elif h['income_index'] < 0.66:
                h['income_bin'] = 2
            else:
                h['income_bin'] = 3
            
            # Competitor bin (1-3)
            if h['competitor_intensity'] < 0.33:
                h['competitor_bin'] = 1
            elif h['competitor_intensity'] < 0.66:
                h['competitor_bin'] = 2
            else:
                h['competitor_bin'] = 3
        
        return hexagons
    
    def transform_for_clickhouse(self, hexagons: List[Dict]) -> List[Dict]:
        """
        Трансформує дані для завантаження в ClickHouse
        
        Args:
            hexagons: Список гексагонів з метриками
            
        Returns:
            Трансформовані дані
        """
        transformed = []
        current_time = datetime.now()
        
        for h in hexagons:
            record = {
                # Ідентифікація
                'h3_index': h['h3_index'],
                'resolution': h['resolution'],
                
                # Core метрики (безпечне приведення типів)
                'population_density': float(h.get('population_density', 0)),
                'income_index': float(h.get('income_index', 0)),
                'competitor_intensity': float(h.get('competitor_intensity', 0)),
                'poi_density': float(h.get('poi_density', 0)),
                'accessibility_score': float(h.get('accessibility_score', 0)),
                'traffic_index': float(h.get('traffic_index', 0)),
                'retail_potential': float(h.get('retail_potential', 0)),
                'risk_score': float(h.get('risk_score', 0)),
                
                # Pre-calculated bins
                'population_bin': int(h.get('population_bin', 1)),
                'income_bin': int(h.get('income_bin', 1)),
                'competitor_bin': int(h.get('competitor_bin', 1)),
                
                # Географічна прив'язка
                'oblast_id': int(h.get('oblast_id', 0)),
                'raion_id': int(h.get('raion_id', 0)) if h.get('raion_id') else 0,
                'gromada_id': int(h.get('gromada_id', 0)) if h.get('gromada_id') else 0,
                
                # Metadata
                'updated_at': current_time
            }
            
            transformed.append(record)
        
        return transformed
    
    def load_to_clickhouse(self, data: List[Dict], resolution: int) -> int:
        """
        Завантажує дані в ClickHouse
        
        Args:
            data: Дані для завантаження
            resolution: H3 резолюція (для логування)
            
        Returns:
            Кількість завантажених записів
        """
        if not data:
            return 0
        
        with self.ch_conn.connect():
            # Отримуємо список колонок
            existing_columns = self.ch_conn.get_table_columns('geo_analytics.h3_analytics')
            
            if not existing_columns:
                logger.error("❌ Не вдалося отримати структуру таблиці h3_analytics")
                return 0
            
            # Завантажуємо батчами
            batch_size = 10000
            total_loaded = 0
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                loaded = self.ch_conn.insert_data(
                    'geo_analytics.h3_analytics',
                    batch,
                    existing_columns
                )
                total_loaded += loaded
            
            return total_loaded
    
    def process_resolution(self, resolution: int) -> Tuple[int, int]:
        """
        Обробляє всі гексагони для конкретної резолюції
        
        Args:
            resolution: H3 резолюція (7-10)
            
        Returns:
            Кортеж (оброблено, завантажено)
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"🔷 ОБРОБКА РЕЗОЛЮЦІЇ H3-{resolution}")
        logger.info(f"{'='*60}")
        
        res_config = self.RESOLUTIONS[resolution]
        oblasts = self.get_oblasts()
        
        total_extracted = 0
        total_loaded = 0
        
        # Прогрес по областях
        oblast_progress = ETLProgress(len(oblasts), f"Області для H3-{resolution}")
        
        for oblast in oblasts:
            oblast_id = oblast['oblast_id']
            oblast_name = oblast['oblast_name']
            
            try:
                # 1. Витягування гексагонів
                hexagons = self.extract_h3_batch(resolution, oblast_id)
                
                if not hexagons:
                    oblast_progress.update(1)
                    continue
                
                total_extracted += len(hexagons)
                
                # 2. Збагачення даними
                hexagons = self.enrich_with_population(hexagons)
                hexagons = self.enrich_with_economic_data(hexagons)
                hexagons = self.enrich_with_poi_metrics(hexagons, resolution)
                
                # 3. Розрахунок метрик
                hexagons = self.calculate_metrics(hexagons, resolution)
                
                # 4. Трансформація
                transformed = self.transform_for_clickhouse(hexagons)
                
                # 5. Завантаження
                loaded = self.load_to_clickhouse(transformed, resolution)
                total_loaded += loaded
                
                # Оновлюємо прогрес
                oblast_progress.update(1)
                
                # Логування для області
                if loaded > 0:
                    logger.debug(f"  {oblast_name}: {loaded:,} гексагонів")
                
            except Exception as e:
                logger.error(f"❌ Помилка для {oblast_name}: {str(e)[:100]}")
                oblast_progress.update(1)
                continue
        
        logger.info(f"\n📊 Резолюція H3-{resolution}:")
        logger.info(f"  Витягнуто: {total_extracted:,}")
        logger.info(f"  Завантажено: {total_loaded:,}")
        
        if res_config['expected_count'] > 0:
            coverage = (total_loaded / res_config['expected_count']) * 100
            logger.info(f"  Покриття: {coverage:.1f}%")
        
        return total_extracted, total_loaded
    
    def verify_migration(self) -> bool:
        """
        Перевіряє результати міграції
        
        Returns:
            True якщо успішно
        """
        logger.info("\n🔍 Перевірка результатів міграції...")
        
        with self.ch_conn.connect():
            # Загальна статистика
            result = self.ch_conn.client.execute("""
                SELECT 
                    resolution,
                    COUNT(*) as count,
                    AVG(population_density) as avg_pop,
                    AVG(retail_potential) as avg_potential,
                    AVG(risk_score) as avg_risk,
                    COUNT(DISTINCT oblast_id) as oblasts,
                    MIN(income_index) as min_income,
                    MAX(income_index) as max_income
                FROM geo_analytics.h3_analytics
                GROUP BY resolution
                ORDER BY resolution
            """)
            
            logger.info("\n📊 Статистика по резолюціях:")
            total_count = 0
            
            for row in result:
                res, count, avg_pop, avg_pot, avg_risk, oblasts, min_inc, max_inc = row
                total_count += count
                
                logger.info(f"\n  H3-{res}: {count:,} гексагонів")
                logger.info(f"    Областей: {oblasts}")
                
                if avg_pop:
                    logger.info(f"    Середня щільність: {avg_pop:.1f} чол/км²")
                if avg_pot:
                    logger.info(f"    Середній потенціал: {avg_pot:.3f}")
                if avg_risk:
                    logger.info(f"    Середній ризик: {avg_risk:.3f}")
                if max_inc > 0:
                    logger.info(f"    Діапазон доходів: {min_inc:.3f} - {max_inc:.3f}")
            
            # Топ гексагони за потенціалом
            top_hexagons = self.ch_conn.client.execute("""
                SELECT 
                    h3_index,
                    resolution,
                    retail_potential,
                    population_density,
                    income_index,
                    competitor_intensity
                FROM geo_analytics.h3_analytics
                WHERE retail_potential > 0
                ORDER BY retail_potential DESC
                LIMIT 5
            """)
            
            if top_hexagons:
                logger.info("\n🏆 ТОП-5 гексагонів за потенціалом:")
                for h3_idx, res, potential, pop, income, comp in top_hexagons:
                    logger.info(f"  {h3_idx[:8]}... (H3-{res}): потенціал={potential:.3f}, "
                              f"населення={pop:.0f}/км², дохід={income:.3f}")
            
            logger.info(f"\n✅ Всього завантажено: {total_count:,} гексагонів")
            
            return total_count > 0
    
    def run(self) -> bool:
        """
        Запускає повний ETL процес для всіх резолюцій
        
        Returns:
            True якщо успішно
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ПОЧАТОК ETL: H3 гексагони → ClickHouse")
            logger.info("=" * 60)
            
            # Очищаємо таблицю перед завантаженням
            with self.ch_conn.connect():
                self.ch_conn.client.execute("TRUNCATE TABLE geo_analytics.h3_analytics")
                logger.info("🗑️ Таблиця h3_analytics очищена")
            
            # Обробляємо кожну резолюцію
            stats = {}
            for resolution in sorted(self.RESOLUTIONS.keys()):
                extracted, loaded = self.process_resolution(resolution)
                stats[resolution] = {'extracted': extracted, 'loaded': loaded}
                self.total_processed += loaded
            
            # Верифікація
            success = self.verify_migration()
            
            # Підсумок
            elapsed = datetime.now() - self.start_time
            logger.info("\n" + "=" * 60)
            
            if success:
                logger.info(f"✅ ETL ЗАВЕРШЕНО УСПІШНО")
                logger.info(f"⏱️ Час виконання: {elapsed}")
                logger.info(f"📊 Статистика по резолюціях:")
                
                for res, data in stats.items():
                    logger.info(f"  H3-{res}: {data['loaded']:,} / {data['extracted']:,}")
                
                logger.info(f"\n📊 Всього оброблено: {self.total_processed:,} гексагонів")
                logger.info(f"\n🎯 Наступні кроки:")
                logger.info(f"  1. Запустіть 02_admin_bins.py для розрахунку bins адмінодиниць")
                logger.info(f"  2. Запустіть 04_h3_bins.py для розрахунку bins гексагонів")
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
    etl = H3AnalyticsETL()
    success = etl.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()