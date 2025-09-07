"""
scripts/etl/clickhouse/03_h3_analytics.py
ETL для міграції H3 гексагонів з PostGIS в ClickHouse
МОДИФІКОВАНА ВЕРСІЯ - з правильним розрахунком населення для всіх резолюцій
ВИПРАВЛЕНО: використання правильних назв функцій h3 (cell_to_children, cell_to_parent)
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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
        # Кеш для H3-8 населення (використовується для розрахунку інших резолюцій)
        self.h8_population_cache = None
        
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
            ham.gromada_id
        FROM osm_ukraine.h3_admin_mapping ham
        WHERE ham.h3_resolution = %s
            AND ham.oblast_id = %s
        """
        
        with self.pg_conn.connect():
            self.pg_conn.cursor.execute(query, (resolution, oblast_id))
            results = self.pg_conn.cursor.fetchall()
            return results if results else []
    
    def load_h8_population_cache(self):
        """
        Завантажує всі H3-8 з населенням в кеш для подальшого використання
        """
        if self.h8_population_cache is not None:
            return  # Вже завантажено
        
        logger.info("📊 Завантаження H3-8 населення в кеш...")
        
        with self.pg_conn.connect():
            query = """
            SELECT 
                hex_id as h3_index,
                population
            FROM demographics.h3_population
            WHERE resolution = 8 AND population > 0
            """
            self.pg_conn.cursor.execute(query)
            h8_data = self.pg_conn.cursor.fetchall()
        
        self.h8_population_cache = {
            row['h3_index']: float(row['population']) 
            for row in h8_data
        }
        logger.info(f"✅ Завантажено {len(self.h8_population_cache):,} H3-8 гексагонів з населенням")
    
    def enrich_with_population(self, hexagons: List[Dict], resolution: int) -> List[Dict]:
        """
        Додає дані про населення залежно від резолюції
        
        Args:
            hexagons: Список гексагонів
            resolution: H3 резолюція
            
        Returns:
            Збагачені гексагони
        """
        if not hexagons:
            return hexagons
        
        if resolution == 8:
            # Для H3-8 беремо дані напряму з demographics.h3_population
            return self.enrich_h8_with_population(hexagons)
        elif resolution == 7:
            # Для H3-7 агрегуємо з H3-8
            return self.calculate_h7_population_from_h8(hexagons)
        elif resolution in [9, 10]:
            # Для H3-9 та H3-10 дезагрегуємо з H3-8
            return self.calculate_h9_h10_population_from_h8(hexagons, resolution)
        
        # Для інших резолюцій (якщо будуть) - нулі
        for h in hexagons:
            h['population'] = 0
            h['population_density'] = 0
        
        return hexagons
    
    def enrich_h8_with_population(self, hexagons: List[Dict]) -> List[Dict]:
        """
        Додає дані про населення для H3-8 з demographics.h3_population
        
        Args:
            hexagons: Список H3-8 гексагонів
            
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
                
                if batch_indices:
                    indices_str = ','.join([f"'{idx}'" for idx in batch_indices])
                    query = f"""
                    SELECT 
                        hex_id as h3_index,
                        population,
                        population_density
                    FROM demographics.h3_population
                    WHERE hex_id IN ({indices_str})
                    """
                    
                    self.pg_conn.cursor.execute(query)
                    results = self.pg_conn.cursor.fetchall()
                else:
                    results = []
                
                # Оновлюємо дані
                for row in results:
                    if row['h3_index'] in hex_map:
                        hex_map[row['h3_index']]['population'] = float(row['population']) if row['population'] else 0
                        hex_map[row['h3_index']]['population_density'] = float(row['population_density']) if row['population_density'] else 0
        
        # Заповнюємо нулями відсутні значення
        for h in hexagons:
            h.setdefault('population', 0)
            h.setdefault('population_density', 0)
        
        return hexagons
    
    def calculate_h7_population_from_h8(self, hexagons: List[Dict]) -> List[Dict]:
        """
        Розраховує населення для H3-7 через агрегацію з H3-8
        
        Args:
            hexagons: Список H3-7 гексагонів
            
        Returns:
            Гексагони з розрахованим населенням
        """
        if not hexagons:
            return hexagons
        
        import h3
        
        logger.info("📊 Розрахунок населення H3-7 через агрегацію з H3-8...")
        
        # Завантажуємо кеш H3-8 якщо ще не завантажено
        self.load_h8_population_cache()
        
        # Площа H3-7
        h7_area = 5.161  # км²
        
        # Розраховуємо для кожного H3-7
        calculated = 0
        for hex_data in hexagons:
            h7_hex = hex_data['h3_index']
            
            try:
                # Отримуємо дочірні H3-8
                # ВИКОРИСТОВУЄМО ПРАВИЛЬНУ НАЗВУ ФУНКЦІЇ
                h8_children = h3.cell_to_children(h7_hex, 8)
                
                # Сумуємо населення дочірніх
                total_population = sum(
                    self.h8_population_cache.get(h8_hex, 0) 
                    for h8_hex in h8_children
                )
                
                hex_data['population'] = total_population
                hex_data['population_density'] = total_population / h7_area if total_population > 0 else 0
                
                if total_population > 0:
                    calculated += 1
                    
            except Exception as e:
                logger.warning(f"  Помилка для H3-7 {h7_hex}: {str(e)}")
                hex_data['population'] = 0
                hex_data['population_density'] = 0
        
        logger.info(f"  ✅ Розраховано населення для {calculated:,} з {len(hexagons):,} H3-7 гексагонів")
        
        return hexagons
    
    def calculate_h9_h10_population_from_h8(self, hexagons: List[Dict], target_resolution: int) -> List[Dict]:
        """
        Розраховує населення для H3-9 або H3-10 через дезагрегацію з H3-8
        використовуючи population_corrected з building_footprints
        
        Args:
            hexagons: Список H3-9 або H3-10 гексагонів
            target_resolution: Цільова резолюція (9 або 10)
            
        Returns:
            Гексагони з розрахованим населенням
        """
        if not hexagons or target_resolution not in [9, 10]:
            return hexagons
        
        import h3
        
        logger.info(f"📊 Розрахунок населення H3-{target_resolution} через дезагрегацію з H3-8...")
        
        # Завантажуємо кеш H3-8 якщо ще не завантажено
        self.load_h8_population_cache()
        
        # Отримуємо population_corrected з building_footprints
        h3_field = f'h3_res_{target_resolution}'
        
        with self.pg_conn.connect():
            query = f"""
            SELECT 
                {h3_field} as h3_index,
                SUM(population_corrected) as total_population
            FROM osm_ukraine.building_footprints
            WHERE {h3_field} IS NOT NULL 
                AND population_corrected > 0
            GROUP BY {h3_field}
            """
            self.pg_conn.cursor.execute(query)
            building_data = self.pg_conn.cursor.fetchall()
        
        building_population_map = {
            row['h3_index']: float(row['total_population']) 
            for row in building_data
        }
        logger.info(f"  Завантажено {len(building_population_map):,} H3-{target_resolution} з population_corrected")
        
        # Створюємо мапу H3-{target_resolution} -> H3-8 (parent)
        hex_to_parent_h8 = {}
        for hex_data in hexagons:
            hex_index = hex_data['h3_index']
            try:
                # Знаходимо батьківський H3-8
                # ВИКОРИСТОВУЄМО ПРАВИЛЬНУ НАЗВУ ФУНКЦІЇ
                parent_h8 = h3.cell_to_parent(hex_index, 8)
                hex_to_parent_h8[hex_index] = parent_h8
            except:
                hex_to_parent_h8[hex_index] = None
        
        # Групуємо гексагони за батьківським H3-8
        h8_to_children = {}
        for hex_index, parent_h8 in hex_to_parent_h8.items():
            if parent_h8 and parent_h8 in self.h8_population_cache:
                if parent_h8 not in h8_to_children:
                    h8_to_children[parent_h8] = []
                h8_to_children[parent_h8].append(hex_index)
        
        # Розподіляємо населення для кожного H3-8
        hex_map = {h['h3_index']: h for h in hexagons}
        calculated = 0
        
        for parent_h8, children in h8_to_children.items():
            parent_population = self.h8_population_cache[parent_h8]
            
            # Отримуємо population_corrected для дочірніх
            children_building_pop = {
                child: building_population_map.get(child, 0) 
                for child in children
            }
            
            total_building_pop = sum(children_building_pop.values())
            
            # Розподіляємо населення
            for child_hex in children:
                if child_hex in hex_map:
                    if total_building_pop > 0:
                        # Пропорційний розподіл
                        ratio = children_building_pop[child_hex] / total_building_pop
                        child_population = parent_population * ratio
                    else:
                        # Рівномірний розподіл при відсутності даних
                        child_population = parent_population / len(children)
                    
                    hex_map[child_hex]['population'] = child_population
                    
                    # Розраховуємо щільність
                    area_km2 = 0.105 if target_resolution == 9 else 0.015
                    hex_map[child_hex]['population_density'] = child_population / area_km2
                    
                    if child_population > 0:
                        calculated += 1
        
        # Заповнюємо нулями ті, що не мають батьківського H3-8 з населенням
        for hex_data in hexagons:
            if 'population' not in hex_data:
                hex_data['population'] = 0
                hex_data['population_density'] = 0
        
        logger.info(f"  ✅ Розраховано населення для {calculated:,} з {len(hexagons):,} H3-{target_resolution} гексагонів")
        
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
                
                # Використовуємо простий IN
                if batch_indices:
                    indices_str = ','.join([f"'{idx}'" for idx in batch_indices])
                    query = f"""
                    SELECT 
                        h3_index,
                        cnt_transaction,
                        total_sum,
                        avg_check_per_client,
                        (avg_check_p25 + avg_check_p75) / 2 as avg_check_median
                    FROM osm_ukraine.rbc_h3_data
                    WHERE h3_index IN ({indices_str})
                    """
                    
                    self.pg_conn.cursor.execute(query)
                    results = self.pg_conn.cursor.fetchall()
                else:
                    results = []
                
                for row in results:
                    if row['h3_index'] in hex_map:
                        h = hex_map[row['h3_index']]
                        h['transaction_count'] = int(row['cnt_transaction']) if row['cnt_transaction'] else 0
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
                
                # Використовуємо простий IN
                if batch_indices:
                    indices_str = ','.join([f"'{idx}'" for idx in batch_indices])
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
                    WHERE {h3_field} IN ({indices_str})
                    GROUP BY {h3_field}
                    """
                    
                    self.pg_conn.cursor.execute(query)
                    results = self.pg_conn.cursor.fetchall()
                else:
                    results = []
                
                for row in results:
                    if row['h3_index'] in hex_map:
                        h = hex_map[row['h3_index']]
                        h['poi_total'] = int(row['poi_total']) if row['poi_total'] else 0
                        h['competitors'] = int(row['competitors']) if row['competitors'] else 0
                        h['retail_count'] = int(row['retail_count']) if row['retail_count'] else 0
                        h['food_count'] = int(row['food_count']) if row['food_count'] else 0
                        h['transport_count'] = int(row['transport_count']) if row['transport_count'] else 0
                        h['services_count'] = int(row['services_count']) if row['services_count'] else 0
                        h['transport_nodes'] = int(row['transport_nodes']) if row['transport_nodes'] else 0
                        h['road_segments'] = int(row['road_segments']) if row['road_segments'] else 0
                        h['unique_brands'] = int(row['unique_brands']) if row['unique_brands'] else 0
        
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
            # 1. Population density - вже розрахована в enrich_with_population
            # Тільки перевіряємо що є значення
            if 'population_density' not in h:
                h['population_density'] = 0
            
            # 2. Income index (0-1)
            transaction_sum = h.get('transaction_sum', 0)
            population = h.get('population', 0)
            
            if transaction_sum > 0 and population > 0:
                income_per_capita = transaction_sum / population
                h['income_index'] = min(1.0, income_per_capita / 50000)
            else:
                h['income_index'] = 0
            
            # 3. Competitor intensity (0-1)
            competitors = h.get('competitors', 0)
            h['competitor_intensity'] = min(1.0, competitors / 10) if competitors else 0
            
            # 4. POI density (на км²)
            poi_total = h.get('poi_total', 0)
            h['poi_density'] = poi_total / area_km2 if poi_total else 0
            
            # 5. Accessibility score (0-1)
            transport_score = 0
            transport_nodes = h.get('transport_nodes', 0)
            road_segments = h.get('road_segments', 0)
            
            if transport_nodes > 0:
                transport_score += min(0.5, transport_nodes / 5)
            if road_segments > 0:
                transport_score += min(0.5, road_segments / 10)
            h['accessibility_score'] = transport_score
            
            # 6. Traffic index (0-1)
            traffic_generators = (h.get('retail_count', 0) + 
                                h.get('food_count', 0) + 
                                h.get('services_count', 0))
            h['traffic_index'] = min(1.0, traffic_generators / 20) if traffic_generators else 0
            
            # 7. Retail potential (0-1)
            factors = []
            
            if h['population_density'] > 0:
                factors.append(min(1.0, h['population_density'] / 1000))
            
            if h['income_index'] > 0:
                factors.append(h['income_index'])
            
            if h['accessibility_score'] > 0:
                factors.append(h['accessibility_score'])
            
            if h['traffic_index'] > 0:
                factors.append(h['traffic_index'])
            
            # Негативний фактор конкуренції
            competition_factor = 1.0 - (h['competitor_intensity'] * 0.5)
            
            if factors:
                h['retail_potential'] = (sum(factors) / len(factors)) * competition_factor
            else:
                h['retail_potential'] = 0
            
            # 8. Risk score (0-1)
            risk_factors = []
            
            if h['competitor_intensity'] > 0.5:
                risk_factors.append(h['competitor_intensity'])
            
            if h['population_density'] < 100:
                risk_factors.append(0.5)
            
            if h['income_index'] < 0.3:
                risk_factors.append(0.6)
            
            if h['accessibility_score'] < 0.3:
                risk_factors.append(0.4)
            
            if risk_factors:
                h['risk_score'] = sum(risk_factors) / len(risk_factors)
            else:
                h['risk_score'] = 0.2  # Базовий ризик
            
            # Pre-calculated bins
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
        Структура таблиці h3_analytics: 17 полів
        
        Args:
            hexagons: Список гексагонів з метриками
            
        Returns:
            Трансформовані дані
        """
        transformed = []
        current_time = datetime.now()
        
        for h in hexagons:
            # Створюємо запис відповідно до структури таблиці h3_analytics
            record = {
                # Ідентифікація (2 поля)
                'h3_index': str(h['h3_index']),
                'resolution': int(h['resolution']),
                
                # Core метрики (8 полів)
                'population_density': float(h.get('population_density', 0)),
                'income_index': float(h.get('income_index', 0)),
                'competitor_intensity': float(h.get('competitor_intensity', 0)),
                'poi_density': float(h.get('poi_density', 0)),
                'accessibility_score': float(h.get('accessibility_score', 0)),
                'traffic_index': float(h.get('traffic_index', 0)),
                'retail_potential': float(h.get('retail_potential', 0)),
                'risk_score': float(h.get('risk_score', 0)),
                
                # Pre-calculated bins (3 поля)
                'population_bin': int(h.get('population_bin', 1)),
                'income_bin': int(h.get('income_bin', 1)),
                'competitor_bin': int(h.get('competitor_bin', 1)),
                
                # Географічна прив'язка (3 поля)
                'oblast_id': int(h.get('oblast_id', 0)),
                'raion_id': int(h.get('raion_id', 0)) if h.get('raion_id') else 0,
                'gromada_id': int(h.get('gromada_id', 0)) if h.get('gromada_id') else 0,
                
                # Metadata (1 поле)
                'updated_at': current_time
            }
            # Всього: 2 + 8 + 3 + 3 + 1 = 17 полів
            
            transformed.append(record)
        
        return transformed
    
    def load_to_clickhouse_direct(self, data: List[Dict], oblast_name: str = "") -> int:
        """
        Завантажує дані в ClickHouse БЕЗ ВИКОРИСТАННЯ utils.insert_data
        Прямий INSERT через clickhouse_driver
        
        Args:
            data: Дані для завантаження
            oblast_name: Назва області для логування
            
        Returns:
            Кількість завантажених записів
        """
        if not data:
            return 0
        
        batch_size = 10000
        total_loaded = 0
        
        # НЕ використовуємо контекстний менеджер, щоб уникнути конфлікту з utils
        try:
            # Підключаємося напряму через clickhouse_driver
            from clickhouse_driver import Client
            
            client = Client(
                host=CH_CONFIG['host'],
                port=CH_CONFIG['port'],
                database=CH_CONFIG['database'],
                user=CH_CONFIG['user'],
                password=CH_CONFIG['password']
            )
            
            logger.info(f"📤 Завантаження {len(data)} записів для {oblast_name}")
            
            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                
                try:
                    # Готуємо дані для вставки - ТОЧНО 17 полів
                    insert_data = []
                    for record in batch:
                        insert_data.append([
                            record['h3_index'],           # 1
                            record['resolution'],          # 2
                            record['population_density'],  # 3
                            record['income_index'],        # 4
                            record['competitor_intensity'], # 5
                            record['poi_density'],         # 6
                            record['accessibility_score'],  # 7
                            record['traffic_index'],       # 8
                            record['retail_potential'],    # 9
                            record['risk_score'],          # 10
                            record['population_bin'],      # 11
                            record['income_bin'],          # 12
                            record['competitor_bin'],      # 13
                            record['oblast_id'],           # 14
                            record['raion_id'],            # 15
                            record['gromada_id'],          # 16
                            record['updated_at']           # 17
                        ])
                    
                    # ПРЯМИЙ INSERT БЕЗ utils
                    client.execute(
                        """
                        INSERT INTO geo_analytics.h3_analytics (
                            h3_index, resolution, population_density, income_index,
                            competitor_intensity, poi_density, accessibility_score,
                            traffic_index, retail_potential, risk_score,
                            population_bin, income_bin, competitor_bin,
                            oblast_id, raion_id, gromada_id, updated_at
                        ) VALUES
                        """,
                        insert_data
                    )
                    
                    total_loaded += len(batch)
                    logger.debug(f"  ✅ Завантажено батч: {len(batch)} записів")
                    
                except Exception as e:
                    logger.error(f"❌ Помилка батчу для {oblast_name}: {str(e)[:200]}")
                    continue
            
            # Закриваємо підключення
            client.disconnect()
            
            if total_loaded > 0:
                logger.info(f"✅ Завантажено {total_loaded} записів для {oblast_name}")
            
            return total_loaded
            
        except Exception as e:
            logger.error(f"❌ Критична помилка завантаження: {str(e)}")
            return 0
    
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
        errors = 0
        
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
                
                # 2. Збагачення даними (населення тепер розраховується залежно від резолюції)
                try:
                    hexagons = self.enrich_with_population(hexagons, resolution)
                except Exception as e:
                    logger.warning(f"⚠️ Помилка збагачення населенням для {oblast_name}: {str(e)[:100]}")
                
                try:
                    hexagons = self.enrich_with_economic_data(hexagons)
                except Exception as e:
                    logger.warning(f"⚠️ Помилка збагачення економікою для {oblast_name}: {str(e)[:100]}")
                
                try:
                    hexagons = self.enrich_with_poi_metrics(hexagons, resolution)
                except Exception as e:
                    logger.warning(f"⚠️ Помилка збагачення POI для {oblast_name}: {str(e)[:100]}")
                
                # 3. Розрахунок метрик
                hexagons = self.calculate_metrics(hexagons, resolution)
                
                # 4. Трансформація
                transformed = self.transform_for_clickhouse(hexagons)
                
                # 5. ПРЯМИЙ ЗАВАНТАЖЕННЯ БЕЗ utils
                loaded = self.load_to_clickhouse_direct(transformed, oblast_name)
                total_loaded += loaded
                
                # Оновлюємо прогрес
                oblast_progress.update(1)
                
            except Exception as e:
                errors += 1
                logger.error(f"❌ Помилка для {oblast_name}: {str(e)}")
                import traceback
                logger.debug(f"Детальна помилка: {traceback.format_exc()}")
                oblast_progress.update(1)
                continue
        
        logger.info(f"\n📊 Резолюція H3-{resolution}:")
        logger.info(f"  Витягнуто: {total_extracted:,}")
        logger.info(f"  Завантажено: {total_loaded:,}")
        
        if errors > 0:
            logger.warning(f"  Помилок: {errors}")
        
        if res_config['expected_count'] > 0 and total_loaded > 0:
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
        
        # Підключаємося напряму без utils
        from clickhouse_driver import Client
        
        try:
            client = Client(
                host=CH_CONFIG['host'],
                port=CH_CONFIG['port'],
                database=CH_CONFIG['database'],
                user=CH_CONFIG['user'],
                password=CH_CONFIG['password']
            )
            
            # Загальна статистика
            result = client.execute("""
                SELECT 
                    resolution,
                    COUNT(*) as count,
                    AVG(population_density) as avg_pop,
                    AVG(retail_potential) as avg_potential,
                    AVG(risk_score) as avg_risk,
                    COUNT(DISTINCT oblast_id) as oblasts,
                    MIN(income_index) as min_income,
                    MAX(income_index) as max_income,
                    COUNT(CASE WHEN population_density > 0 THEN 1 END) as with_population
                FROM geo_analytics.h3_analytics
                GROUP BY resolution
                ORDER BY resolution
            """)
            
            if not result:
                logger.warning("⚠️ Таблиця h3_analytics порожня")
                client.disconnect()
                return False
            
            logger.info("\n📊 Статистика по резолюціях:")
            total_count = 0
            
            for row in result:
                res, count, avg_pop, avg_pot, avg_risk, oblasts, min_inc, max_inc, with_pop = row
                total_count += count
                
                logger.info(f"\n  H3-{res}: {count:,} гексагонів")
                logger.info(f"    Областей: {oblasts}")
                logger.info(f"    З населенням: {with_pop:,} ({with_pop/count*100:.1f}%)")
                
                if avg_pop:
                    logger.info(f"    Середня щільність: {avg_pop:.1f} чол/км²")
                if avg_pot:
                    logger.info(f"    Середній потенціал: {avg_pot:.3f}")
                if avg_risk:
                    logger.info(f"    Середній ризик: {avg_risk:.3f}")
                if max_inc and max_inc > 0:
                    logger.info(f"    Діапазон доходів: {min_inc:.3f} - {max_inc:.3f}")
            
            logger.info(f"\n✅ Всього завантажено: {total_count:,} гексагонів")
            
            client.disconnect()
            return total_count > 0
            
        except Exception as e:
            logger.error(f"❌ Помилка верифікації: {e}")
            return False
    
    def run(self, resolutions: Optional[List[int]] = None) -> bool:
        """
        Запускає повний ETL процес для вказаних резолюцій
        ЗМІНЕНО: Новий порядок обробки - спочатку H3-8, потім інші
        
        Args:
            resolutions: Список резолюцій для обробки (за замовчуванням всі)
            
        Returns:
            True якщо успішно
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ПОЧАТОК ETL: H3 гексагони → ClickHouse")
            logger.info("=" * 60)
            
            # Визначаємо які резолюції обробляти
            if resolutions is None:
                # НОВИЙ ПОРЯДОК: спочатку 8, потім 7, потім 9, 10
                resolutions = [8, 7, 9, 10]
            else:
                # Якщо вказані конкретні резолюції - обробляємо в правильному порядку
                ordered_resolutions = []
                if 8 in resolutions:
                    ordered_resolutions.append(8)
                if 7 in resolutions:
                    ordered_resolutions.append(7)
                if 9 in resolutions:
                    ordered_resolutions.append(9)
                if 10 in resolutions:
                    ordered_resolutions.append(10)
                resolutions = ordered_resolutions
            
            logger.info(f"📋 Резолюції для обробки (в порядку виконання): {resolutions}")
            logger.info("   ℹ️ Порядок важливий: H3-8 → H3-7 → H3-9 → H3-10")
            
            # Очищаємо таблицю перед завантаженням
            from clickhouse_driver import Client
            client = Client(
                host=CH_CONFIG['host'],
                port=CH_CONFIG['port'],
                database=CH_CONFIG['database'],
                user=CH_CONFIG['user'],
                password=CH_CONFIG['password']
            )
            client.execute("TRUNCATE TABLE geo_analytics.h3_analytics")
            client.disconnect()
            logger.info("🗑️ Таблиця h3_analytics очищена")
            
            # Обробляємо кожну резолюцію В ПРАВИЛЬНОМУ ПОРЯДКУ
            stats = {}
            for resolution in resolutions:
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
                
                for res in [8, 7, 9, 10]:  # Показуємо в логічному порядку
                    if res in stats:
                        data = stats[res]
                        if data['extracted'] > 0:
                            efficiency = (data['loaded'] / data['extracted']) * 100
                            logger.info(f"  H3-{res}: {data['loaded']:,} / {data['extracted']:,} ({efficiency:.1f}%)")
                        else:
                            logger.info(f"  H3-{res}: немає даних")
                
                logger.info(f"\n📊 Всього оброблено: {self.total_processed:,} гексагонів")
                logger.info(f"\n🎯 Наступні кроки:")
                logger.info(f"  1. Запустіть 02_admin_bins.py для розрахунку bins адмінодиниць")
                logger.info(f"  2. Запустіть 04_h3_bins.py для розрахунку bins гексагонів")
                logger.info(f"  3. Файл 05_recalculate_population.py більше не потрібен!")
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
    import argparse
    
    parser = argparse.ArgumentParser(description='ETL для H3 гексагонів')
    parser.add_argument(
        '--resolutions', 
        type=int, 
        nargs='+',
        default=None,
        help='Резолюції для обробки (наприклад: --resolutions 8 7 9 10)'
    )
    
    args = parser.parse_args()
    
    etl = H3AnalyticsETL()
    
    # Для тестування можна запустити тільки з меншими резолюціями
    # python 03_h3_analytics.py --resolutions 8
    # python 03_h3_analytics.py --resolutions 8 7
    # python 03_h3_analytics.py --resolutions 8 7 9 10
    
    success = etl.run(resolutions=args.resolutions)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()