#!/usr/bin/env python3
"""
H3 Analytics Engine V3 - FINAL VERSION
Розширена система агрегації метрик по H3 гексагонах
Інтеграція з новими V3 entity типами (transport + roads)
Точна адаптація до існуючої схеми h3_analytics_current
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"

@dataclass
class H3Metrics:
    """Метрики для одного H3 гексагону"""
    h3_index: str
    resolution: int
    
    # Entity counts
    total_entities: int = 0
    poi_count: int = 0
    transport_count: int = 0
    road_count: int = 0
    
    # Functional groups
    competitors: int = 0
    traffic_generators: int = 0
    accessibility_entities: int = 0
    
    # Influence metrics
    negative_influence: float = 0.0
    positive_influence: float = 0.0
    net_influence: float = 0.0
    
    # Competition analysis
    competition_intensity: float = 0.0
    market_saturation: float = 0.0
    
    # Accessibility analysis  
    transport_accessibility: float = 0.0
    road_accessibility: float = 0.0
    multimodal_accessibility: float = 0.0
    
    # Quality metrics
    avg_quality_score: float = 0.0
    data_completeness: float = 0.0
    
    # Demographics proxies
    residential_indicator: float = 0.0
    commercial_activity: float = 0.0

class H3AnalyticsEngineV3:
    """
    Розширений H3 Analytics Engine для V3 entity types
    FINAL VERSION з точною адаптацією до існуючої схеми БД
    """
    
    def __init__(self, connection_string=None):
        self.connection_string = connection_string or DB_CONNECTION_STRING
        
        # Конфігурація метрик
        self.COMPETITION_RADIUS_KM = 0.5  # Радіус аналізу конкуренції
        self.TRANSPORT_WEIGHT_MULTIPLIER = 1.2  # Множник для transport доступності
        self.ROAD_WEIGHT_MULTIPLIER = 1.0  # Множник для road доступності
        
        logger.info("🔧 H3AnalyticsEngineV3 ініціалізовано")
    
    def calculate_h3_metrics_batch(self, h3_resolution: int = 9, limit: Optional[int] = None) -> List[H3Metrics]:
        """
        Batch розрахунок H3 метрик для всіх гексагонів
        
        Args:
            h3_resolution: H3 резолюція (8, 9, або 10)
            limit: Обмеження кількості гексагонів (для тестування)
        """
        logger.info(f"🧮 Початок batch розрахунку H3 метрик (resolution {h3_resolution})")
        
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Отримуємо всі унікальні H3 гексагони
            h3_column = f'h3_res_{h3_resolution}'
            
            query = f"""
                SELECT DISTINCT {h3_column} as h3_index
                FROM osm_ukraine.poi_processed
                WHERE {h3_column} IS NOT NULL
                ORDER BY {h3_column}
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            cur.execute(query)
            h3_indices = [row['h3_index'] for row in cur.fetchall()]
            
            logger.info(f"📊 Знайдено {len(h3_indices)} унікальних H3 гексагонів")
            
            # Розраховуємо метрики для кожного гексагону
            metrics_list = []
            batch_size = 100
            
            for i in range(0, len(h3_indices), batch_size):
                batch = h3_indices[i:i+batch_size]
                logger.info(f"  Обробка batch {i//batch_size + 1}/{(len(h3_indices)-1)//batch_size + 1}")
                
                batch_metrics = self._calculate_batch_metrics(cur, batch, h3_resolution)
                metrics_list.extend(batch_metrics)
            
            logger.info(f"✅ Розраховано метрики для {len(metrics_list)} гексагонів")
            return metrics_list
            
        finally:
            cur.close()
            conn.close()
    
    def _calculate_batch_metrics(self, cur, h3_indices: List[str], resolution: int) -> List[H3Metrics]:
        """Розрахунок метрик для batch гексагонів"""
        
        h3_column = f'h3_res_{resolution}'
        h3_list_str = "', '".join(h3_indices)
        
        # Основний запит для розрахунку метрик
        query = f"""
        SELECT 
            {h3_column} as h3_index,
            
            -- Entity counts
            COUNT(*) as total_entities,
            COUNT(CASE WHEN entity_type = 'poi' THEN 1 END) as poi_count,
            COUNT(CASE WHEN entity_type = 'transport_node' THEN 1 END) as transport_count,
            COUNT(CASE WHEN entity_type = 'road_segment' THEN 1 END) as road_count,
            
            -- Functional groups
            COUNT(CASE WHEN functional_group = 'competitor' THEN 1 END) as competitors,
            COUNT(CASE WHEN functional_group = 'traffic_generator' THEN 1 END) as traffic_generators,
            COUNT(CASE WHEN functional_group = 'accessibility' THEN 1 END) as accessibility_entities,
            
            -- Influence metrics
            COALESCE(SUM(CASE WHEN influence_weight < 0 THEN influence_weight ELSE 0 END), 0) as negative_influence,
            COALESCE(SUM(CASE WHEN influence_weight > 0 THEN influence_weight ELSE 0 END), 0) as positive_influence,
            COALESCE(SUM(influence_weight), 0) as net_influence,
            
            -- Accessibility metrics (NEW for V3)
            COALESCE(AVG(CASE WHEN entity_type = 'transport_node' AND accessibility_score IS NOT NULL 
                              THEN accessibility_score END), 0) as avg_transport_accessibility,
            COALESCE(AVG(CASE WHEN entity_type = 'road_segment' AND accessibility_score IS NOT NULL 
                              THEN accessibility_score END), 0) as avg_road_accessibility,
            
            -- Quality metrics
            COALESCE(AVG(quality_score), 0) as avg_quality_score,
            
            -- Data completeness
            COUNT(CASE WHEN name_original IS NOT NULL AND length(trim(name_original)) > 0 THEN 1 END)::float / 
            NULLIF(COUNT(*), 0) as data_completeness
            
        FROM osm_ukraine.poi_processed
        WHERE {h3_column} IN ('{h3_list_str}')
        GROUP BY {h3_column}
        """
        
        cur.execute(query)
        results = cur.fetchall()
        
        metrics_list = []
        for row in results:
            # Розраховуємо додаткові метрики з безпечною конвертацією типів
            competition_intensity = self._calculate_competition_intensity(
                int(row['competitors']), int(row['poi_count'])
            )
            
            multimodal_accessibility = self._calculate_multimodal_accessibility(
                row['avg_transport_accessibility'], row['avg_road_accessibility']
            )
            
            market_saturation = self._calculate_market_saturation(
                int(row['competitors']), int(row['total_entities'])
            )
            
            residential_indicator = self._calculate_residential_indicator(
                int(row['poi_count']), int(row['transport_count']), int(row['road_count'])
            )
            
            commercial_activity = self._calculate_commercial_activity(
                int(row['poi_count']), int(row['competitors']), int(row['traffic_generators'])
            )
            
            metrics = H3Metrics(
                h3_index=row['h3_index'],
                resolution=resolution,
                total_entities=int(row['total_entities']),
                poi_count=int(row['poi_count']),
                transport_count=int(row['transport_count']),
                road_count=int(row['road_count']),
                competitors=int(row['competitors']),
                traffic_generators=int(row['traffic_generators']),
                accessibility_entities=int(row['accessibility_entities']),
                negative_influence=float(row['negative_influence']),
                positive_influence=float(row['positive_influence']),
                net_influence=float(row['net_influence']),
                competition_intensity=competition_intensity,
                market_saturation=market_saturation,
                transport_accessibility=float(row['avg_transport_accessibility']),
                road_accessibility=float(row['avg_road_accessibility']),
                multimodal_accessibility=multimodal_accessibility,
                avg_quality_score=float(row['avg_quality_score']),
                data_completeness=float(row['data_completeness']),
                residential_indicator=residential_indicator,
                commercial_activity=commercial_activity
            )
            
            metrics_list.append(metrics)
        
        return metrics_list
    
    def _calculate_competition_intensity(self, competitors: int, total_poi: int) -> float:
        """Розрахунок інтенсивності конкуренції"""
        if total_poi == 0:
            return 0.0
        
        competition_ratio = competitors / total_poi
        
        # Нормалізуємо в діапазон 0-1
        if competition_ratio <= 0.2:
            return competition_ratio * 2.5  # 0-0.5
        elif competition_ratio <= 0.6:
            return 0.5 + (competition_ratio - 0.2) * 1.25  # 0.5-1.0
        else:
            return min(1.0, 0.5 + (competition_ratio - 0.2) * 1.25)
    
    def _calculate_multimodal_accessibility(self, transport_access, road_access) -> float:
        """Розрахунок мультимодальної доступності (NEW для V3)"""
        # Конвертуємо Decimal в float для безпечних обчислень
        transport_access = float(transport_access) if transport_access is not None else 0.0
        road_access = float(road_access) if road_access is not None else 0.0
        
        if transport_access == 0 and road_access == 0:
            return 0.0
        
        # Зважена комбінація transport та road accessibility
        weighted_score = (
            transport_access * self.TRANSPORT_WEIGHT_MULTIPLIER +
            road_access * self.ROAD_WEIGHT_MULTIPLIER
        ) / (self.TRANSPORT_WEIGHT_MULTIPLIER + self.ROAD_WEIGHT_MULTIPLIER)
        
        return min(1.0, weighted_score)
    
    def _calculate_market_saturation(self, competitors: int, total_entities: int) -> float:
        """Розрахунок насиченості ринку"""
        if total_entities == 0:
            return 0.0
        
        # Враховуємо не тільки конкурентів, а й загальну щільність entities
        density_factor = min(1.0, total_entities / 50)  # Нормалізація до 50 entities
        competition_factor = competitors / total_entities
        
        return min(1.0, density_factor * 0.6 + competition_factor * 0.4)
    
    def _calculate_residential_indicator(self, poi_count: int, transport_count: int, road_count: int) -> float:
        """Індикатор житлової активності (демографічний проксі)"""
        # Більше транспортних вузлів та доріг = більша житлова активність
        transport_score = min(1.0, transport_count / 5)  # Нормалізація до 5 transport nodes
        road_score = min(1.0, road_count / 10)  # Нормалізація до 10 road segments
        poi_score = min(1.0, poi_count / 20)  # Нормалізація до 20 POI
        
        # Зважена комбінація
        return (transport_score * 0.4 + road_score * 0.3 + poi_score * 0.3)
    
    def _calculate_commercial_activity(self, poi_count: int, competitors: int, traffic_generators: int) -> float:
        """Індикатор комерційної активності"""
        if poi_count == 0:
            return 0.0
        
        commercial_density = min(1.0, poi_count / 30)  # Нормалізація до 30 POI
        competition_factor = competitors / poi_count if poi_count > 0 else 0
        traffic_factor = traffic_generators / poi_count if poi_count > 0 else 0
        
        return min(1.0, commercial_density * 0.5 + competition_factor * 0.3 + traffic_factor * 0.2)
    
    def _get_h3_area_km2(self, resolution: int) -> float:
        """Отримання площі H3 гексагону в км² за резолюцією"""
        # Приблизні площі H3 гексагонів за резолюціями
        h3_areas = {
            7: 5.16,      # ~5.16 км²
            8: 0.737,     # ~0.737 км²
            9: 0.105,     # ~0.105 км²
            10: 0.0150    # ~0.015 км²
        }
        
        return h3_areas.get(resolution, 0.105)  # default res 9
    
    def save_metrics_to_database(self, metrics_list: List[H3Metrics], update_mode: str = 'upsert'):
        """
        Збереження розрахованих метрик в h3_analytics_current
        ТОЧНА АДАПТАЦІЯ до існуючої схеми таблиці
        """
        logger.info(f"💾 Збереження {len(metrics_list)} H3 метрик в базу")
        
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor()
        
        try:
            # SQL запит точно відповідає існуючим полям таблиці
            insert_query = """
                INSERT INTO osm_ukraine.h3_analytics_current (
                    h3_index, resolution, last_updated,
                    poi_total_count, retail_count, competitor_count, 
                    traffic_generator_count, infrastructure_count,
                    poi_density, retail_density, competition_intensity,
                    total_positive_influence, total_negative_influence, net_influence_score,
                    transport_accessibility_score, road_density_km_per_km2, average_road_quality,
                    avg_poi_quality, data_completeness,
                    residential_indicator_score, commercial_activity_score, market_saturation_index,
                    calculation_timestamp, processing_version
                ) VALUES (
                    %(h3_index)s, %(resolution)s, %(last_updated)s,
                    %(poi_total_count)s, %(retail_count)s, %(competitor_count)s,
                    %(traffic_generator_count)s, %(infrastructure_count)s,
                    %(poi_density)s, %(retail_density)s, %(competition_intensity)s,
                    %(total_positive_influence)s, %(total_negative_influence)s, %(net_influence_score)s,
                    %(transport_accessibility_score)s, %(road_density_km_per_km2)s, %(average_road_quality)s,
                    %(avg_poi_quality)s, %(data_completeness)s,
                    %(residential_indicator_score)s, %(commercial_activity_score)s, %(market_saturation_index)s,
                    %(calculation_timestamp)s, %(processing_version)s
                )
                ON CONFLICT (h3_index, resolution) DO UPDATE SET
                    last_updated = EXCLUDED.last_updated,
                    poi_total_count = EXCLUDED.poi_total_count,
                    retail_count = EXCLUDED.retail_count,
                    competitor_count = EXCLUDED.competitor_count,
                    traffic_generator_count = EXCLUDED.traffic_generator_count,
                    infrastructure_count = EXCLUDED.infrastructure_count,
                    poi_density = EXCLUDED.poi_density,
                    retail_density = EXCLUDED.retail_density,
                    competition_intensity = EXCLUDED.competition_intensity,
                    total_positive_influence = EXCLUDED.total_positive_influence,
                    total_negative_influence = EXCLUDED.total_negative_influence,
                    net_influence_score = EXCLUDED.net_influence_score,
                    transport_accessibility_score = EXCLUDED.transport_accessibility_score,
                    road_density_km_per_km2 = EXCLUDED.road_density_km_per_km2,
                    average_road_quality = EXCLUDED.average_road_quality,
                    avg_poi_quality = EXCLUDED.avg_poi_quality,
                    data_completeness = EXCLUDED.data_completeness,
                    residential_indicator_score = EXCLUDED.residential_indicator_score,
                    commercial_activity_score = EXCLUDED.commercial_activity_score,
                    market_saturation_index = EXCLUDED.market_saturation_index,
                    calculation_timestamp = EXCLUDED.calculation_timestamp,
                    processing_version = EXCLUDED.processing_version
            """
            
            saved_count = 0
            current_time = datetime.now()
            
            for metrics in metrics_list:
                try:
                    # Розраховуємо додаткові метрики які є в таблиці
                    
                    # Density metrics (на км²) 
                    h3_area_km2 = self._get_h3_area_km2(metrics.resolution)
                    poi_density = metrics.poi_count / h3_area_km2 if h3_area_km2 > 0 else 0
                    retail_density = metrics.poi_count / h3_area_km2 if h3_area_km2 > 0 else 0
                    road_density = metrics.road_count / h3_area_km2 if h3_area_km2 > 0 else 0
                    
                    # Average road quality з accessibility score
                    avg_road_quality = metrics.road_accessibility if metrics.road_accessibility > 0 else 0.5
                    
                    params = {
                        # Базові поля
                        'h3_index': metrics.h3_index,
                        'resolution': metrics.resolution,
                        'last_updated': current_time,
                        
                        # Лічильники
                        'poi_total_count': metrics.poi_count,
                        'retail_count': metrics.poi_count,  # припускаємо що більшість POI - retail
                        'competitor_count': metrics.competitors,
                        'traffic_generator_count': metrics.traffic_generators,
                        'infrastructure_count': metrics.accessibility_entities,
                        
                        # Density metrics
                        'poi_density': round(poi_density, 2),
                        'retail_density': round(retail_density, 2),
                        'road_density_km_per_km2': round(road_density, 2),
                        
                        # Competition & influence
                        'competition_intensity': round(metrics.competition_intensity, 3),
                        'total_positive_influence': round(metrics.positive_influence, 2),
                        'total_negative_influence': round(metrics.negative_influence, 2),
                        'net_influence_score': round(metrics.net_influence, 2),
                        'market_saturation_index': round(metrics.market_saturation, 3),
                        
                        # Accessibility
                        'transport_accessibility_score': round(metrics.transport_accessibility, 3),
                        'average_road_quality': round(avg_road_quality, 3),
                        
                        # Quality metrics
                        'avg_poi_quality': round(metrics.avg_quality_score, 3),
                        'data_completeness': round(metrics.data_completeness, 3),
                        
                        # Demographics proxies
                        'residential_indicator_score': round(metrics.residential_indicator, 3),
                        'commercial_activity_score': round(metrics.commercial_activity, 3),
                        
                        # Metadata
                        'calculation_timestamp': current_time,
                        'processing_version': '3.0'
                    }
                    
                    cur.execute(insert_query, params)
                    saved_count += 1
                    
                except Exception as e:
                    logger.error(f"Помилка збереження метрик для {metrics.h3_index}: {e}")
                    # Продовжуємо з наступними записами
                    continue
            
            conn.commit()
            logger.info(f"✅ Збережено {saved_count}/{len(metrics_list)} H3 метрик")
            
        except Exception as e:
            logger.error(f"💥 Критична помилка збереження: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def update_single_h3_metrics(self, h3_index: str, resolution: int) -> Optional[H3Metrics]:
        """
        Оновлення метрик для одного H3 гексагону (для incremental updates)
        """
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            metrics_list = self._calculate_batch_metrics(cur, [h3_index], resolution)
            
            if metrics_list:
                self.save_metrics_to_database(metrics_list)
                return metrics_list[0]
            
            return None
            
        finally:
            cur.close()
            conn.close()
    
    def get_h3_analytics_summary(self, resolution: int = 9) -> Dict:
        """
        Отримання загальної статистики H3 analytics
        """
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            query = """
            SELECT 
                COUNT(*) as total_hexes,
                SUM(poi_total_count) as total_poi,
                SUM(competitor_count) as total_competitors,
                SUM(traffic_generator_count) as total_traffic_generators,
                SUM(infrastructure_count) as total_infrastructure,
                ROUND(AVG(competition_intensity), 3) as avg_competition,
                ROUND(AVG(transport_accessibility_score), 3) as avg_transport_access,
                ROUND(AVG(residential_indicator_score), 3) as avg_residential,
                ROUND(AVG(commercial_activity_score), 3) as avg_commercial,
                MAX(last_updated) as last_calculation
            FROM osm_ukraine.h3_analytics_current
            WHERE resolution = %s
            AND processing_version = '3.0'
            """
            
            cur.execute(query, (resolution,))
            result = cur.fetchone()
            
            return dict(result) if result else {}
            
        finally:
            cur.close()
            conn.close()

def main():
    """Тестування H3 Analytics Engine V3"""
    import argparse
    
    parser = argparse.ArgumentParser(description='H3 Analytics Engine V3 - FINAL')
    parser.add_argument('--resolution', type=int, default=9, choices=[7, 8, 9, 10], 
                       help='H3 resolution')
    parser.add_argument('--limit', type=int, help='Limit hexagons for testing')
    parser.add_argument('--test-only', action='store_true', help='Calculate but do not save')
    parser.add_argument('--summary', action='store_true', help='Show analytics summary')
    
    args = parser.parse_args()
    
    engine = H3AnalyticsEngineV3()
    
    if args.summary:
        # Показати статистику
        summary = engine.get_h3_analytics_summary(args.resolution)
        logger.info(f"\n📊 H3 Analytics Summary (resolution {args.resolution}):")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")
        return
    
    # Розраховуємо метрики
    metrics = engine.calculate_h3_metrics_batch(
        h3_resolution=args.resolution,
        limit=args.limit
    )
    
    if metrics:
        logger.info(f"\n📊 Приклад розрахованих метрик:")
        sample = metrics[0]
        logger.info(f"H3: {sample.h3_index}")
        logger.info(f"Entities: {sample.total_entities} (POI: {sample.poi_count}, Transport: {sample.transport_count}, Roads: {sample.road_count})")
        logger.info(f"Competition: {sample.competition_intensity:.3f}")
        logger.info(f"Multimodal access: {sample.multimodal_accessibility:.3f}")
        logger.info(f"Residential indicator: {sample.residential_indicator:.3f}")
        logger.info(f"Commercial activity: {sample.commercial_activity:.3f}")
        
        if not args.test_only:
            engine.save_metrics_to_database(metrics)
            
            # Показати підсумок після збереження
            summary = engine.get_h3_analytics_summary(args.resolution)
            logger.info(f"\n📈 Updated Summary:")
            logger.info(f"Total hexes: {summary.get('total_hexes', 0)}")
            logger.info(f"Total POI: {summary.get('total_poi', 0)}")
        else:
            logger.info("🧪 Тестовий режим - дані не збережені")

if __name__ == "__main__":
    main()