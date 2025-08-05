#!/usr/bin/env python3
"""
Batch processing script для обробки даних з osm_raw
"""

import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
import uuid

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Додаємо поточну директорію до path
sys.path.insert(0, str(Path(__file__).parent))

# Імпорти модулів
from normalization.tag_parser import TagParser
from normalization.brand_matcher import BrandMatcher

# Використовуємо connection string напряму
DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"

class BatchProcessor:
    """Процесор для batch обробки POI"""
    
    def __init__(self, connection_string=None):
        self.connection_string = connection_string or DB_CONNECTION_STRING
        self.tag_parser = TagParser()
        self.brand_matcher = BrandMatcher()
        self.stats = {
            'processed': 0,
            'poi_found': 0,
            'brands_matched': 0,
            'errors': 0
        }
    
    def process_batch(self, limit=1000, region=None):
        """Обробка batch записів"""
        logger.info(f"🚀 Початок обробки batch (limit={limit}, region={region})")
        
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Вибираємо дані для обробки
            base_query = """
                SELECT id, osm_id, tags, name, brand, 
                       ST_AsText(geom) as geom_wkt,
                       h3_res_7, h3_res_8, h3_res_9, h3_res_10,
                       region_name
                FROM osm_ukraine.osm_raw
                WHERE tags IS NOT NULL
                AND (
                    tags::text LIKE '%%shop%%' 
                    OR tags::text LIKE '%%amenity%%'
                    OR tags::text LIKE '%%brand%%'
                )
            """
            
            if region:
                query = base_query + f" AND region_name = '{region}' LIMIT {limit}"
                logger.info(f"📊 Вибірка даних з регіону {region}...")
                cur.execute(query)
            else:
                query = base_query + f" LIMIT {limit}"
                logger.info(f"📊 Вибірка даних...")
                cur.execute(query)
            
            rows = cur.fetchall()
            logger.info(f"✅ Знайдено {len(rows)} записів для обробки")
            
            # Обробляємо кожен запис
            processed_entities = []
            
            for i, row in enumerate(rows):
                if i % 100 == 0:
                    logger.info(f"  Оброблено {i}/{len(rows)} записів...")
                
                try:
                    entity = self.process_row(row)
                    if entity:
                        processed_entities.append(entity)
                        self.stats['poi_found'] += 1
                except Exception as e:
                    logger.error(f"Помилка обробки запису {row['id']}: {e}")
                    self.stats['errors'] += 1
                
                self.stats['processed'] += 1
            
            # Зберігаємо результати
            if processed_entities:
                logger.info(f"💾 Збереження {len(processed_entities)} оброблених POI...")
                self.save_entities(conn, processed_entities)
            
            # Виводимо статистику
            logger.info("\n📊 Статистика обробки:")
            logger.info(f"  Оброблено записів: {self.stats['processed']}")
            logger.info(f"  Знайдено POI: {self.stats['poi_found']}")
            logger.info(f"  Розпізнано брендів: {self.stats['brands_matched']}")
            logger.info(f"  Помилок: {self.stats['errors']}")
            
        finally:
            cur.close()
            conn.close()
    
    def process_row(self, row):
        """Обробка одного запису"""
        # Парсимо теги
        parsed_tags = self.tag_parser.parse_tags(row['tags'])
        
        # Визначаємо категорії
        primary_cat, secondary_cat = self.tag_parser.get_category_from_tags(parsed_tags.tags)
        
        # Пропускаємо не-POI
        if primary_cat in ['road', 'building', 'landuse', 'other']:
            return None
        
        # Отримуємо назву
        name = parsed_tags.name or row.get('name')
        if not name:
            return None
        
        # Матчимо бренд
        brand_result = self.brand_matcher.match_brand(
            name=name,
            osm_tags=parsed_tags.tags
        )
        
        if brand_result:
            self.stats['brands_matched'] += 1
        
        # Формуємо entity
        entity = {
            'entity_id': str(uuid.uuid4()),
            'osm_id': row['osm_id'],
            'osm_raw_id': row['id'],
            'entity_type': 'poi',
            'primary_category': primary_cat,
            'secondary_category': secondary_cat,
            'name_original': name,
            'name_standardized': name,  # TODO: нормалізація назви
            'brand_normalized': brand_result.canonical_name if brand_result else None,
            'brand_confidence': brand_result.confidence if brand_result else 0.0,
            'brand_match_type': brand_result.match_type if brand_result else 'none',
            'functional_group': brand_result.functional_group if brand_result else self._get_default_group(primary_cat),
            'influence_weight': brand_result.influence_weight if brand_result else 0.0,
            'geom_wkt': row['geom_wkt'],
            'h3_res_7': row['h3_res_7'],
            'h3_res_8': row['h3_res_8'],
            'h3_res_9': row['h3_res_9'],
            'h3_res_10': row['h3_res_10'],
            'quality_score': 0.8 if brand_result else 0.5,  # TODO: реальний quality scoring
            'region_name': row['region_name'],
            'processing_timestamp': datetime.now(),
            'processing_version': '2.0.0'
        }
        
        return entity
    
    def _get_default_group(self, category):
        """Визначає функціональну групу за замовчуванням"""
        if category == 'retail':
            return 'competitor'
        elif category in ['food_service', 'financial', 'healthcare']:
            return 'traffic_generator'
        elif category == 'transport':
            return 'accessibility'
        else:
            return 'neutral'
    
    def save_entities(self, conn, entities):
        """Збереження entities в БД"""
        cur = conn.cursor()
        saved_count = 0
        
        insert_query = """
            INSERT INTO osm_ukraine.poi_processed (
                entity_id, osm_id, osm_raw_id, entity_type,
                primary_category, secondary_category,
                name_original, name_standardized,
                brand_normalized, brand_confidence, brand_match_type,
                functional_group, influence_weight,
                geom, h3_res_7, h3_res_8, h3_res_9, h3_res_10,
                quality_score, region_name,
                processing_timestamp, processing_version
            ) VALUES (
                %(entity_id)s, %(osm_id)s, %(osm_raw_id)s, %(entity_type)s,
                %(primary_category)s, %(secondary_category)s,
                %(name_original)s, %(name_standardized)s,
                %(brand_normalized)s, %(brand_confidence)s, %(brand_match_type)s,
                %(functional_group)s, %(influence_weight)s,
                ST_GeomFromText(%(geom_wkt)s, 4326),
                %(h3_res_7)s, %(h3_res_8)s, %(h3_res_9)s, %(h3_res_10)s,
                %(quality_score)s, %(region_name)s,
                %(processing_timestamp)s, %(processing_version)s
            )
            ON CONFLICT (entity_id) DO NOTHING
        """
        
        for entity in entities:
            try:
                cur.execute(insert_query, entity)
                saved_count += 1
            except Exception as e:
                logger.error(f"Помилка збереження entity {entity['entity_id']}: {e}")
                conn.rollback()  # Rollback для цього запису
                cur = conn.cursor()  # Новий курсор
        
        conn.commit()
        cur.close()
        logger.info(f"✅ Збережено {saved_count}/{len(entities)} entities")


def main():
    """Головна функція"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch processing POI з osm_raw')
    parser.add_argument('--limit', type=int, default=1000, help='Кількість записів для обробки')
    parser.add_argument('--region', type=str, help='Обробити тільки конкретний регіон')
    
    args = parser.parse_args()
    
    processor = BatchProcessor()
    processor.process_batch(limit=args.limit, region=args.region)


if __name__ == "__main__":
    main()