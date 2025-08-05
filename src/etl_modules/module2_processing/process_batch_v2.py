#!/usr/bin/env python3
"""
Batch processing script V2 - з покращеним brand matching та tracking невідомих брендів
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
from normalization.brand_manager import BrandManager

# Database connection
DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"

class BatchProcessorV2:
    """Покращений процесор з tracking невідомих брендів"""
    
    def __init__(self, connection_string=None):
        self.connection_string = connection_string or DB_CONNECTION_STRING
        self.tag_parser = TagParser()
        
        # Налаштовуємо brand matcher з вищим threshold
        matcher_config = {
            'algorithms': {
                'exact': {'enabled': True, 'priority': 1},
                'fuzzy': {
                    'enabled': True, 
                    'priority': 2,
                    'threshold': 0.95,  # Підвищений threshold!
                    'algorithm': 'token_sort_ratio'
                },
                'osm_tags': {'enabled': True, 'priority': 3},
                'keywords': {
                    'enabled': False,  # Вимкнено щоб уникнути false positives
                    'priority': 4,
                    'min_confidence': 0.8
                }
            },
            'cache': {
                'enabled': True,
                'max_size': 10000
            },
            'quality': {
                'min_confidence': 0.9,  # Високий мінімум!
                'auto_approve_threshold': 0.95
            }
        }
        
        self.brand_matcher = BrandMatcher(config=matcher_config)
        self.brand_manager = BrandManager(self.connection_string)
        
        self.stats = {
            'processed': 0,
            'poi_found': 0,
            'brands_matched': 0,
            'unknown_brands': 0,
            'errors': 0
        }
        
        # Для tracking невідомих брендів в batch
        self.unknown_brands = {}
    
    def process_batch(self, limit=1000, region=None, batch_size=1000):
        """Обробка batch записів з покращеним error handling"""
        logger.info(f"🚀 Початок обробки batch V2 (limit={limit}, region={region})")
        
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
                AND id NOT IN (
                    SELECT osm_raw_id FROM osm_ukraine.poi_processed 
                    WHERE osm_raw_id IS NOT NULL
                )
            """
            
            if region:
                query = base_query + f" AND region_name = '{region}' LIMIT {limit}"
                logger.info(f"📊 Вибірка даних з регіону {region}...")
            else:
                query = base_query + f" LIMIT {limit}"
                logger.info(f"📊 Вибірка даних...")
            
            cur.execute(query)
            rows = cur.fetchall()
            logger.info(f"✅ Знайдено {len(rows)} нових записів для обробки")
            
            # Обробляємо батчами
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                logger.info(f"  Обробка batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1}")
                
                processed_entities = []
                
                for row in batch:
                    try:
                        entity = self.process_row(row)
                        if entity:
                            processed_entities.append(entity)
                            self.stats['poi_found'] += 1
                    except Exception as e:
                        logger.error(f"Помилка обробки запису {row['id']}: {e}")
                        self.stats['errors'] += 1
                    
                    self.stats['processed'] += 1
                
                # Зберігаємо batch
                if processed_entities:
                    self.save_entities(conn, processed_entities)
            
            # Зберігаємо невідомі бренди
            self.save_unknown_brands()
            
            # Виводимо статистику
            self.print_statistics()
            
        finally:
            cur.close()
            conn.close()
    
    def process_row(self, row):
        """Обробка одного запису з tracking невідомих брендів"""
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
        
        # Матчимо бренд з високим threshold
        brand_result = self.brand_matcher.match_brand(
            name=name,
            osm_tags=parsed_tags.tags
        )
        
        # Якщо бренд не розпізнано або низька впевненість
        if not brand_result or brand_result.confidence < 0.9:
            # Записуємо як невідомий бренд
            self.track_unknown_brand(name, row['region_name'], primary_cat)
            
            # Скидаємо brand result якщо низька впевненість
            if brand_result and brand_result.confidence < 0.9:
                logger.debug(f"Відхилено нечітке співпадіння: {name} -> {brand_result.canonical_name} (conf: {brand_result.confidence})")
                brand_result = None
        else:
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
            'name_standardized': name,
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
            'quality_score': 0.8 if brand_result else 0.5,
            'region_name': row['region_name'],
            'processing_timestamp': datetime.now(),
            'processing_version': '2.0.1'
        }
        
        return entity
    
    def track_unknown_brand(self, name, region, category):
        """Відстежує невідомі бренди"""
        # Пропускаємо загальні назви
        generic_names = ['продукти', 'магазин', 'аптека', 'кафе', 'ресторан', 'банк']
        if name.lower() in generic_names:
            return
        
        if name not in self.unknown_brands:
            self.unknown_brands[name] = {
                'count': 0,
                'regions': set(),
                'categories': set()
            }
        
        self.unknown_brands[name]['count'] += 1
        self.unknown_brands[name]['regions'].add(region)
        self.unknown_brands[name]['categories'].add(category)
        self.stats['unknown_brands'] += 1
    
    def save_unknown_brands(self):
        """Зберігає невідомі бренди в brand manager"""
        logger.info(f"\n📝 Збереження {len(self.unknown_brands)} невідомих брендів...")
        
        for name, data in self.unknown_brands.items():
            if data['count'] >= 2:  # Мінімум 2 появи
                for region in data['regions']:
                    self.brand_manager.record_unknown_brand(
                        name=name,
                        region=region,
                        category=list(data['categories'])[0] if data['categories'] else None
                    )
        
        # Виводимо топ невідомих брендів
        top_unknown = sorted(self.unknown_brands.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        
        if top_unknown:
            logger.info("\n🔍 Топ-10 невідомих брендів:")
            for name, data in top_unknown:
                logger.info(f"  - {name}: {data['count']} разів")
    
    def save_entities(self, conn, entities):
        """Збереження entities з proper error handling"""
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
        
        # Кожен запис в окремій транзакції
        for entity in entities:
            try:
                cur = conn.cursor()
                cur.execute(insert_query, entity)
                conn.commit()
                cur.close()
                saved_count += 1
            except Exception as e:
                conn.rollback()
                logger.error(f"Помилка збереження: {e}")
                logger.debug(f"Entity: {entity.get('name_original')}, Geom: {entity.get('geom_wkt')[:50]}")
        
        logger.info(f"✅ Збережено {saved_count}/{len(entities)} entities")
    
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
    
    def print_statistics(self):
        """Виводить детальну статистику"""
        logger.info("\n📊 Статистика обробки V2:")
        logger.info(f"  Оброблено записів: {self.stats['processed']:,}")
        logger.info(f"  Знайдено POI: {self.stats['poi_found']:,}")
        logger.info(f"  Розпізнано брендів: {self.stats['brands_matched']:,}")
        logger.info(f"  Невідомих брендів: {self.stats['unknown_brands']:,}")
        logger.info(f"  Помилок: {self.stats['errors']:,}")
        
        if self.stats['poi_found'] > 0:
            brand_rate = (self.stats['brands_matched'] / self.stats['poi_found']) * 100
            logger.info(f"  Brand recognition rate: {brand_rate:.1f}%")


def main():
    """Головна функція"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Batch processing POI V2')
    parser.add_argument('--limit', type=int, default=1000, help='Кількість записів')
    parser.add_argument('--region', type=str, help='Конкретний регіон')
    parser.add_argument('--batch-size', type=int, default=1000, help='Розмір batch')
    
    args = parser.parse_args()
    
    processor = BatchProcessorV2()
    processor.process_batch(
        limit=args.limit, 
        region=args.region,
        batch_size=args.batch_size
    )


if __name__ == "__main__":
    main()