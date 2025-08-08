#!/usr/bin/env python3
"""
Process Entities V3 - Universal Entity Processing Pipeline
Розширення process_batch_v2.py для обробки POI + Transport + Roads
"""

import sys
import json
import uuid
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Додаємо поточну директорію до path
sys.path.insert(0, str(Path(__file__).parent))

# Імпорти компонентів (з існуючих та нових модулів)
from normalization.tag_parser import TagParser
from normalization.brand_matcher import BrandMatcher
from normalization.entity_classifier import EntityClassifier

# Connection string
DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"

class EntityProcessorV3:
    """
    Universal Entity Processor - розширення V2 для transport та road entities
    """
    
    def __init__(self, connection_string=None):
        self.connection_string = connection_string or DB_CONNECTION_STRING
        
        # Ініціалізуємо компоненти
        self.tag_parser = TagParser()
        self.brand_matcher = BrandMatcher() 
        self.entity_classifier = EntityClassifier()
        
        # Статистика
        self.stats = {
            'processed': 0,
            'poi_found': 0,
            'transport_found': 0, 
            'road_found': 0,
            'brands_matched': 0,
            'errors': 0,
            'skipped': 0
        }
        
        # Functional group weights
        self.FUNCTIONAL_WEIGHTS = {
            # POI weights (з існуючої V2 логіки)
            'competitor': -0.8,
            'traffic_generator': 0.6,
            
            # Transport weights (нові)
            'accessibility': {
                'bus_stop': 0.4,
                'bus_station': 0.6, 
                'train_station': 0.8,
                'metro_station': 0.9,
                'tram_stop': 0.3,
                'transport_node': 0.4  # generic
            },
            
            # Road weights (нові) 
            'road_accessibility': {
                'motorway': 0.9,
                'trunk': 0.8,
                'primary': 0.7,
                'secondary': 0.6,
                'tertiary': 0.5,
                'residential': 0.2,
                'service': 0.1,
                'unclassified': 0.3
            }
        }
        
        logger.info("🚀 EntityProcessorV3 ініціалізовано")
    
    def process_batch(self, limit=1000, region=None, batch_size=1000):
        """
        Головний метод для batch обробки записів
        Розширення process_batch_v2.py логіки
        """
        logger.info(f"🚀 Початок V3 обробки batch (limit={limit}, region={region})")
        
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Вибираємо дані для обробки (розширений запит)
            base_query = """
                SELECT id, osm_id, tags, name, brand, 
                       ST_AsText(geom) as geom_wkt,
                       ST_GeometryType(geom) as geom_type,
                       h3_res_7, h3_res_8, h3_res_9, h3_res_10,
                       region_name
                FROM osm_ukraine.osm_raw
                WHERE tags IS NOT NULL
                AND (
                    tags::text LIKE '%%shop%%' 
                    OR tags::text LIKE '%%amenity%%'
                    OR tags::text LIKE '%%highway%%'
                    OR tags::text LIKE '%%public_transport%%'
                    OR tags::text LIKE '%%railway%%'
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
            logger.info(f"✅ Знайдено {len(rows)} нових записів для V3 обробки")
            
            # Обробляємо батчами
            all_entities = []
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                logger.info(f"  Обробка V3 batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1} ({len(batch)} записів)")
                
                entities = self.process_records_batch(batch)
                all_entities.extend(entities)
                
                # Періодичне збереження
                if len(all_entities) >= 500:
                    self.save_entities(conn, all_entities)
                    all_entities = []
            
            # Збереження залишків
            if all_entities:
                self.save_entities(conn, all_entities)
            
            self.print_statistics()
            
        except Exception as e:
            logger.error(f"❌ Помилка V3 batch processing: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    
    def process_records_batch(self, records: List[Dict]) -> List[Dict]:
        """
        Обробка batch записів з класифікацією по типах
        """
        entities = []
        
        for record in records:
            try:
                self.stats['processed'] += 1
                
                # Парсимо складні JSON теги з osm_raw
                tags = self._parse_complex_tags(record.get('tags'))
                if not tags:
                    self.stats['skipped'] += 1
                    continue
                
                # Класифікуємо entity type
                entity_type = self.entity_classifier.classify_entity_type(tags)
                if not entity_type:
                    self.stats['skipped'] += 1
                    continue
                
                # Обробляємо по типу
                if entity_type == 'poi':
                    entity = self.process_poi(record, tags)
                    if entity:
                        self.stats['poi_found'] += 1
                elif entity_type == 'transport_node':
                    entity = self.process_transport_node(record, tags)
                    if entity:
                        self.stats['transport_found'] += 1
                elif entity_type == 'road_segment':
                    entity = self.process_road_segment(record, tags)
                    if entity:
                        self.stats['road_found'] += 1
                else:
                    continue
                
                if entity:
                    entities.append(entity)
                    
            except Exception as e:
                logger.error(f"Помилка обробки запису {record.get('id', 'unknown')}: {e}")
                self.stats['errors'] += 1
                continue
        
        return entities
    
    def process_poi(self, record: Dict, tags: Dict[str, str]) -> Optional[Dict]:
        """
        Обробка POI - використовуємо існуючу V2 логіку з незначними змінами
        """
        try:
            # Використовуємо існуючий brand matcher
            name = record.get('name') or tags.get('name', '')
            brand_result = None
            
            if name:
                brand_result = self.brand_matcher.match_brand(name, tags)
                if brand_result and brand_result.confidence > 0.8:
                    self.stats['brands_matched'] += 1
            
            # Визначаємо категорії (існуюча V2 логіка)
            primary_category, secondary_category = self._get_poi_categories(tags)
            
            # Функціональна група (існуюча логіка)
            functional_group = self._get_poi_functional_group(primary_category, secondary_category)
            influence_weight = self.FUNCTIONAL_WEIGHTS.get(functional_group, 0.0)
            
            entity = {
                'entity_id': str(uuid.uuid4()),
                'osm_id': record.get('osm_id'),
                'osm_raw_id': record.get('id'),
                'entity_type': 'poi',
                'primary_category': primary_category,
                'secondary_category': secondary_category,
                'name_original': name,
                'name_standardized': name,  # TODO: додати стандартизацію
                'brand_normalized': brand_result.brand_name if brand_result else None,
                'brand_confidence': brand_result.confidence if brand_result else 0.0,
                'brand_match_type': brand_result.match_type if brand_result else 'none',
                'functional_group': functional_group,
                'influence_weight': influence_weight,
                'geom_wkt': record.get('geom_wkt'),
                'h3_res_7': record.get('h3_res_7'),
                'h3_res_8': record.get('h3_res_8'),
                'h3_res_9': record.get('h3_res_9'),
                'h3_res_10': record.get('h3_res_10'),
                'highway_type': None,
                'max_speed': None,
                'accessibility_score': None,
                'quality_score': 0.8,  # Default quality
                'region_name': record.get('region_name'),
                'processing_timestamp': datetime.now(),
                'processing_version': '3.0'
            }
            
            return entity
            
        except Exception as e:
            logger.error(f"Помилка обробки POI: {e}")
            return None
    
    def process_transport_node(self, record: Dict, tags: Dict[str, str]) -> Optional[Dict]:
        """
        Обробка Transport Node - нова логіка для V3
        """
        try:
            # Назва транспортного вузла
            name = record.get('name') or tags.get('name', '')
            
            # Визначаємо підтип транспорту
            transport_subtype = self._get_transport_subtype(tags)
            
            # Розрахунок accessibility score
            accessibility_score = self._calculate_transport_accessibility(tags, transport_subtype)
            
            # Функціональна група та вага
            functional_group = 'accessibility'
            influence_weight = self.FUNCTIONAL_WEIGHTS['accessibility'].get(
                transport_subtype, 
                self.FUNCTIONAL_WEIGHTS['accessibility']['transport_node']
            )
            
            entity = {
                'entity_id': str(uuid.uuid4()),
                'osm_id': record.get('osm_id'),
                'osm_raw_id': record.get('id'),
                'entity_type': 'transport_node',
                'primary_category': 'transport',
                'secondary_category': transport_subtype,
                'name_original': name,
                'name_standardized': name,
                'brand_normalized': None,  # Transport nodes не мають брендів в нашій логіці
                'brand_confidence': 0.0,
                'brand_match_type': 'none',
                'functional_group': functional_group,
                'influence_weight': influence_weight,
                'geom_wkt': record.get('geom_wkt'),
                'h3_res_7': record.get('h3_res_7'),
                'h3_res_8': record.get('h3_res_8'),
                'h3_res_9': record.get('h3_res_9'),
                'h3_res_10': record.get('h3_res_10'),
                'highway_type': tags.get('highway') if tags.get('highway') == 'bus_stop' else None,
                'max_speed': None,
                'accessibility_score': accessibility_score,
                'quality_score': self._calculate_transport_quality(tags, name),
                'region_name': record.get('region_name'),
                'processing_timestamp': datetime.now(),
                'processing_version': '3.0'
            }
            
            return entity
            
        except Exception as e:
            logger.error(f"Помилка обробки transport node: {e}")
            return None
    
    def process_road_segment(self, record: Dict, tags: Dict[str, str]) -> Optional[Dict]:
        """
        Обробка Road Segment - нова логіка для V3
        """
        try:
            # Назва дороги
            name = record.get('name') or tags.get('name', '')
            
            # Тип дороги
            highway_type = tags.get('highway', 'unclassified')
            road_subtype = self._get_road_subtype(tags)
            
            # Обмеження швидкості
            max_speed = self._parse_speed_limit(tags.get('maxspeed'))
            
            # Розрахунок accessibility score
            accessibility_score = self._calculate_road_accessibility(tags, highway_type)
            
            # Функціональна група та вага
            functional_group = 'accessibility'
            influence_weight = self.FUNCTIONAL_WEIGHTS['road_accessibility'].get(
                road_subtype,
                self.FUNCTIONAL_WEIGHTS['road_accessibility']['unclassified']
            )
            
            entity = {
                'entity_id': str(uuid.uuid4()),
                'osm_id': record.get('osm_id'),
                'osm_raw_id': record.get('id'),
                'entity_type': 'road_segment',
                'primary_category': 'road',
                'secondary_category': road_subtype,
                'name_original': name,
                'name_standardized': name,
                'brand_normalized': None,  # Roads не мають брендів
                'brand_confidence': 0.0,
                'brand_match_type': 'none',
                'functional_group': functional_group,
                'influence_weight': influence_weight,
                'geom_wkt': record.get('geom_wkt'),
                'h3_res_7': record.get('h3_res_7'),
                'h3_res_8': record.get('h3_res_8'),
                'h3_res_9': record.get('h3_res_9'),
                'h3_res_10': record.get('h3_res_10'),
                'highway_type': highway_type,
                'max_speed': max_speed,
                'accessibility_score': accessibility_score,
                'quality_score': self._calculate_road_quality(tags),
                'region_name': record.get('region_name'),
                'processing_timestamp': datetime.now(),
                'processing_version': '3.0'
            }
            
            return entity
            
        except Exception as e:
            logger.error(f"Помилка обробки road segment: {e}")
            return None
    
    def _parse_complex_tags(self, tags_field: Any) -> Dict[str, str]:
        """
        Парсить складну JSON структуру з osm_raw.tags
        Використовує логіку з TagParserExtension
        """
        if not tags_field:
            return {}
        
        try:
            if isinstance(tags_field, str):
                outer_json = json.loads(tags_field)
            elif isinstance(tags_field, dict):
                outer_json = tags_field
            else:
                return {}
            
            inner_tags_string = outer_json.get('tags', '{}')
            if not inner_tags_string or inner_tags_string == '{}':
                return {}
            
            inner_tags = json.loads(inner_tags_string)
            
            # Очищуємо теги
            cleaned_tags = {}
            for key, value in inner_tags.items():
                if key and value is not None:
                    cleaned_key = str(key).strip()
                    cleaned_value = str(value).strip()
                    if cleaned_key and cleaned_value:
                        cleaned_tags[cleaned_key] = cleaned_value
            
            return cleaned_tags
            
        except Exception as e:
            logger.warning(f"Error parsing complex tags: {e}")
            return {}
    
    def _get_poi_categories(self, tags: Dict[str, str]) -> tuple:
        """
        Визначає POI категорії - використовуємо існуючу V2 логіку
        """
        if 'shop' in tags:
            return 'retail', tags['shop']
        
        if 'amenity' in tags:
            amenity = tags['amenity']
            if amenity in ['restaurant', 'cafe', 'fast_food', 'bar', 'pub']:
                return 'food_service', amenity
            elif amenity in ['pharmacy', 'hospital', 'clinic', 'doctors']:
                return 'healthcare', amenity
            elif amenity in ['school', 'university', 'kindergarten']:
                return 'education', amenity
            elif amenity in ['bank', 'atm']:
                return 'financial', amenity
            else:
                return 'amenity', amenity
        
        if 'office' in tags:
            return 'office', tags.get('office', 'company')
        
        return 'poi', 'unknown'
    
    def _get_poi_functional_group(self, primary_category: str, secondary_category: str) -> str:
        """
        Визначає функціональну групу POI - існуюча V2 логіка
        """
        # Конкуренти (роздрібна торгівля)
        if primary_category == 'retail':
            return 'competitor'
        
        # Генератори трафіку
        if primary_category in ['food_service', 'healthcare', 'education']:
            return 'traffic_generator'
        
        return 'competitor'  # Default для POI
    
    def _get_transport_subtype(self, tags: Dict[str, str]) -> str:
        """
        Визначає підтип транспортного вузла
        """
        if tags.get('highway') == 'bus_stop':
            return 'bus_stop'
        if tags.get('amenity') == 'bus_station':
            return 'bus_station'
        if tags.get('railway') == 'station':
            if tags.get('station') == 'subway':
                return 'metro_station'
            return 'train_station'
        if tags.get('railway') == 'halt':
            return 'train_halt'
        if tags.get('railway') in ['subway_entrance']:
            return 'metro_entrance'
        if tags.get('railway') == 'tram_stop':
            return 'tram_stop'
        if tags.get('amenity') == 'ferry_terminal':
            return 'ferry_terminal'
        if tags.get('amenity') == 'taxi':
            return 'taxi_stand'
        
        return 'transport_node'
    
    def _get_road_subtype(self, tags: Dict[str, str]) -> str:
        """
        Визначає підтип дороги зі стандартизацією
        """
        highway_type = tags.get('highway', '').lower()
        
        highway_mapping = {
            'motorway': 'motorway',
            'motorway_link': 'motorway',
            'trunk': 'trunk', 
            'trunk_link': 'trunk',
            'primary': 'primary',
            'primary_link': 'primary',
            'secondary': 'secondary',
            'secondary_link': 'secondary', 
            'tertiary': 'tertiary',
            'tertiary_link': 'tertiary',
            'residential': 'residential',
            'living_street': 'residential',
            'service': 'service',
            'unclassified': 'unclassified',
            'track': 'track'
        }
        
        return highway_mapping.get(highway_type, 'unclassified')
    
    def _parse_speed_limit(self, maxspeed_value: Optional[str]) -> Optional[int]:
        """
        Парсить обмеження швидкості
        """
        if not maxspeed_value:
            return None
        
        maxspeed_str = str(maxspeed_value).strip().lower()
        
        # Спеціальні значення
        if maxspeed_str in ['walk', 'walking']:
            return 5
        if maxspeed_str == 'none':
            return 130
        
        try:
            # Видаляємо одиниці виміру
            speed_str = maxspeed_str.replace('km/h', '').replace('kmh', '').strip()
            
            # Конвертуємо милі
            if 'mph' in maxspeed_str:
                speed_mph = float(speed_str.replace('mph', '').strip())
                return int(speed_mph * 1.60934)
            
            return int(float(speed_str))
            
        except (ValueError, TypeError):
            return None
    
    def _calculate_transport_accessibility(self, tags: Dict[str, str], transport_subtype: str) -> float:
        """
        Розрахунок accessibility score для transport node
        """
        base_scores = {
            'metro_station': 0.95,
            'train_station': 0.9,
            'bus_station': 0.8,
            'bus_stop': 0.6,
            'tram_stop': 0.5,
            'taxi_stand': 0.3,
            'transport_node': 0.4
        }
        
        base_score = base_scores.get(transport_subtype, 0.4)
        
        # Бонуси за додаткові атрибути
        if tags.get('shelter') == 'yes':
            base_score += 0.1
        if tags.get('bench') == 'yes':
            base_score += 0.05
        if 'network' in tags:
            base_score += 0.05
        
        return min(base_score, 1.0)
    
    def _calculate_road_accessibility(self, tags: Dict[str, str], highway_type: str) -> float:
        """
        Розрахунок accessibility score для road segment
        """
        base_scores = {
            'motorway': 0.95,
            'trunk': 0.9,
            'primary': 0.8,
            'secondary': 0.7,
            'tertiary': 0.6,
            'residential': 0.4,
            'service': 0.2,
            'unclassified': 0.3
        }
        
        base_score = base_scores.get(highway_type, 0.3)
        
        # Бонуси за якість дороги
        if tags.get('surface') == 'asphalt':
            base_score += 0.05
        elif tags.get('surface') in ['concrete', 'paved']:
            base_score += 0.03
        
        # Кількість смуг
        try:
            lanes = int(tags.get('lanes', '1'))
            if lanes >= 4:
                base_score += 0.1
            elif lanes >= 2:
                base_score += 0.05
        except ValueError:
            pass
        
        return min(base_score, 1.0)
    
    def _calculate_transport_quality(self, tags: Dict[str, str], name: str) -> float:
        """
        Розрахунок quality score для transport node
        """
        quality = 0.5  # Base quality
        
        # Назва підвищує якість
        if name and len(name) > 3:
            quality += 0.3
        
        # Додаткові атрибути
        quality_tags = ['shelter', 'bench', 'network', 'operator']
        for tag in quality_tags:
            if tag in tags:
                quality += 0.05
        
        return min(quality, 1.0)
    
    def _calculate_road_quality(self, tags: Dict[str, str]) -> float:
        """
        Розрахунок quality score для road segment
        """
        quality = 0.6  # Base quality
        
        # Атрибути якості
        if 'maxspeed' in tags:
            quality += 0.1
        if 'surface' in tags:
            quality += 0.1
        if 'lanes' in tags:
            quality += 0.1
        if 'ref' in tags:  # Номер дороги
            quality += 0.1
        
        return min(quality, 1.0)
    
    def save_entities(self, conn, entities: List[Dict]):
        """
        Збереження entities в poi_processed - використовуємо V2 логіку
        """
        if not entities:
            return
        
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
                highway_type, max_speed, accessibility_score,
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
                %(highway_type)s, %(max_speed)s, %(accessibility_score)s,
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
                logger.error(f"Помилка збереження entity {entity.get('entity_id', 'unknown')}: {e}")
                conn.rollback()
                cur = conn.cursor()
        
        conn.commit()
        cur.close()
        logger.info(f"✅ Збережено {saved_count}/{len(entities)} V3 entities")
    
    def print_statistics(self):
        """
        Виводить детальну статистику V3 обробки
        """
        logger.info("\n📊 Статистика V3 обробки:")
        logger.info(f"  Оброблено записів: {self.stats['processed']:,}")
        logger.info(f"  Знайдено POI: {self.stats['poi_found']:,}")
        logger.info(f"  Знайдено Transport: {self.stats['transport_found']:,}")
        logger.info(f"  Знайдено Roads: {self.stats['road_found']:,}")
        logger.info(f"  Розпізнано брендів: {self.stats['brands_matched']:,}")
        logger.info(f"  Пропущено: {self.stats['skipped']:,}")
        logger.info(f"  Помилок: {self.stats['errors']:,}")
        
        total_found = self.stats['poi_found'] + self.stats['transport_found'] + self.stats['road_found']
        if self.stats['processed'] > 0:
            success_rate = (total_found / self.stats['processed']) * 100
            logger.info(f"  Success rate: {success_rate:.1f}%")

def main():
    """Головна функція"""
    import argparse
    
    parser = argparse.ArgumentParser(description='V3 Universal Entity Processing')
    parser.add_argument('--limit', type=int, default=1000, help='Кількість записів')
    parser.add_argument('--region', type=str, help='Конкретний регіон')
    parser.add_argument('--batch-size', type=int, default=1000, help='Розмір batch')
    
    args = parser.parse_args()
    
    processor = EntityProcessorV3()
    processor.process_batch(
        limit=args.limit, 
        region=args.region,
        batch_size=args.batch_size
    )

if __name__ == "__main__":
    main()