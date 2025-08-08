#!/usr/bin/env python3
"""
OSM Tag Parser - Enhanced Version for V3
Парсинг складних JSON тегів з osm_raw таблиці + розширення для transport/road
"""

import json
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ParsedTags:
    """Результат парсингу OSM тегів"""
    tags: Dict[str, str]
    name: Optional[str] = None
    brand: Optional[str] = None
    shop_type: Optional[str] = None
    amenity_type: Optional[str] = None
    highway_type: Optional[str] = None
    raw_json: Optional[Dict] = None


class TagParser:
    """Парсер для складних OSM тегів - Enhanced Version V3"""
    
    def __init__(self):
        self.stats = {
            "total_parsed": 0,
            "parse_errors": 0,
            "empty_tags": 0,
            "complex_json_parsed": 0,
            "transport_entities": 0,
            "road_entities": 0
        }
    
    def parse_tags(self, tags_json: Any) -> ParsedTags:
        """
        Парсинг JSON тегів з різних форматів
        
        Args:
            tags_json: JSON об'єкт або рядок з тегами
            
        Returns:
            ParsedTags об'єкт з розпарсеними тегами
        """
        self.stats["total_parsed"] += 1
        
        if not tags_json:
            self.stats["empty_tags"] += 1
            return ParsedTags(tags={})
        
        try:
            # Обробка різних форматів тегів
            if isinstance(tags_json, dict):
                # Вже dict
                tags_dict = tags_json
            elif isinstance(tags_json, str):
                # JSON string
                tags_dict = json.loads(tags_json)
            else:
                # JSONB from PostgreSQL
                tags_dict = dict(tags_json)
            
            # Особливий випадок для вкладеного 'tags'
            if 'tags' in tags_dict and isinstance(tags_dict['tags'], str):
                try:
                    # Подвійний JSON encoding
                    inner_tags = json.loads(tags_dict['tags'])
                    tags_dict.update(inner_tags)
                except:
                    pass
            
            # Витягуємо ключові поля
            parsed = ParsedTags(
                tags=tags_dict,
                name=self._extract_name(tags_dict),
                brand=tags_dict.get('brand'),
                shop_type=tags_dict.get('shop'),
                amenity_type=tags_dict.get('amenity'),
                highway_type=tags_dict.get('highway'),
                raw_json=tags_dict
            )
            
            # Статистика для нових типів
            if self._is_transport_entity(tags_dict):
                self.stats["transport_entities"] += 1
            if self._is_road_entity(tags_dict):
                self.stats["road_entities"] += 1
            
            return parsed
            
        except Exception as e:
            self.stats["parse_errors"] += 1
            logger.warning(f"Tag parsing error: {e}")
            return ParsedTags(tags={})
    
    # ====================================================================
    # V3 РОЗШИРЕННЯ: Методи для роботи зі складними JSON структурами
    # ====================================================================
    
    def parse_complex_tags(self, tags_field: Any) -> Dict[str, str]:
        """
        Парсить складну JSON структуру з osm_raw.tags
        
        Структура: {"tags": "{\"key\":\"value\", ...}", "version": "1", "osm_type": "nodes"}
        Повертає: {"key": "value", ...}
        
        Args:
            tags_field: Поле tags з osm_raw (може бути str, dict, або None)
            
        Returns:
            Dict з розпарсованими внутрішніми тегами
        """
        if not tags_field:
            return {}
        
        try:
            self.stats["complex_json_parsed"] += 1
            
            # Якщо це строка - парсимо як JSON
            if isinstance(tags_field, str):
                outer_json = json.loads(tags_field)
            elif isinstance(tags_field, dict):
                outer_json = tags_field
            else:
                logger.warning(f"Unexpected tags_field type: {type(tags_field)}")
                return {}
            
            # Витягуємо внутрішній tags string
            inner_tags_string = outer_json.get('tags', '{}')
            
            if not inner_tags_string or inner_tags_string == '{}':
                return {}
            
            # Парсимо внутрішні теги
            inner_tags = json.loads(inner_tags_string)
            
            # Конвертуємо всі значення в строки та очищуємо
            cleaned_tags = {}
            for key, value in inner_tags.items():
                if key and value is not None:
                    cleaned_key = str(key).strip()
                    cleaned_value = str(value).strip()
                    if cleaned_key and cleaned_value:
                        cleaned_tags[cleaned_key] = cleaned_value
            
            return cleaned_tags
            
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error in complex tags: {e}, tags_field: {str(tags_field)[:100]}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error parsing complex tags: {e}, tags_field type: {type(tags_field)}")
            return {}
    
    def extract_osm_metadata(self, tags_field: Any) -> Dict[str, str]:
        """
        Витягує метадані OSM з зовнішнього JSON
        
        Returns:
            {"osm_type": "nodes", "version": "1", "changeset": "12345"}
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
            
            metadata = {}
            metadata_keys = ['osm_type', 'version', 'changeset']
            
            for key in metadata_keys:
                if key in outer_json:
                    metadata[key] = str(outer_json[key])
            
            return metadata
            
        except Exception as e:
            logger.warning(f"Error extracting OSM metadata: {e}")
            return {}
    
    def parse_speed_limit(self, maxspeed_value: Optional[str]) -> Optional[int]:
        """
        Парсить обмеження швидкості з OSM
        
        Args:
            maxspeed_value: "50", "90", "50 mph", "walk", "none"
            
        Returns:
            Швидкість в км/год або None
        """
        if not maxspeed_value:
            return None
        
        maxspeed_str = str(maxspeed_value).strip().lower()
        
        # Спеціальні значення
        special_speeds = {
            'walk': 5,
            'walking': 5,
            'none': 130,  # Без обмежень (автобан)
            'signals': 50,  # За сигналами світлофора
            'variable': 50   # Змінне обмеження
        }
        
        if maxspeed_str in special_speeds:
            return special_speeds[maxspeed_str]
        
        # Парсимо числове значення
        try:
            # Видаляємо одиниці виміру
            speed_str = maxspeed_str.replace('km/h', '').replace('kmh', '').replace('kph', '').strip()
            
            # Конвертуємо милі в км/год
            if 'mph' in maxspeed_str:
                speed_mph = float(speed_str.replace('mph', '').strip())
                return int(speed_mph * 1.60934)  # mph to km/h
            
            # Просто число - припускаємо км/год
            return int(float(speed_str))
            
        except (ValueError, TypeError):
            logger.warning(f"Cannot parse maxspeed: {maxspeed_value}")
            return None
    
    def get_transport_subtype(self, tags: Dict[str, str]) -> str:
        """
        Визначає підтип транспортного вузла
        
        Returns:
            'bus_stop' | 'bus_station' | 'train_station' | 'metro_station' | 'tram_stop' | 'ferry' | 'taxi' | 'transport_hub'
        """
        # Bus infrastructure
        if tags.get('highway') == 'bus_stop':
            return 'bus_stop'
        if tags.get('amenity') == 'bus_station':
            return 'bus_station'
        
        # Railway infrastructure  
        if tags.get('railway') == 'station':
            if tags.get('station') == 'subway':
                return 'metro_station'
            return 'train_station'
        if tags.get('railway') == 'halt':
            return 'train_halt'
        if tags.get('railway') == 'subway_entrance':
            return 'metro_entrance'
        if tags.get('railway') == 'tram_stop':
            return 'tram_stop'
        
        # Other transport
        if tags.get('amenity') == 'ferry_terminal':
            return 'ferry_terminal'
        if tags.get('amenity') == 'taxi':
            return 'taxi_stand'
        
        # Public transport generic
        if tags.get('public_transport') == 'platform':
            return 'platform'
        if tags.get('public_transport') == 'stop_position':
            return 'stop_position' 
        if tags.get('public_transport') == 'station':
            return 'transport_station'
        
        return 'transport_node'  # generic
    
    def get_road_subtype(self, tags: Dict[str, str]) -> str:
        """
        Визначає підтип дороги з стандартизацією
        
        Returns:
            Стандартизований highway тип
        """
        highway_type = tags.get('highway', '').lower()
        
        # Mapping для стандартизації
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
            'track': 'track',
            'path': 'path',
            'footway': 'footway',
            'cycleway': 'cycleway'
        }
        
        return highway_mapping.get(highway_type, highway_type)
    
    # ====================================================================
    # ІСНУЮЧІ МЕТОДИ (збережені з оригінального TagParser)
    # ====================================================================
    
    def _extract_name(self, tags: Dict[str, str]) -> Optional[str]:
        """Витягує назву з різних можливих полів"""
        # Пріоритет полів для назви
        name_fields = [
            'name', 'name:uk', 'name:en', 'name:ru',
            'official_name', 'alt_name', 'short_name'
        ]
        
        for field in name_fields:
            if field in tags and tags[field]:
                return tags[field]
        
        return None
    
    def extract_address(self, tags: Dict[str, str]) -> Dict[str, str]:
        """Витягує адресну інформацію"""
        address_keys = [
            'addr:housenumber', 'addr:street', 'addr:city',
            'addr:postcode', 'addr:district', 'addr:region'
        ]
        
        address = {}
        for key in address_keys:
            if key in tags:
                clean_key = key.replace('addr:', '')
                address[clean_key] = tags[key]
        
        return address
    
    def get_category_from_tags(self, tags: Dict[str, str]) -> tuple[str, str]:
        """
        Визначає primary та secondary категорії з тегів
        
        Returns:
            (primary_category, secondary_category)
        """
        # Shop tags
        if 'shop' in tags:
            return 'retail', tags['shop']
        
        # Amenity tags
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
            elif amenity in ['fuel', 'charging_station']:
                return 'automotive', amenity
            else:
                return 'amenity', amenity
        
        # Transport tags  
        if 'public_transport' in tags:
            return 'transport', tags.get('public_transport', 'stop')
        
        if 'railway' in tags:
            return 'transport', tags['railway']
        
        # Highway tags
        if 'highway' in tags:
            return 'road', tags['highway']
        
        # Building tags
        if 'building' in tags:
            return 'building', tags.get('building', 'yes')
        
        # Landuse tags
        if 'landuse' in tags:
            return 'landuse', tags['landuse']
        
        # Default
        return 'other', 'unclassified'
    
    def get_stats(self) -> Dict[str, int]:
        """Повертає статистику парсингу"""
        return self.stats.copy()
    
    # ====================================================================
    # ДОПОМІЖНІ МЕТОДИ ДЛЯ СТАТИСТИКИ
    # ====================================================================
    
    def _is_transport_entity(self, tags: Dict[str, str]) -> bool:
        """Перевіряє чи це transport entity"""
        transport_indicators = [
            'public_transport', 'railway', 'highway'  
        ]
        
        for indicator in transport_indicators:
            if indicator in tags:
                value = tags[indicator]
                if indicator == 'highway' and value == 'bus_stop':
                    return True
                elif indicator == 'public_transport' and value in ['platform', 'stop_position', 'station']:
                    return True
                elif indicator == 'railway' and value in ['station', 'halt', 'subway_entrance']:
                    return True
        
        return False
    
    def _is_road_entity(self, tags: Dict[str, str]) -> bool:
        """Перевіряє чи це road entity"""
        highway_value = tags.get('highway')
        if not highway_value:
            return False
        
        road_highway_types = {
            'motorway', 'trunk', 'primary', 'secondary', 'tertiary',
            'residential', 'service', 'unclassified', 'track'
        }
        
        return highway_value in road_highway_types


# ====================================================================
# ТЕСТУВАННЯ РОЗШИРЕНОГО TAG PARSER
# ====================================================================

def main():
    """Розширене тестування Enhanced Tag Parser"""
    parser = TagParser()
    
    print("🧪 Тестування Enhanced Tag Parser V3:")
    print("=" * 60)
    
    # === ТЕСТ 1: Оригінальні тестові кейси ===
    print("\n📋 ТЕСТ 1: Існуючі функції")
    
    original_test_cases = [
        # Простий dict
        {"shop": "supermarket", "name": "АТБ", "brand": "АТБ"},
        
        # JSON string
        '{"shop": "convenience", "name": "Сільпо", "addr:city": "Київ"}',
        
        # Вкладений tags (як в БД)
        {"tags": '{"amenity": "restaurant", "name": "Pizza Day", "cuisine": "pizza"}'},
        
        # Транспорт
        {"public_transport": "platform", "name": "Театральна", "railway": "station"}
    ]
    
    for i, test_data in enumerate(original_test_cases, 1):
        print(f"\n  {i}. {test_data}")
        result = parser.parse_tags(test_data)
        print(f"     Назва: {result.name}")
        print(f"     Бренд: {result.brand}")
        print(f"     Highway: {result.highway_type}")
        
        if result.tags:
            categories = parser.get_category_from_tags(result.tags)
            print(f"     Категорії: {categories}")
    
    # === ТЕСТ 2: Нові V3 функції ===
    print(f"\n📋 ТЕСТ 2: V3 розширення - Complex JSON parsing")
    
    v3_test_cases = [
        # Transport node з реальною структурою  
        '{"tags": "{\\"bus\\": \\"yes\\", \\"name\\": \\"Салівонки\\", \\"highway\\": \\"bus_stop\\", \\"public_transport\\": \\"platform\\"}", "version": "1", "osm_type": "nodes"}',
        
        # Road segment з реальною структурою
        '{"tags": "{\\"ref\\": \\"H-02\\", \\"highway\\": \\"primary\\", \\"maxspeed\\": \\"90\\", \\"surface\\": \\"asphalt\\", \\"lanes\\": \\"2\\"}", "version": "1", "osm_type": "ways"}',
        
        # POI з реальною структурою
        '{"tags": "{\\"shop\\": \\"supermarket\\", \\"brand\\": \\"АТБ\\", \\"name\\": \\"АТБ\\", \\"addr:city\\": \\"Київ\\"}", "version": "2", "osm_type": "nodes"}'
    ]
    
    for i, tags_json in enumerate(v3_test_cases, 1):
        print(f"\n  {i}. Complex JSON Test:")
        print(f"     Input: {tags_json[:60]}...")
        
        # Парсимо complex tags
        parsed_tags = parser.parse_complex_tags(tags_json)
        metadata = parser.extract_osm_metadata(tags_json)
        
        print(f"     Parsed tags: {parsed_tags}")
        print(f"     Metadata: {metadata}")
        
        # Додаткова обробка для V3 типів
        if 'highway' in parsed_tags:
            if parsed_tags['highway'] == 'bus_stop':
                transport_subtype = parser.get_transport_subtype(parsed_tags)
                print(f"     🚌 Transport subtype: {transport_subtype}")
            elif parsed_tags['highway'] in ['primary', 'secondary', 'residential']:
                road_subtype = parser.get_road_subtype(parsed_tags)
                speed_limit = parser.parse_speed_limit(parsed_tags.get('maxspeed'))
                print(f"     🛣️  Road subtype: {road_subtype}")
                print(f"     🚗 Speed limit: {speed_limit} km/h")
    
    # === ТЕСТ 3: Speed limit parsing ===
    print(f"\n📋 ТЕСТ 3: Speed limit parsing")
    
    speed_test_cases = ['50', '90', '120', '50 mph', 'walk', 'none', 'signals', '60 km/h', None]
    
    for speed_input in speed_test_cases:
        result = parser.parse_speed_limit(speed_input)
        print(f"     '{speed_input}' → {result} km/h")
    
    # === ФІНАЛЬНА СТАТИСТИКА ===
    print(f"\n📊 Фінальна статистика Enhanced TagParser:")
    stats = parser.get_stats()
    for key, value in stats.items():
        print(f"     {key}: {value}")


if __name__ == "__main__":
    main()