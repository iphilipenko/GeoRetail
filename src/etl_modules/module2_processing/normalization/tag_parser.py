"""
OSM Tag Parser
Парсинг складних JSON тегів з osm_raw таблиці
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
    """Парсер для складних OSM тегів"""
    
    def __init__(self):
        self.stats = {
            "total_parsed": 0,
            "parse_errors": 0,
            "empty_tags": 0
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
            
            return parsed
            
        except Exception as e:
            self.stats["parse_errors"] += 1
            logger.warning(f"Tag parsing error: {e}")
            return ParsedTags(tags={})
    
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


# Тестування
if __name__ == "__main__":
    parser = TagParser()
    
    # Тестові дані різних форматів
    test_cases = [
        # Простий dict
        {"shop": "supermarket", "name": "АТБ", "brand": "АТБ"},
        
        # JSON string
        '{"shop": "convenience", "name": "Сільпо", "addr:city": "Київ"}',
        
        # Вкладений tags (як в БД)
        {"tags": '{"amenity": "restaurant", "name": "Pizza Day", "cuisine": "pizza"}'},
        
        # Порожні дані
        None,
        {},
        
        # Транспорт
        {"public_transport": "platform", "name": "Театральна", "railway": "station"}
    ]
    
    print("🧪 Тестування Tag Parser:\n")
    
    for i, test_data in enumerate(test_cases, 1):
        print(f"Тест {i}: {test_data}")
        result = parser.parse_tags(test_data)
        print(f"  Назва: {result.name}")
        print(f"  Бренд: {result.brand}")
        print(f"  Тип shop: {result.shop_type}")
        print(f"  Тип amenity: {result.amenity_type}")
        
        if result.tags:
            categories = parser.get_category_from_tags(result.tags)
            print(f"  Категорії: {categories}")
        
        if result.tags and any(k.startswith('addr:') for k in result.tags):
            address = parser.extract_address(result.tags)
            print(f"  Адреса: {address}")
        
        print()
    
    print(f"📊 Статистика: {parser.get_stats()}")