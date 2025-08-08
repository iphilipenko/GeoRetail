#!/usr/bin/env python3
"""
Entity Classifier для process_entities_v3.py
Визначає тип сутності з OSM тегів: poi | transport_node | road_segment
"""

import logging
from typing import Dict, Optional, Set

logger = logging.getLogger(__name__)

class EntityClassifier:
    """
    Класифікатор типів сутностей з OSM даних
    """
    
    def __init__(self):
        """Ініціалізація з конфігурацією типів"""
        
        # Transport node типи
        self.TRANSPORT_HIGHWAY_TYPES = {
            'bus_stop'
        }
        
        self.TRANSPORT_PUBLIC_TRANSPORT_TYPES = {
            'platform',
            'stop_position',
            'station'
        }
        
        self.TRANSPORT_RAILWAY_TYPES = {
            'station',
            'halt',
            'subway_entrance',
            'tram_stop'
        }
        
        self.TRANSPORT_AMENITY_TYPES = {
            'bus_station',
            'ferry_terminal',
            'taxi'
        }
        
        # Road segment типи
        self.ROAD_HIGHWAY_TYPES = {
            'motorway',
            'trunk', 
            'primary',
            'secondary',
            'tertiary',
            'residential',
            'service',
            'unclassified',
            'track'
        }
        
        # POI типи (з існуючого V2)
        self.POI_SHOP_REQUIRED = True  # shop=* завжди POI
        self.POI_AMENITY_TYPES = {
            'restaurant', 'cafe', 'fast_food', 'bar', 'pub',
            'pharmacy', 'hospital', 'clinic', 'doctors',
            'school', 'university', 'kindergarten',
            'bank', 'atm',
            'fuel', 'charging_station',
            'post_office', 'post_box'
        }
        
        logger.info("🏷️ EntityClassifier ініціалізовано")
    
    def classify_entity_type(self, osm_tags: Dict[str, str]) -> Optional[str]:
        """
        Основний метод класифікації entity type
        
        Args:
            osm_tags: Словник OSM тегів {"highway": "bus_stop", "name": "Зупинка"}
            
        Returns:
            'poi' | 'transport_node' | 'road_segment' | None
        """
        if not osm_tags or not isinstance(osm_tags, dict):
            return None
        
        # 1. Перевіряємо Transport Nodes (найвища пріоритетність)
        if self._is_transport_node(osm_tags):
            return 'transport_node'
        
        # 2. Перевіряємо Road Segments  
        if self._is_road_segment(osm_tags):
            return 'road_segment'
        
        # 3. Перевіряємо POI (найнижча пріоритетність)
        if self._is_poi(osm_tags):
            return 'poi'
        
        # 4. Не класифікується
        return None
    
    def _is_transport_node(self, tags: Dict[str, str]) -> bool:
        """
        Перевіряє чи це transport node
        
        Transport node criteria:
        - highway=bus_stop
        - public_transport=platform|stop_position|station  
        - railway=station|halt|subway_entrance|tram_stop
        - amenity=bus_station|ferry_terminal|taxi
        """
        
        # Highway-based transport
        if tags.get('highway') in self.TRANSPORT_HIGHWAY_TYPES:
            return True
        
        # Public transport
        if tags.get('public_transport') in self.TRANSPORT_PUBLIC_TRANSPORT_TYPES:
            return True
            
        # Railway transport
        if tags.get('railway') in self.TRANSPORT_RAILWAY_TYPES:
            return True
            
        # Amenity-based transport  
        if tags.get('amenity') in self.TRANSPORT_AMENITY_TYPES:
            return True
        
        return False
    
    def _is_road_segment(self, tags: Dict[str, str]) -> bool:
        """
        Перевіряє чи це road segment
        
        Road segment criteria:
        - highway=motorway|trunk|primary|secondary|tertiary|residential|service|unclassified|track
        - НЕ transport node (highway=bus_stop виключений)
        """
        highway_type = tags.get('highway')
        
        if not highway_type:
            return False
        
        # Перевіряємо чи це road highway (не transport)
        if highway_type in self.ROAD_HIGHWAY_TYPES:
            return True
        
        return False
    
    def _is_poi(self, tags: Dict[str, str]) -> bool:
        """
        Перевіряє чи це POI (Point of Interest)
        Використовуємо існуючу V2 логіку
        
        POI criteria:
        - shop=* (будь-який shop)
        - amenity=restaurant|cafe|pharmacy|bank|hospital|school|etc
        - office=*
        - tourism=*
        - leisure=*
        """
        
        # Усі shop теги є POI
        if 'shop' in tags and tags['shop']:
            return True
        
        # Специфічні amenity типи є POI
        if tags.get('amenity') in self.POI_AMENITY_TYPES:
            return True
        
        # Office, tourism, leisure також POI
        if any(key in tags for key in ['office', 'tourism', 'leisure']):
            if tags.get('office') or tags.get('tourism') or tags.get('leisure'):
                return True
        
        return False
    
    def get_classification_stats(self, osm_tags: Dict[str, str]) -> Dict[str, any]:
        """
        Повертає детальну статистику класифікації для debug
        """
        result = {
            'entity_type': self.classify_entity_type(osm_tags),
            'is_transport_node': self._is_transport_node(osm_tags),
            'is_road_segment': self._is_road_segment(osm_tags), 
            'is_poi': self._is_poi(osm_tags),
            'relevant_tags': {}
        }
        
        # Додаємо релевантні теги
        relevant_keys = ['highway', 'public_transport', 'railway', 'amenity', 'shop', 'office', 'tourism', 'leisure']
        for key in relevant_keys:
            if key in osm_tags:
                result['relevant_tags'][key] = osm_tags[key]
        
        return result

def main():
    """Тестування Entity Classifier"""
    classifier = EntityClassifier()
    
    # Тестові кейси
    test_cases = [
        # Transport nodes
        {"highway": "bus_stop", "name": "Зупинка автобуса"},
        {"public_transport": "platform", "bus": "yes", "name": "Салівонки"},  
        {"railway": "station", "name": "Київ-Пасажирський"},
        {"amenity": "bus_station", "name": "Автовокзал"},
        
        # Road segments  
        {"highway": "primary", "ref": "H-02", "maxspeed": "90"},
        {"highway": "residential", "name": "вулиця Шевченка"},
        {"highway": "service"},
        
        # POI
        {"shop": "supermarket", "brand": "АТБ"},
        {"amenity": "restaurant", "name": "Ресторан"},
        {"amenity": "pharmacy", "brand": "Аптека низьких цін"},
        {"office": "company", "name": "ТОВ Компанія"},
        
        # Edge cases
        {"natural": "tree"},  # Should return None
        {"power": "pole"},    # Should return None
        {}                   # Empty tags
    ]
    
    print("🧪 Тестування Entity Classifier:")
    print("=" * 50)
    
    for i, tags in enumerate(test_cases, 1):
        result = classifier.get_classification_stats(tags)
        print(f"\n{i:2d}. Tags: {tags}")
        print(f"    Entity Type: {result['entity_type']}")
        print(f"    Relevant Tags: {result['relevant_tags']}")
        
        if result['entity_type']:
            print(f"    ✅ Classified as: {result['entity_type']}")
        else:
            print(f"    ❌ Not classified")

if __name__ == "__main__":
    main()