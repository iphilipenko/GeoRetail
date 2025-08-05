"""
Brand Matcher
Інтелектуальний матчинг брендів з використанням multiple algorithms
"""

import re
import logging
from typing import Dict, Optional, Tuple, List, Any
from dataclasses import dataclass
from difflib import SequenceMatcher
from collections import defaultdict

# Optional: fuzzy matching library
try:
    from fuzzywuzzy import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    logging.warning("fuzzywuzzy not installed, fuzzy matching will be limited")

from .brand_dictionary import BrandDictionary, BrandInfo

logger = logging.getLogger(__name__)


@dataclass
class MatchResult:
    """Результат матчингу бренду"""
    brand_id: str
    canonical_name: str
    confidence: float
    match_type: str  # exact, fuzzy, osm_tag, keyword, generic
    functional_group: str
    influence_weight: float
    debug_info: Optional[Dict[str, Any]] = None


class BrandMatcher:
    """Матчинг брендів з використанням різних алгоритмів"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.brand_dict = BrandDictionary()
        self.config = config or self._default_config()
        
        # Кеш результатів
        self.cache = {} if self.config['cache']['enabled'] else None
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Статистика
        self.stats = defaultdict(int)
        
        # Побудова додаткових індексів
        self._build_indexes()
    
    def _default_config(self) -> Dict[str, Any]:
        """Конфігурація за замовчуванням"""
        return {
            'algorithms': {
                'exact': {'enabled': True, 'priority': 1},
                'fuzzy': {
                    'enabled': True, 
                    'priority': 2,
                    'threshold': 0.9,
                    'algorithm': 'token_sort_ratio'
                },
                'osm_tags': {'enabled': True, 'priority': 3},
                'keywords': {
                    'enabled': False,
                    'priority': 4,
                    'min_confidence': 0.8
                }
            },
            'cache': {
                'enabled': True,
                'max_size': 10000
            },
            'quality': {
                'min_confidence': 0.8,
                'auto_approve_threshold': 0.9
            }
        }
    
    def _build_indexes(self):
        """Будує додаткові індекси для швидкого пошуку"""
        # Індекс ключових слів
        self.keyword_index = defaultdict(list)
        
        # Індекс OSM тегів
        self.osm_tag_index = defaultdict(list)
        
        for brand_id, brand_info in self.brand_dict.brands.items():
            # Ключові слова з назви
            keywords = self._extract_keywords(brand_info.canonical_name)
            for keyword in keywords:
                self.keyword_index[keyword].append(brand_id)
            
            # OSM теги
            if brand_info.osm_tags:
                for tag in brand_info.osm_tags:
                    self.osm_tag_index[tag].append(brand_id)
    
    def match_brand(
        self, 
        name: Optional[str], 
        osm_tags: Optional[Dict[str, str]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> Optional[MatchResult]:
        """
        Основний метод матчингу бренду
        
        Args:
            name: Назва для пошуку
            osm_tags: OSM теги об'єкта
            context: Додатковий контекст (категорія, локація тощо)
            
        Returns:
            MatchResult або None якщо не знайдено
        """
        self.stats['total_requests'] += 1
        
        # Перевірка кешу
        cache_key = self._get_cache_key(name, osm_tags)
        if self.cache is not None and cache_key in self.cache:
            self.cache_hits += 1
            self.stats['cache_hits'] += 1
            return self.cache[cache_key]
        
        self.cache_misses += 1
        
        # Спробувати різні алгоритми за пріоритетом
        result = None
        
        if name and self.config['algorithms']['exact']['enabled']:
            result = self._exact_match(name)
            
        if not result and name and self.config['algorithms']['fuzzy']['enabled']:
            result = self._fuzzy_match(name)
            
        if not result and osm_tags and self.config['algorithms']['osm_tags']['enabled']:
            result = self._osm_tag_match(osm_tags, name)
            
        if not result and name and self.config['algorithms']['keywords']['enabled']:
            result = self._keyword_match(name, context)
        
        # Перевірка мінімальної довіри
        if result and result.confidence < self.config['quality']['min_confidence']:
            logger.debug(f"Match rejected due to low confidence: {result.confidence}")
            result = None
        
        # Зберігання в кеш
        if self.cache is not None and len(self.cache) < self.config['cache']['max_size']:
            self.cache[cache_key] = result
        
        # Оновлення статистики
        if result:
            self.stats['successful_matches'] += 1
            self.stats[f'match_type_{result.match_type}'] += 1
        else:
            self.stats['failed_matches'] += 1
        
        return result
    
    def _exact_match(self, name: str) -> Optional[MatchResult]:
        """Точний збіг з синонімами"""
        result = self.brand_dict.find_brand_by_name(name)
        
        if result:
            brand_id, brand_info = result
            return MatchResult(
                brand_id=brand_id,
                canonical_name=brand_info.canonical_name,
                confidence=1.0,
                match_type='exact',
                functional_group=brand_info.functional_group,
                influence_weight=brand_info.influence_weight
            )
        
        return None
    
    def _fuzzy_match(self, name: str) -> Optional[MatchResult]:
        """Нечіткий пошук з використанням fuzzy matching"""
        if not FUZZY_AVAILABLE:
            return self._simple_fuzzy_match(name)
        
        threshold = self.config['algorithms']['fuzzy']['threshold']
        algorithm = self.config['algorithms']['fuzzy']['algorithm']
        
        best_match = None
        best_score = 0
        best_brand_id = None
        
        # Перебираємо всі бренди
        for brand_id, brand_info in self.brand_dict.brands.items():
            # Перевіряємо канонічну назву та синоніми
            all_names = [brand_info.canonical_name] + brand_info.synonyms
            
            for brand_name in all_names:
                # Різні алгоритми fuzzy matching
                if algorithm == 'token_sort_ratio':
                    score = fuzz.token_sort_ratio(name, brand_name) / 100.0
                elif algorithm == 'token_set_ratio':
                    score = fuzz.token_set_ratio(name, brand_name) / 100.0
                elif algorithm == 'partial_ratio':
                    score = fuzz.partial_ratio(name, brand_name) / 100.0
                else:
                    score = fuzz.ratio(name, brand_name) / 100.0
                
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = brand_info
                    best_brand_id = brand_id
        
        if best_match:
            return MatchResult(
                brand_id=best_brand_id,
                canonical_name=best_match.canonical_name,
                confidence=best_score,
                match_type='fuzzy',
                functional_group=best_match.functional_group,
                influence_weight=best_match.influence_weight,
                debug_info={'algorithm': algorithm, 'score': best_score}
            )
        
        return None
    
    def _simple_fuzzy_match(self, name: str) -> Optional[MatchResult]:
        """Простий fuzzy matching без зовнішніх бібліотек"""
        threshold = self.config['algorithms']['fuzzy']['threshold']
        
        best_match = None
        best_score = 0
        best_brand_id = None
        
        normalized_name = self._normalize_for_fuzzy(name)
        
        for brand_id, brand_info in self.brand_dict.brands.items():
            all_names = [brand_info.canonical_name] + brand_info.synonyms
            
            for brand_name in all_names:
                normalized_brand = self._normalize_for_fuzzy(brand_name)
                
                # Використовуємо SequenceMatcher
                score = SequenceMatcher(None, normalized_name, normalized_brand).ratio()
                
                # Додаткові бали за спільні слова
                name_words = set(normalized_name.split())
                brand_words = set(normalized_brand.split())
                common_words = name_words.intersection(brand_words)
                
                if common_words:
                    word_bonus = len(common_words) / max(len(name_words), len(brand_words))
                    score = score * 0.7 + word_bonus * 0.3
                
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = brand_info
                    best_brand_id = brand_id
        
        if best_match:
            return MatchResult(
                brand_id=best_brand_id,
                canonical_name=best_match.canonical_name,
                confidence=best_score,
                match_type='fuzzy',
                functional_group=best_match.functional_group,
                influence_weight=best_match.influence_weight
            )
        
        return None
    
    def _osm_tag_match(self, osm_tags: Dict[str, str], name: Optional[str] = None) -> Optional[MatchResult]:
        """Матчинг на основі OSM тегів"""
        candidates = defaultdict(float)
        
        # Перевірка brand тегу
        if 'brand' in osm_tags:
            brand_result = self.brand_dict.find_brand_by_name(osm_tags['brand'])
            if brand_result:
                brand_id, brand_info = brand_result
                candidates[brand_id] += 0.8
        
        # Перевірка brand:wikidata
        if 'brand:wikidata' in osm_tags:
            # TODO: Можна додати mapping wikidata -> brand_id
            pass
        
        # Перевірка shop/amenity типів
        shop_type = osm_tags.get('shop')
        amenity_type = osm_tags.get('amenity')
        
        # Пошук за OSM тегами в індексі
        for tag_key, tag_value in osm_tags.items():
            tag_pattern = f"{tag_key}={tag_value}"
            if tag_pattern in self.osm_tag_index:
                for brand_id in self.osm_tag_index[tag_pattern]:
                    candidates[brand_id] += 0.5
        
        # Якщо є назва, додаємо додаткову перевірку
        if name and candidates:
            for brand_id in list(candidates.keys()):
                brand_info = self.brand_dict.get_brand_by_id(brand_id)
                if brand_info:
                    # Перевірка схожості назви
                    name_similarity = self._calculate_name_similarity(name, brand_info.canonical_name)
                    candidates[brand_id] += name_similarity * 0.3
        
        # Вибираємо найкращого кандидата
        if candidates:
            best_brand_id = max(candidates, key=candidates.get)
            confidence = min(candidates[best_brand_id], 1.0)
            
            if confidence >= 0.5:  # Мінімальний поріг для OSM tag match
                brand_info = self.brand_dict.get_brand_by_id(best_brand_id)
                return MatchResult(
                    brand_id=best_brand_id,
                    canonical_name=brand_info.canonical_name,
                    confidence=confidence,
                    match_type='osm_tag',
                    functional_group=brand_info.functional_group,
                    influence_weight=brand_info.influence_weight,
                    debug_info={'osm_tags': osm_tags}
                )
        
        return None
    
    def _keyword_match(self, name: str, context: Optional[Dict[str, Any]] = None) -> Optional[MatchResult]:
        """Матчинг на основі ключових слів"""
        keywords = self._extract_keywords(name)
        if not keywords:
            return None
        
        candidates = defaultdict(float)
        
        # Пошук за ключовими словами
        for keyword in keywords:
            if keyword in self.keyword_index:
                for brand_id in self.keyword_index[keyword]:
                    candidates[brand_id] += 1.0 / len(keywords)
        
        # Враховуємо контекст (якщо є)
        if context and 'category' in context:
            category = context['category']
            # Додаємо бонус брендам з відповідною категорією
            for brand_id in candidates:
                brand_info = self.brand_dict.get_brand_by_id(brand_id)
                if brand_info and self._category_matches(category, brand_info.format):
                    candidates[brand_id] *= 1.2
        
        # Вибираємо найкращого кандидата
        if candidates:
            best_brand_id = max(candidates, key=candidates.get)
            confidence = candidates[best_brand_id] * self.config['algorithms']['keywords']['min_confidence']
            
            if confidence >= self.config['algorithms']['keywords']['min_confidence']:
                brand_info = self.brand_dict.get_brand_by_id(best_brand_id)
                return MatchResult(
                    brand_id=best_brand_id,
                    canonical_name=brand_info.canonical_name,
                    confidence=min(confidence, 0.8),  # Обмежуємо максимальну довіру
                    match_type='keyword',
                    functional_group=brand_info.functional_group,
                    influence_weight=brand_info.influence_weight,
                    debug_info={'keywords': keywords}
                )
        
        return None
    
    def _normalize_for_fuzzy(self, text: str) -> str:
        """Нормалізація тексту для fuzzy matching"""
        if not text:
            return ""
        
        # Нижній регістр
        text = text.lower()
        
        # Видаляємо спеціальні символи
        text = re.sub(r'[^\w\s-]', ' ', text)
        
        # Замінюємо множинні пробіли
        text = ' '.join(text.split())
        
        return text.strip()
    
    def _extract_keywords(self, text: str) -> List[str]:
        """Витягує ключові слова з тексту"""
        if not text:
            return []
        
        # Нормалізація
        text = self._normalize_for_fuzzy(text)
        
        # Розбиваємо на слова
        words = text.split()
        
        # Фільтруємо короткі слова та стоп-слова
        stop_words = {'та', 'і', 'або', 'the', 'and', 'or', 'of', 'магазин', 'маркет'}
        keywords = [w for w in words if len(w) > 2 and w not in stop_words]
        
        return keywords
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Розраховує схожість двох назв"""
        norm1 = self._normalize_for_fuzzy(name1)
        norm2 = self._normalize_for_fuzzy(name2)
        
        return SequenceMatcher(None, norm1, norm2).ratio()
    
    def _category_matches(self, category: str, brand_format: str) -> bool:
        """Перевіряє відповідність категорії формату бренду"""
        category_mappings = {
            'supermarket': ['супермаркет', 'гіпермаркет', 'дискаунтер'],
            'convenience': ['магазин біля дому', 'міні-маркет'],
            'electronics': ['магазин електроніки', 'побутова техніка'],
            'clothing': ['магазин одягу', 'fashion'],
            'pharmacy': ['аптека', 'дрогері'],
            'bank': ['банк', 'фінансова установа'],
            'restaurant': ['ресторан', 'кав\'ярня', 'фастфуд', 'піцерія']
        }
        
        for cat_key, formats in category_mappings.items():
            if category == cat_key and brand_format in formats:
                return True
        
        return False
    
    def _get_cache_key(self, name: Optional[str], osm_tags: Optional[Dict[str, str]]) -> str:
        """Генерує ключ для кешу"""
        parts = []
        
        if name:
            parts.append(f"name:{self._normalize_for_fuzzy(name)}")
        
        if osm_tags:
            # Сортуємо теги для консистентності
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(osm_tags.items()) if k in ['shop', 'amenity', 'brand'])
            if tag_str:
                parts.append(f"tags:{tag_str}")
        
        return '|'.join(parts)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Повертає статистику роботи matcher"""
        total_matches = self.stats['successful_matches'] + self.stats['failed_matches']
        success_rate = self.stats['successful_matches'] / total_matches if total_matches > 0 else 0
        
        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / cache_total if cache_total > 0 else 0
        
        return {
            'total_requests': self.stats['total_requests'],
            'successful_matches': self.stats['successful_matches'],
            'failed_matches': self.stats['failed_matches'],
            'success_rate': success_rate,
            'match_types': {
                'exact': self.stats.get('match_type_exact', 0),
                'fuzzy': self.stats.get('match_type_fuzzy', 0),
                'osm_tag': self.stats.get('match_type_osm_tag', 0),
                'keyword': self.stats.get('match_type_keyword', 0)
            },
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'cache_hit_rate': cache_hit_rate,
            'cache_size': len(self.cache) if self.cache else 0
        }
    
    def clear_cache(self):
        """Очищає кеш"""
        if self.cache is not None:
            self.cache.clear()
        self.cache_hits = 0
        self.cache_misses = 0


# Тестування
if __name__ == "__main__":
    # Створюємо matcher
    matcher = BrandMatcher()
    
    # Тестові випадки
    test_cases = [
        # (name, osm_tags, expected_brand)
        ("АТБ-маркет", None, "АТБ-Маркет"),
        ("силпо", None, "Сільпо"),
        ("ЕПІЦЕНТР", None, "Епіцентр К"),
        ("Аптека EVA", None, "EVA"),
        ("Нова пошта №123", None, "Нова Пошта"),
        ("McDonald's", {"amenity": "fast_food", "brand": "McDonald's"}, "McDonald's"),
        ("Супермаркет Novus", {"shop": "supermarket"}, "Novus"),
        ("Неизвестный магазин", None, None)
    ]
    
    print("🧪 Тестування Brand Matcher:\n")
    
    for name, tags, expected in test_cases:
        result = matcher.match_brand(name, tags)
        
        if result:
            print(f"✅ '{name}' → {result.canonical_name}")
            print(f"   Довіра: {result.confidence:.2f}, Тип: {result.match_type}")
            print(f"   Вплив: {result.influence_weight}, Група: {result.functional_group}")
        else:
            print(f"❌ '{name}' → Не знайдено")
        print()
    
    # Виводимо статистику
    stats = matcher.get_statistics()
    print("\n📊 Статистика:")
    print(f"  Всього запитів: {stats['total_requests']}")
    print(f"  Успішних: {stats['successful_matches']} ({stats['success_rate']*100:.1f}%)")
    print(f"  Типи матчингу: {stats['match_types']}")
    print(f"  Cache hit rate: {stats['cache_hit_rate']*100:.1f}%")