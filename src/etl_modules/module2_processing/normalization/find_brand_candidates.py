#!/usr/bin/env python3
"""
Find Brand Candidates Script
Аналізує poi_processed для знаходження POI без розпізнаних брендів
та створює кандидатів для подальшого review та затвердження
"""

import sys
import logging
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from psycopg2.extras import RealDictCursor
import json

# Додаємо поточну директорію до path для імпортів
sys.path.insert(0, str(Path(__file__).parent.parent))

# Імпорти наших модулів
from normalization.brand_manager import BrandManager
from normalization.brand_matcher import BrandMatcher

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфігурація (hardcoded для швидкості)
CONFIG = {
    'analysis': {
        'min_frequency': 3,              # Мінімальна частота для розгляду
        'min_quality_score': 0.0,       # Мінімальна якість POI
        'min_confidence': 0.3,           # Мінімальна впевненість для збереження  
        'min_regions_for_network': 2,   # Мінімум регіонів для мережі
        'min_name_length': 3,            # Мінімальна довжина назви
        'max_name_length': 50,           # Максимальна довжина назви
    },
    'performance': {
        'batch_size': 1000,              # Розмір batch для обробки
        'regions_parallel': False,       # Поки без паралелізму
    }
}

# Generic names patterns - базовий фільтр
GENERIC_PATTERNS = [
    'магазин', 'магазін', 'аптека', 'кафе', 'ресторан', 'банк',
    'shop', 'store', 'cafe', 'restaurant', 'pharmacy', 'market',
    'супермаркет', 'мінімаркет', 'гастроном', 'продукти'
]

# Database connection
DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"


class BrandCandidateFinder:
    """Основний клас для аналізу кандидатів брендів"""
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.brand_manager = BrandManager(db_connection_string)
        self.brand_matcher = BrandMatcher()
        
        # Статистика
        self.stats = {
            'regions_processed': 0,
            'poi_analyzed': 0,
            'unique_names_found': 0,
            'generic_filtered': 0,
            'existing_brand_matches': 0,
            'quality_candidates': 0,
            'network_candidates': 0,
            'saved_candidates': 0,
            'errors': 0
        }
        
        # Результати аналізу
        self.regional_results = {}  # {region_name: [candidates]}
        self.aggregated_candidates = {}  # {name: aggregated_data}
        
        logger.info("✅ BrandCandidateFinder ініціалізовано")
    
    def run_analysis(self) -> Dict[str, Any]:
        """Головний метод виконання аналізу"""
        logger.info("🔍 Починаємо аналіз кандидатів брендів...")
        
        start_time = datetime.now()
        
        try:
            # 1. Валідація
            self._validate_prerequisites()
            
            # 2. Отримання списку регіонів
            regions = self._get_available_regions()
            logger.info(f"📍 Знайдено {len(regions)} регіонів для аналізу")
            
            # 3. Аналіз по регіонах
            for region in regions:
                try:
                    self._analyze_single_region(region)
                    self.stats['regions_processed'] += 1
                except Exception as e:
                    logger.error(f"❌ Помилка обробки регіону {region}: {e}")
                    self.stats['errors'] += 1
                    continue
            
            # 4. Агрегація даних по регіонах
            self._aggregate_regional_data()
            
            # 5. Розрахунок мережевого потенціалу
            self._calculate_network_potential()
            
            # 6. Застосування фінальних фільтрів
            final_candidates = self._apply_quality_filters()
            
            # 7. Збереження кандидатів
            saved_count = self._save_candidates(final_candidates)
            self.stats['saved_candidates'] = saved_count
            
            # 8. Генерація звіту
            execution_time = datetime.now() - start_time
            report = self._generate_summary_report(execution_time)
            
            logger.info("✅ Аналіз завершено успішно!")
            return report
            
        except Exception as e:
            logger.error(f"💥 Критична помилка аналізу: {e}")
            raise
    
    def _validate_prerequisites(self):
        """Валідація передумов для аналізу"""
        logger.info("🔧 Валідація передумов...")
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Перевіряємо існування таблиць
                    cur.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema = 'osm_ukraine' 
                        AND table_name IN ('poi_processed', 'brand_candidates')
                    """)
                    
                    existing_tables = [row[0] for row in cur.fetchall()]
                    
                    if 'poi_processed' not in existing_tables:
                        raise Exception("Таблиця osm_ukraine.poi_processed не існує")
                    
                    if 'brand_candidates' not in existing_tables:
                        raise Exception("Таблиця osm_ukraine.brand_candidates не існує")
                    
                    # Перевіряємо кількість POI
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM osm_ukraine.poi_processed 
                        WHERE entity_type = 'poi'
                    """)
                    
                    poi_count = cur.fetchone()[0]
                    if poi_count == 0:
                        raise Exception("Немає POI в таблиці poi_processed")
                    
                    logger.info(f"✅ Знайдено {poi_count:,} POI для аналізу")
                    
        except psycopg2.Error as e:
            raise Exception(f"Помилка підключення до БД: {e}")
    
    def _get_available_regions(self) -> List[str]:
        """Отримання списку доступних регіонів"""
        regions = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT region_name
                        FROM osm_ukraine.poi_processed 
                        WHERE entity_type = 'poi'
                        AND brand_normalized IS NULL
                        AND quality_score >= %s
                        ORDER BY region_name
                    """, (CONFIG['analysis']['min_quality_score'],))
                    
                    regions = [row[0] for row in cur.fetchall()]
                    
        except Exception as e:
            logger.error(f"Помилка отримання регіонів: {e}")
            raise
        
        return regions
    
    def _analyze_single_region(self, region_name: str):
        """Аналіз кандидатів в одному регіоні"""
        logger.info(f"🔍 Аналізуємо регіон: {region_name}")
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # SQL запит для знаходження POI без брендів
                    cur.execute("""
                        SELECT 
                            name_original,
                            primary_category,
                            secondary_category,
                            COUNT(*) as frequency,
                            array_agg(DISTINCT h3_res_8) as h3_hexes,
                            AVG(quality_score) as avg_quality,
                            MIN(quality_score) as min_quality,
                            MAX(quality_score) as max_quality,
                            array_agg(DISTINCT secondary_category) as categories
                        FROM osm_ukraine.poi_processed 
                        WHERE region_name = %s
                          AND brand_normalized IS NULL 
                          AND entity_type = 'poi'
                          AND quality_score >= %s
                          AND name_original IS NOT NULL
                          AND length(name_original) BETWEEN %s AND %s
                        GROUP BY name_original, primary_category, secondary_category
                        HAVING COUNT(*) >= %s
                        ORDER BY COUNT(*) DESC
                    """, (
                        region_name,
                        CONFIG['analysis']['min_quality_score'],
                        CONFIG['analysis']['min_name_length'],
                        CONFIG['analysis']['max_name_length'],
                        CONFIG['analysis']['min_frequency']
                    ))
                    
                    regional_candidates = []
                    
                    for row in cur.fetchall():
                        candidate = dict(row)
                        candidate['region_name'] = region_name
                        
                        # Валідація кандидата
                        if self._validate_candidate(candidate):
                            regional_candidates.append(candidate)
                            self.stats['poi_analyzed'] += candidate['frequency']
                    
                    self.regional_results[region_name] = regional_candidates
                    self.stats['unique_names_found'] += len(regional_candidates)
                    
                    logger.info(f"📊 {region_name}: знайдено {len(regional_candidates)} кандидатів")
                    
        except Exception as e:
            logger.error(f"Помилка аналізу регіону {region_name}: {e}")
            raise
    
    def _validate_candidate(self, candidate: Dict[str, Any]) -> bool:
        """Валідація окремого кандидата"""
        name = candidate['name_original'].strip().lower()
        
        # 1. Перевірка на generic назви
        if self._is_generic_name(name):
            self.stats['generic_filtered'] += 1
            return False
        
        # 2. Перевірка через BrandMatcher (чи не пропустили існуючий бренд)
        try:
            brand_result = self.brand_matcher.match_brand(
                candidate['name_original'],
                {'shop': candidate.get('secondary_category', '')}
            )
            
            if brand_result and brand_result.confidence > 0.8:
                logger.debug(f"🔍 Знайдено існуючий бренд для '{candidate['name_original']}': {brand_result.canonical_name}")
                self.stats['existing_brand_matches'] += 1
                return False
                
        except Exception as e:
            logger.warning(f"Помилка BrandMatcher для '{candidate['name_original']}': {e}")
        
        # 3. Перевірка якості
        if candidate['avg_quality'] < CONFIG['analysis']['min_quality_score']:
            return False
        
        return True
    
    def _is_generic_name(self, name: str) -> bool:
        """Перевірка чи є назва загальною (generic)"""
        name_lower = name.lower().strip()
        
        # Точне співпадіння з generic patterns
        if name_lower in GENERIC_PATTERNS:
            return True
        
        # Pattern matching для типових конструкцій
        generic_patterns_regex = [
            r'^магазин\s*\d*$',
            r'^аптека\s*\d*$', 
            r'^кафе\s*\d*$',
            r'^\d+$',  # Тільки цифри
            r'^[а-я]{1,2}\d+$',  # Короткі абревіатури + цифри
        ]
        
        import re
        for pattern in generic_patterns_regex:
            if re.match(pattern, name_lower):
                return True
        
        return False
    
    def _aggregate_regional_data(self):
        """Агрегація результатів по всіх регіонах"""
        logger.info("🔄 Агрегуємо дані по регіонах...")
        
        aggregated = {}
        
        for region_name, candidates in self.regional_results.items():
            for candidate in candidates:
                name = candidate['name_original']
                
                if name not in aggregated:
                    aggregated[name] = {
                        'name_original': name,
                        'total_frequency': 0,
                        'regions': [],
                        'categories': set(),
                        'h3_coverage': set(),
                        'quality_scores': [],
                        'primary_categories': set()
                    }
                
                # Агрегуємо дані
                agg_data = aggregated[name]
                agg_data['total_frequency'] += candidate['frequency']
                agg_data['regions'].append(region_name)
                agg_data['categories'].update(candidate['categories'] or [])
                agg_data['h3_coverage'].update(candidate['h3_hexes'] or [])
                agg_data['quality_scores'].append(candidate['avg_quality'])
                agg_data['primary_categories'].add(candidate['primary_category'])
        
        self.aggregated_candidates = aggregated
        logger.info(f"📊 Агреговано {len(aggregated)} унікальних назв")
    
    def _calculate_network_potential(self):
        """Розрахунок мережевого потенціалу для кожного кандидата"""
        logger.info("🧮 Розраховуємо мережевий потенціал...")
        
        for name, data in self.aggregated_candidates.items():
            # Основні метрики
            region_count = len(data['regions'])
            total_frequency = data['total_frequency']
            quality_scores = [float(score) for score in data['quality_scores']]
            avg_quality = sum(quality_scores) / len(quality_scores)
            h3_spread = len(data['h3_coverage'])
            
            # Розрахунок scores
            network_score = min((region_count - 1) / 5.0, 1.0) if region_count >= 2 else 0.0
            frequency_score = min(total_frequency / 50.0, 1.0)
            geographic_score = min(h3_spread / 20.0, 1.0)
            
            # Комбінований confidence score
            confidence = (
                network_score * 0.4 +      # Найважливіше - мережевість
                frequency_score * 0.3 +    # Частота появи  
                avg_quality * 0.2 +        # Якість даних
                geographic_score * 0.1     # Географічне покриття
            )
            
            # Додаємо розраховані метрики
            data.update({
                'region_count': region_count,
                'network_score': float(network_score),
                'frequency_score': float(frequency_score),
                'geographic_score': geographic_score,
                'avg_quality': avg_quality,
                'confidence_score': confidence,
                'is_network_candidate': region_count >= CONFIG['analysis']['min_regions_for_network']
            })
        
        # Статистика
        network_candidates = sum(1 for data in self.aggregated_candidates.values() 
                               if data['is_network_candidate'])
        self.stats['network_candidates'] = network_candidates
        
        logger.info(f"📈 Знайдено {network_candidates} мережевих кандидатів")
    
    def _apply_quality_filters(self) -> List[Dict[str, Any]]:
        """Застосування фінальних фільтрів якості"""
        logger.info("🔧 Застосовуємо фільтри якості...")
        
        quality_candidates = []
        
        for name, data in self.aggregated_candidates.items():
            # Фільтр по мінімальній впевненості
            if data['confidence_score'] >= CONFIG['analysis']['min_confidence']:
                # Підготовка даних для збереження
                candidate = {
                    'name': name,
                    'frequency': data['total_frequency'],
                    'locations': data['regions'],
                    'categories': list(data['categories']),
                    'confidence_score': round(data['confidence_score'], 3),
                    'suggested_canonical_name': name,  # Поки без обробки
                    'suggested_functional_group': self._suggest_functional_group(data),
                    'suggested_influence_weight': self._suggest_influence_weight(data),
                    'suggested_format': self._suggest_format(data),
                    'recommendation_reason': self._generate_recommendation_reason(data)
                }
                
                quality_candidates.append(candidate)
        
        self.stats['quality_candidates'] = len(quality_candidates)
        
        # Сортуємо по впевненості
        quality_candidates.sort(key=lambda x: x['confidence_score'], reverse=True)
        
        logger.info(f"✅ Відібрано {len(quality_candidates)} якісних кандидатів")
        return quality_candidates
    
    def _suggest_functional_group(self, data: Dict[str, Any]) -> str:
        """Пропонуємо функціональну групу на основі категорій"""
        primary_cats = data['primary_categories']
        
        if 'retail' in primary_cats:
            return 'competitor'
        elif 'food_service' in primary_cats:
            return 'competitor'
        elif 'transport' in primary_cats:
            return 'accessibility'
        else:
            return 'traffic_generator'
    
    def _suggest_influence_weight(self, data: Dict[str, Any]) -> float:
        """Пропонуємо вагу впливу"""
        functional_group = self._suggest_functional_group(data)
        
        if functional_group == 'competitor':
            # Чим більше мережа, тим сильніше конкуренція
            return -0.3 - (data['region_count'] * 0.1)
        elif functional_group == 'accessibility':
            return 0.3 + (data['region_count'] * 0.05)
        else:
            return 0.1
    
    def _suggest_format(self, data: Dict[str, Any]) -> str:
        """Пропонуємо формат закладу"""
        categories = data['categories']
        
        if 'supermarket' in categories or 'convenience' in categories:
            return 'магазин'
        elif 'restaurant' in categories or 'cafe' in categories:
            return 'заклад харчування'
        elif 'pharmacy' in categories:
            return 'аптека'
        else:
            return 'заклад обслуговування'
    
    def _generate_recommendation_reason(self, data: Dict[str, Any]) -> str:
        """Генеруємо причину рекомендації"""
        reasons = []
        
        if data['region_count'] >= 3:
            reasons.append(f"мережа в {data['region_count']} регіонах")
        elif data['region_count'] == 2:
            reasons.append("присутність в 2 регіонах")
        
        if data['total_frequency'] >= 20:
            reasons.append(f"висока частота ({data['total_frequency']} локацій)")
        
        if data['avg_quality'] >= 0.8:
            reasons.append("висока якість даних")
        
        return f"Авто-рекомендація: {', '.join(reasons)}"
    
    def _save_candidates(self, candidates: List[Dict[str, Any]]) -> int:
        """Збереження кандидатів в БД"""
        if not candidates:
            logger.info("📝 Немає кандидатів для збереження")
            return 0
        
        logger.info(f"💾 Зберігаємо {len(candidates)} кандидатів...")
        
        saved_count = 0
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    for candidate in candidates:
                        try:
                            # Використовуємо метод з BrandManager
                            from datetime import datetime
                            
                            # Перевіряємо чи існує кандидат
                            cur.execute("""
                                SELECT candidate_id FROM osm_ukraine.brand_candidates
                                WHERE name = %s
                            """, (candidate['name'],))
                            
                            if cur.fetchone():
                                # Оновлюємо існуючий
                                cur.execute("""
                                    UPDATE osm_ukraine.brand_candidates 
                                    SET frequency = %s,
                                        last_seen = NOW(),
                                        locations = %s,
                                        categories = %s,
                                        confidence_score = %s,
                                        suggested_canonical_name = %s,
                                        suggested_functional_group = %s,
                                        suggested_influence_weight = %s,
                                        suggested_format = %s,
                                        recommendation_reason = %s
                                    WHERE name = %s
                                """, (
                                    candidate['frequency'],
                                    candidate['locations'],
                                    candidate['categories'],
                                    candidate['confidence_score'],
                                    candidate['suggested_canonical_name'],
                                    candidate['suggested_functional_group'],
                                    candidate['suggested_influence_weight'],
                                    candidate['suggested_format'],
                                    candidate['recommendation_reason'],
                                    candidate['name']
                                ))
                            else:
                                # Створюємо новий
                                cur.execute("""
                                    INSERT INTO osm_ukraine.brand_candidates (
                                        name, frequency, first_seen, last_seen, locations, categories, 
                                        status, confidence_score, suggested_canonical_name,
                                        suggested_functional_group, suggested_influence_weight,
                                        suggested_format, recommendation_reason
                                    ) VALUES (
                                        %s, %s, NOW(), NOW(), %s, %s, 'new', %s, %s, %s, %s, %s, %s
                                    )
                                """, (
                                    candidate['name'],
                                    candidate['frequency'],
                                    candidate['locations'],
                                    candidate['categories'],
                                    candidate['confidence_score'],
                                    candidate['suggested_canonical_name'],
                                    candidate['suggested_functional_group'],
                                    candidate['suggested_influence_weight'],
                                    candidate['suggested_format'],
                                    candidate['recommendation_reason']
                                ))
                            
                            saved_count += 1
                            
                        except Exception as e:
                            logger.error(f"Помилка збереження кандидата '{candidate['name']}': {e}")
                            self.stats['errors'] += 1
                            continue
                    
                    conn.commit()
                    
        except Exception as e:
            logger.error(f"Критична помилка збереження: {e}")
            raise
        
        logger.info(f"✅ Збережено {saved_count} кандидатів")
        return saved_count
    
    def _generate_summary_report(self, execution_time) -> Dict[str, Any]:
        """Генерація підсумкового звіту"""
        logger.info("📊 Генеруємо підсумковий звіт...")
        
        report = {
            'execution_time': str(execution_time),
            'statistics': self.stats.copy(),
            'top_candidates': self._get_top_candidates(10),
            'timestamp': datetime.now().isoformat()
        }
        
        # Console output
        print("\n" + "="*60)
        print("🔍 BRAND CANDIDATE ANALYSIS RESULTS")
        print("="*60)
        print(f"📊 Regions analyzed: {self.stats['regions_processed']}")
        print(f"📈 Total POI processed: {self.stats['poi_analyzed']:,}")
        print(f"🏷️  Unique names found: {self.stats['unique_names_found']:,}")
        print(f"🚫 Generic names filtered: {self.stats['generic_filtered']:,}")
        print(f"✅ Existing brand matches: {self.stats['existing_brand_matches']:,}")
        print(f"🎯 Quality candidates: {self.stats['quality_candidates']}")
        print(f"🏢 Network candidates (2+ regions): {self.stats['network_candidates']}")
        print(f"💾 Saved to database: {self.stats['saved_candidates']}")
        print(f"❌ Errors encountered: {self.stats['errors']}")
        print(f"⏱️  Execution time: {execution_time}")
        
        # Top candidates
        if report['top_candidates']:
            print(f"\n🏆 TOP {len(report['top_candidates'])} CANDIDATES:")
            for i, candidate in enumerate(report['top_candidates'], 1):
                print(f"  {i:2d}. \"{candidate['name']}\" - {candidate['frequency']} locations, "
                      f"{len(candidate['locations'])} regions (conf: {candidate['confidence_score']:.3f})")
        
        print(f"\n✅ Analysis completed successfully!")
        print("="*60)
        
        return report
    
    def _get_top_candidates(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Отримання топ кандидатів для звіту"""
        top_candidates = []
        
        # Сортуємо по впевненості
        sorted_candidates = sorted(
            self.aggregated_candidates.items(),
            key=lambda x: x[1]['confidence_score'],
            reverse=True
        )
        
        for name, data in sorted_candidates[:limit]:
            if data['confidence_score'] >= CONFIG['analysis']['min_confidence']:
                top_candidates.append({
                    'name': name,
                    'frequency': data['total_frequency'],
                    'locations': data['regions'],
                    'confidence_score': data['confidence_score'],
                    'is_network': data['is_network_candidate']
                })
        
        return top_candidates


def main():
    """Головна функція"""
    try:
        # Створюємо аналізатор
        finder = BrandCandidateFinder(DB_CONNECTION_STRING)
        
        # Запускаємо аналіз
        report = finder.run_analysis()
        
        # Опціонально можемо зберегти звіт у файл
        # with open(f'brand_candidates_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
        #     json.dump(report, f, indent=2, ensure_ascii=False)
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 Фатальна помилка: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)