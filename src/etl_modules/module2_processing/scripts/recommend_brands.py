#!/usr/bin/env python3
"""
Brand Recommendation Engine
Автоматично аналізує brand_candidates та створює рекомендації
для затвердження, відхилення або ручного review
"""

import sys
import logging
import psycopg2
import re
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from psycopg2.extras import RealDictCursor, Json
from dataclasses import dataclass
from enum import Enum

# Додаємо поточну директорію до path для імпортів
sys.path.insert(0, str(Path(__file__).parent.parent))

# Імпорти наших модулів
from normalization.brand_manager import BrandManager

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"


class RecommendationStatus(Enum):
    """Статуси рекомендацій - відповідають DB constraint"""
    APPROVED = "approved"        # Для автоматичного затвердження
    REVIEWING = "reviewing"      # Для ручного перегляду
    REJECTED = "rejected"        # Для відхилення


@dataclass
class RecommendationResult:
    """Результат рекомендації"""
    status: RecommendationStatus
    confidence_score: float
    reason: str
    suggested_canonical_name: str
    suggested_functional_group: str
    suggested_influence_weight: float
    suggested_format: str


class BrandRecommendationEngine:
    """Основний клас для аналізу та рекомендацій брендів"""
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        
        # Статистика
        self.stats = {
            'candidates_processed': 0,
            'approved': 0,
            'reviewing': 0,
            'rejected': 0,
            'saved_successfully': 0,
            'save_errors': 0
        }
        
        # Завантажуємо правила та patterns
        self._load_recommendation_rules()
        
        logger.info("✅ BrandRecommendationEngine ініціалізовано")
    
    def _load_recommendation_rules(self):
        """Завантаження правил для рекомендацій"""
        
        # Patterns для автоматичного відхилення
        self.rejection_patterns = [
            # Адміністративні установи
            r'сільська рада', r'міська рада', r'селищна рада',
            r'будинок культури', r'дім культури', r'клуб$',
            r'фельдшерсько.*пункт', r'амбулаторія', r'поліклініка',
            r'лікарня', r'школа$', r'гімназія', r'ліцей',
            
            # Загальні сервіси  
            r'^сто$', r'^шиномонтаж$', r'^автосервіс$', r'^автомийка$',
            r'^перукарня$', r'^салон краси$', r'^майстерня$',
            r'^ательє$', r'^прокат$',
            
            # Занадто загальні
            r'^магазин\s*\d*$', r'^shop\s*\d*$', r'^store\s*\d*$',
            r'^кафе\s*\d*$', r'^ресторан\s*\d*$', r'^їдальня\s*\d*$',
            r'^\d+$', r'^№\s*\d+$', r'^n\s*\d+$',
            r'^[а-яё]{1,2}\d+$'
        ]
        
        # Високоякісні брендові patterns
        self.brand_patterns = [
            # Банки
            r'.*банк$', r'.*bank$', r'креди.*банк', r'.*фінанс.*',
            # АЗС
            r'.*нафта$', r'.*oil$', r'.*petrol$', r'.*gas$',
            # Франшизи
            r'.*pizza$', r'.*burger$', r'.*coffee$', r'.*express$'
        ]
        
        logger.info("📋 Завантажено правила рекомендацій")
    
    def run_recommendations(self) -> Dict[str, Any]:
        """Головний метод виконання рекомендацій"""
        logger.info("🤖 Початок автоматичних рекомендацій...")
        
        start_time = datetime.now()
        
        try:
            # 1. Отримуємо нових кандидатів
            new_candidates = self._get_new_candidates()
            logger.info(f"📋 Знайдено {len(new_candidates)} нових кандидатів")
            
            if not new_candidates:
                logger.info("✅ Немає нових кандидатів для обробки")
                return self._generate_report(datetime.now() - start_time)
            
            # 2. Обробляємо кожного кандидата
            for candidate in new_candidates:
                try:
                    result = self._analyze_candidate(candidate)
                    self._save_single_recommendation(candidate, result)
                    self.stats['candidates_processed'] += 1
                    
                    # Оновлюємо статистику
                    if result.status == RecommendationStatus.APPROVED:
                        self.stats['approved'] += 1
                    elif result.status == RecommendationStatus.REVIEWING:
                        self.stats['reviewing'] += 1
                    elif result.status == RecommendationStatus.REJECTED:
                        self.stats['rejected'] += 1
                        
                except Exception as e:
                    logger.error(f"❌ Помилка обробки кандидата '{candidate.get('name', 'Unknown')}': {e}")
                    continue
            
            # 3. Генеруємо звіт
            execution_time = datetime.now() - start_time
            report = self._generate_report(execution_time)
            
            logger.info("✅ Рекомендації завершено успішно!")
            return report
            
        except Exception as e:
            logger.error(f"💥 Критична помилка рекомендацій: {e}")
            raise
    
    def _get_new_candidates(self) -> List[Dict[str, Any]]:
        """Отримання нових кандидатів зі status='new'"""
        candidates = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT 
                            candidate_id, name, frequency, locations, categories,
                            confidence_score, suggested_canonical_name,
                            suggested_functional_group, suggested_influence_weight,
                            suggested_format, recommendation_reason,
                            first_seen, last_seen
                        FROM osm_ukraine.brand_candidates
                        WHERE status = 'new'
                        ORDER BY frequency DESC, confidence_score DESC
                        LIMIT 2000
                    """)
                    
                    for row in cur.fetchall():
                        candidates.append(dict(row))
                        
        except Exception as e:
            logger.error(f"Помилка отримання кандидатів: {e}")
            raise
        
        return candidates
    
    def _analyze_candidate(self, candidate: Dict[str, Any]) -> RecommendationResult:
        """Аналіз одного кандидата та генерація рекомендації"""
        name = candidate['name'].strip()
        name_lower = name.lower()
        frequency = int(candidate['frequency'])
        regions_count = len(candidate['locations'] or [])
        current_confidence = float(candidate.get('confidence_score', 0.0))
        categories = candidate.get('categories', []) or []
        
        # 1. Перевірка на автоматичне відхилення
        for pattern in self.rejection_patterns:
            if re.search(pattern, name_lower):
                return RecommendationResult(
                    status=RecommendationStatus.REJECTED,
                    confidence_score=max(current_confidence - 0.3, 0.1),
                    reason=f"Відхилено: збіг з pattern '{pattern}'",
                    suggested_canonical_name=name.title(),
                    suggested_functional_group='neutral',
                    suggested_influence_weight=0.0,
                    suggested_format='заклад'
                )
        
        # 2. Розрахунок базових метрик
        # Frequency score
        if frequency >= 50:
            freq_score = 1.0
        elif frequency >= 20:
            freq_score = 0.8
        elif frequency >= 10:
            freq_score = 0.6
        else:
            freq_score = 0.4
        
        # Regional spread score
        if regions_count >= 10:
            region_score = 1.0
        elif regions_count >= 5:
            region_score = 0.9
        elif regions_count >= 3:
            region_score = 0.7
        elif regions_count >= 2:
            region_score = 0.5
        else:
            region_score = 0.2
        
        # Brand quality score
        brand_score = self._calculate_brand_score(name, name_lower)
        
        # Category consistency score
        category_score = self._calculate_category_score(categories)
        
        # 3. Комбінований score
        final_score = (
            freq_score * 0.3 +      # Частота
            region_score * 0.3 +    # Географія  
            brand_score * 0.25 +    # Брендовість
            category_score * 0.15   # Категорії
        )
        
        # 4. Генерація рекомендацій
        suggested_canonical_name = self._suggest_canonical_name(name)
        suggested_functional_group = self._suggest_functional_group(categories)
        suggested_influence_weight = self._suggest_influence_weight(suggested_functional_group, regions_count)
        suggested_format = self._suggest_format(categories, name)
        
        # 5. Фінальне рішення з правильними статусами
        if final_score >= 0.75 and freq_score >= 0.6:
            status = RecommendationStatus.APPROVED
            reason = f"Затверджено: високий score ({final_score:.3f}), частота {frequency}, {regions_count} регіонів"
            final_confidence = min(current_confidence + 0.2, 1.0)
        elif final_score >= 0.5:
            status = RecommendationStatus.REVIEWING
            reason = f"Потребує перегляду: середній score ({final_score:.3f}), частота {frequency}, {regions_count} регіонів"
            final_confidence = current_confidence
        else:
            status = RecommendationStatus.REJECTED
            reason = f"Відхилено: низький score ({final_score:.3f}), частота {frequency}, {regions_count} регіонів"
            final_confidence = max(current_confidence - 0.2, 0.1)
        
        logger.debug(f"🔍 '{name}': {status.value} (score: {final_score:.3f})")
        
        return RecommendationResult(
            status=status,
            confidence_score=final_confidence,
            reason=reason,
            suggested_canonical_name=suggested_canonical_name,
            suggested_functional_group=suggested_functional_group,
            suggested_influence_weight=suggested_influence_weight,
            suggested_format=suggested_format
        )
    
    def _calculate_brand_score(self, name: str, name_lower: str) -> float:
        """Розрахунок брендової якості назви"""
        score = 0.5  # базова оцінка
        
        # Довжина назви
        if 3 <= len(name) <= 15:
            score += 0.1
        elif len(name) > 20:
            score -= 0.1
        
        # Капіталізація
        if name.istitle():
            score += 0.1
        
        # Брендові patterns
        for pattern in self.brand_patterns:
            if re.search(pattern, name_lower):
                score += 0.2
                break
        
        # Унікальність (не містить загальних слів)
        generic_words = ['магазин', 'кафе', 'ресторан', 'аптека', 'сервіс']
        if not any(word in name_lower for word in generic_words):
            score += 0.1
        
        # Спеціальні символи (брендові)
        if re.search(r'[&+\-]', name):
            score += 0.05
        
        return min(score, 1.0)
    
    def _calculate_category_score(self, categories: List[str]) -> float:
        """Розрахунок якості категорій"""
        if not categories:
            return 0.3
        
        if len(categories) == 1:
            return 1.0  # Єдина категорія - найкраща консистентність
        
        # Перевірка на пов'язані категорії
        related_groups = [
            {'retail', 'supermarket', 'convenience'},
            {'restaurant', 'cafe', 'fast_food'},
            {'pharmacy', 'medical'},
            {'bank', 'financial'}
        ]
        
        categories_set = set(cat.lower() for cat in categories)
        for group in related_groups:
            if len(categories_set & group) > 1:
                return 0.8
        
        return 0.4  # Різнорідні категорії
    
    def _suggest_canonical_name(self, name: str) -> str:
        """Пропозиція канонічної назви"""
        canonical = name.strip()
        
        # Title case якщо все великими або малими
        if canonical.isupper() or canonical.islower():
            canonical = canonical.title()
        
        # Прибираємо зайві пробіли
        canonical = re.sub(r'\s+', ' ', canonical)
        
        return canonical
    
    def _suggest_functional_group(self, categories: List[str]) -> str:
        """Пропозиція функціональної групи"""
        if not categories:
            return 'traffic_generator'
        
        categories_lower = [cat.lower() for cat in categories]
        
        # Retail competitors
        if any(cat in categories_lower for cat in ['retail', 'supermarket', 'convenience', 'grocery']):
            return 'competitor'
        
        # Food service competitors
        if any(cat in categories_lower for cat in ['restaurant', 'cafe', 'fast_food']):
            return 'competitor'
        
        # Healthcare competitors
        if any(cat in categories_lower for cat in ['pharmacy', 'medical']):
            return 'competitor'
        
        # Financial services
        if any(cat in categories_lower for cat in ['bank', 'financial']):
            return 'traffic_generator'
        
        # Transport/fuel
        if any(cat in categories_lower for cat in ['fuel', 'gas_station', 'transport']):
            return 'accessibility'
        
        return 'traffic_generator'
    
    def _suggest_influence_weight(self, functional_group: str, regions_count: int) -> float:
        """Пропозиція ваги впливу"""
        base_weights = {
            'competitor': -0.4,
            'traffic_generator': 0.2,
            'accessibility': 0.3,
            'neutral': 0.0
        }
        
        base_weight = base_weights.get(functional_group, 0.0)
        
        # Коригування на розмір мережі
        if functional_group == 'competitor':
            # Більша мережа = сильніша конкуренція
            network_penalty = min(regions_count * 0.05, 0.3)
            final_weight = base_weight - network_penalty
            return max(final_weight, -1.0)
        
        elif functional_group in ['traffic_generator', 'accessibility']:
            # Більша мережа = більший позитивний вплив
            network_bonus = min(regions_count * 0.03, 0.2)
            final_weight = base_weight + network_bonus
            return min(final_weight, 1.0)
        
        return base_weight
    
    def _suggest_format(self, categories: List[str], name: str) -> str:
        """Пропозиція формату"""
        if not categories:
            return 'заклад'
        
        categories_lower = [cat.lower() for cat in categories]
        name_lower = name.lower()
        
        # Mapping категорій до форматів
        format_mapping = {
            'supermarket': 'супермаркет',
            'convenience': 'магазин',
            'retail': 'магазин',
            'restaurant': 'ресторан',
            'cafe': 'кафе',
            'fast_food': 'заклад швидкого харчування',
            'pharmacy': 'аптека',
            'bank': 'банк',
            'fuel': 'АЗС',
            'gas_station': 'АЗС'
        }
        
        # Знаходимо найкращий match
        for category in categories_lower:
            if category in format_mapping:
                return format_mapping[category]
        
        # Аналіз по назві
        if any(word in name_lower for word in ['банк', 'bank']):
            return 'банк'
        elif any(word in name_lower for word in ['аптека', 'pharmacy']):
            return 'аптека'
        elif any(word in name_lower for word in ['кафе', 'cafe']):
            return 'кафе'
        
        return 'заклад'
    
    def _save_single_recommendation(self, candidate: Dict[str, Any], result: RecommendationResult):
        """Збереження однієї рекомендації в окремій транзакції"""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Безпечне обрізання тексту
                    reason = result.reason[:500] if result.reason else None
                    
                    cur.execute("""
                        UPDATE osm_ukraine.brand_candidates
                        SET status = %s,
                            confidence_score = %s,
                            suggested_canonical_name = %s,
                            suggested_functional_group = %s,
                            suggested_influence_weight = %s,
                            suggested_format = %s,
                            recommendation_reason = %s,
                            reviewed_at = NOW(),
                            reviewed_by = %s
                        WHERE candidate_id = %s
                    """, (
                        result.status.value,
                        result.confidence_score,
                        result.suggested_canonical_name,
                        result.suggested_functional_group,
                        result.suggested_influence_weight,
                        result.suggested_format,
                        reason,
                        'recommendation_engine',
                        candidate['candidate_id']
                    ))
                    
                    if cur.rowcount > 0:
                        conn.commit()
                        self.stats['saved_successfully'] += 1
                        logger.debug(f"✅ Збережено рекомендацію для '{candidate['name']}'")
                    else:
                        logger.warning(f"⚠️ Кандидат '{candidate['name']}' не знайдено в БД")
                        
        except Exception as e:
            self.stats['save_errors'] += 1
            logger.error(f"❌ Помилка збереження рекомендації для '{candidate.get('name', 'Unknown')}': {e}")
    
    def _generate_report(self, execution_time) -> Dict[str, Any]:
        """Генерація підсумкового звіту"""
        logger.info("📊 Генеруємо звіт рекомендацій...")
        
        # Отримуємо приклади з БД
        top_approved = self._get_examples_by_status('approved', 5)
        top_rejected = self._get_examples_by_status('rejected', 5)
        needs_review = self._get_examples_by_status('reviewing', 5)
        
        report = {
            'execution_time': str(execution_time),
            'statistics': self.stats.copy(),
            'top_approved': top_approved,
            'top_rejected': top_rejected,
            'needs_review_examples': needs_review,
            'timestamp': datetime.now().isoformat()
        }
        
        # Console output
        print("\n" + "="*70)
        print("🤖 BRAND RECOMMENDATION ENGINE RESULTS")
        print("="*70)
        print(f"📊 Total candidates processed: {self.stats['candidates_processed']:,}")
        print(f"✅ Approved for batch processing: {self.stats['approved']:,}")
        print(f"🔍 Needs manual review: {self.stats['reviewing']:,}")
        print(f"❌ Rejected: {self.stats['rejected']:,}")
        print(f"💾 Successfully saved: {self.stats['saved_successfully']:,}")
        print(f"⚠️  Save errors: {self.stats['save_errors']:,}")
        print(f"⏱️  Execution time: {execution_time}")
        
        # Breakdown percentages
        if self.stats['candidates_processed'] > 0:
            approval_rate = (self.stats['approved'] / self.stats['candidates_processed']) * 100
            review_rate = (self.stats['reviewing'] / self.stats['candidates_processed']) * 100
            rejection_rate = (self.stats['rejected'] / self.stats['candidates_processed']) * 100
            
            print(f"\n📈 BREAKDOWN:")
            print(f"   Auto-approval rate: {approval_rate:.1f}%")
            print(f"   Manual review rate: {review_rate:.1f}%")
            print(f"   Rejection rate: {rejection_rate:.1f}%")
        
        # Top approved
        if top_approved:
            print(f"\n🏆 TOP APPROVED BRANDS:")
            for i, candidate in enumerate(top_approved, 1):
                regions = len(candidate.get('locations', []))
                print(f"   {i}. \"{candidate['name']}\" - {candidate['frequency']} locations, "
                      f"{regions} regions (conf: {candidate['confidence_score']:.3f})")
        
        # Examples needing review
        if needs_review:
            print(f"\n🔍 EXAMPLES NEEDING REVIEW:")
            for i, candidate in enumerate(needs_review[:3], 1):
                regions = len(candidate.get('locations', []))
                print(f"   {i}. \"{candidate['name']}\" - {candidate['frequency']} locations, "
                      f"{regions} regions (conf: {candidate['confidence_score']:.3f})")
        
        # Common rejections
        if top_rejected:
            print(f"\n❌ COMMON REJECTIONS:")
            for i, candidate in enumerate(top_rejected[:3], 1):
                print(f"   {i}. \"{candidate['name']}\" - {candidate.get('recommendation_reason', 'No reason')[:50]}...")
        
        print(f"\n✅ Recommendation analysis completed!")
        print("="*70)
        
        return report
    
    def _get_examples_by_status(self, status: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Отримання прикладів за статусом"""
        examples = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT name, frequency, locations, confidence_score, 
                               recommendation_reason, suggested_canonical_name
                        FROM osm_ukraine.brand_candidates
                        WHERE status = %s 
                        AND reviewed_by = 'recommendation_engine'
                        ORDER BY confidence_score DESC, frequency DESC
                        LIMIT %s
                    """, (status, limit))
                    
                    for row in cur.fetchall():
                        examples.append(dict(row))
                        
        except Exception as e:
            logger.error(f"Помилка отримання прикладів для статусу '{status}': {e}")
        
        return examples


def main():
    """Головна функція"""
    try:
        # Створюємо recommendation engine
        engine = BrandRecommendationEngine(DB_CONNECTION_STRING)
        
        # Запускаємо рекомендації
        report = engine.run_recommendations()
        
        return 0
        
    except Exception as e:
        logger.error(f"💥 Фатальна помилка: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)