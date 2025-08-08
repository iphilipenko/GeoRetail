"""
Brand Manager
Динамічне управління брендами з можливістю додавання нових
"""

import json
import logging
import uuid
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
import psycopg2
from psycopg2.extras import Json, RealDictCursor

from .brand_dictionary import BrandDictionary, BrandInfo

logger = logging.getLogger(__name__)


@dataclass
class BrandCandidate:
    """Кандидат на новий бренд"""
    name: str
    frequency: int
    first_seen: datetime
    last_seen: datetime
    locations: List[str]  # region_names
    categories: Set[str]
    suggested_canonical_name: Optional[str] = None
    suggested_functional_group: Optional[str] = None
    confidence_score: float = 0.0


class BrandManager:
    """Управління брендами з можливістю додавання нових"""
    
    def __init__(self, db_connection_string: str, config_path: Optional[Path] = None):
        self.db_connection_string = db_connection_string
        self.config_path = config_path or Path(__file__).parent / "data" / "dictionaries"
        self.config_path.mkdir(parents=True, exist_ok=True)
        
        # Завантажуємо базовий словник
        self.brand_dict = BrandDictionary()
        
        # Завантажуємо додаткові бренди з файлу/БД
        self.custom_brands = self._load_custom_brands()
        
        # Кандидати на нові бренди
        self.brand_candidates = {}
        
        # Статистика
        self.stats = {
            'brands_added': 0,
            'candidates_found': 0,
            'brands_updated': 0
        }
    
    def _load_custom_brands(self) -> Dict[str, BrandInfo]:
        """Завантажує кастомні бренди з файлу або БД"""
        custom_brands = {}
        
        # Спроба завантажити з файлу
        custom_brands_file = self.config_path / "custom_brands.json"
        if custom_brands_file.exists():
            try:
                with open(custom_brands_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                for brand_id, brand_data in data.items():
                    custom_brands[brand_id] = BrandInfo(**brand_data)
                    
                logger.info(f"Завантажено {len(custom_brands)} кастомних брендів з файлу")
            except Exception as e:
                logger.error(f"Помилка завантаження кастомних брендів: {e}")
        
        # Спроба завантажити з БД
        try:
            custom_brands.update(self._load_brands_from_db())
        except Exception as e:
            logger.warning(f"Не вдалось завантажити бренди з БД: {e}")
        
        return custom_brands
    
    def _load_brands_from_db(self) -> Dict[str, BrandInfo]:
        """Завантажує бренди з PostgreSQL"""
        brands = {}
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT brand_id, canonical_name, synonyms, format,
                               influence_weight, functional_group, parent_company, osm_tags
                        FROM osm_ukraine.custom_brands
                        WHERE is_active = true
                    """)
                    
                    for row in cur:
                        brands[row['brand_id']] = BrandInfo(
                            canonical_name=row['canonical_name'],
                            synonyms=row['synonyms'] or [],
                            format=row['format'],
                            influence_weight=row['influence_weight'],
                            functional_group=row['functional_group'],
                            parent_company=row['parent_company'],
                            osm_tags=row['osm_tags'] or []
                        )
                        
            logger.info(f"Завантажено {len(brands)} брендів з БД")
            
        except Exception as e:
            logger.error(f"Помилка завантаження брендів з БД: {e}")
            
        return brands
    
    def _create_brand_tables(self):
        """Створює таблиці для управління брендами"""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Таблиця кастомних брендів
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS osm_ukraine.custom_brands (
                            brand_id VARCHAR PRIMARY KEY,
                            canonical_name VARCHAR NOT NULL,
                            synonyms TEXT[],
                            format VARCHAR,
                            influence_weight FLOAT DEFAULT 0.0,
                            functional_group VARCHAR DEFAULT 'neutral',
                            parent_company VARCHAR,
                            osm_tags TEXT[],
                            active BOOLEAN DEFAULT true,
                            created_at TIMESTAMP DEFAULT NOW(),
                            created_by VARCHAR,
                            updated_at TIMESTAMP DEFAULT NOW(),
                            updated_by VARCHAR,
                            source VARCHAR DEFAULT 'manual',
                            source_candidate_id UUID,
                            confidence_score FLOAT DEFAULT 1.0
                        )
                    """)
                    
                    # Таблиця кандидатів
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS osm_ukraine.brand_candidates (
                            candidate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            name VARCHAR NOT NULL,
                            frequency INT DEFAULT 1,
                            first_seen TIMESTAMP DEFAULT NOW(),
                            last_seen TIMESTAMP DEFAULT NOW(),
                            locations TEXT[],
                            categories TEXT[],
                            status VARCHAR DEFAULT 'new',
                            confidence_score FLOAT,
                            suggested_canonical_name VARCHAR,
                            suggested_functional_group VARCHAR,
                            suggested_influence_weight FLOAT,
                            suggested_format VARCHAR,
                            recommendation_reason TEXT,
                            reviewed_at TIMESTAMP,
                            reviewed_by VARCHAR,
                            approved_brand_id VARCHAR,
                            batch_id UUID,
                            processed_at TIMESTAMP,
                            rejection_reason TEXT,
                            created_at TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    
                    # Таблиця логів
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS osm_ukraine.brand_approval_log (
                            log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                            batch_id UUID,
                            action VARCHAR NOT NULL,
                            filters_used JSONB,
                            candidates_processed INT DEFAULT 0,
                            candidates_approved INT DEFAULT 0,
                            candidates_rejected INT DEFAULT 0,
                            processed_by VARCHAR,
                            processed_at TIMESTAMP DEFAULT NOW()
                        )
                    """)
                    
                    conn.commit()
                    logger.info("Створено таблиці для управління брендами")
                    
        except Exception as e:
            logger.error(f"Помилка створення таблиць: {e}")
    
    def add_brand(
        self,
        brand_id: str,
        canonical_name: str,
        synonyms: List[str],
        format: str,
        influence_weight: float,
        functional_group: str,
        parent_company: Optional[str] = None,
        osm_tags: Optional[List[str]] = None,
        created_by: str = "system"
    ) -> bool:
        """
        Додає новий бренд
        
        Args:
            brand_id: Унікальний ID бренду (наприклад, 'pizza_day')
            canonical_name: Офіційна назва (наприклад, 'Піцца Дей')
            synonyms: Список синонімів
            format: Формат закладу
            influence_weight: Вага впливу (-1.0 до 1.0)
            functional_group: Функціональна група
            
        Returns:
            True якщо успішно додано
        """
        # Валідація
        if not (-1.0 <= influence_weight <= 1.0):
            raise ValueError("influence_weight має бути між -1.0 та 1.0")
        
        if functional_group not in ['competitor', 'traffic_generator', 'accessibility', 'residential_indicator', 'neutral']:
            raise ValueError(f"Невідома функціональна група: {functional_group}")
        
        # Створюємо BrandInfo
        brand_info = BrandInfo(
            canonical_name=canonical_name,
            synonyms=synonyms,
            format=format,
            influence_weight=influence_weight,
            functional_group=functional_group,
            parent_company=parent_company,
            osm_tags=osm_tags
        )
        
        # Додаємо в пам'ять
        self.custom_brands[brand_id] = brand_info
        
        # Зберігаємо в БД
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO osm_ukraine.custom_brands 
                        (brand_id, canonical_name, synonyms, format, 
                         influence_weight, functional_group, parent_company, 
                         osm_tags, created_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (brand_id) DO UPDATE SET
                            canonical_name = EXCLUDED.canonical_name,
                            synonyms = EXCLUDED.synonyms,
                            format = EXCLUDED.format,
                            influence_weight = EXCLUDED.influence_weight,
                            functional_group = EXCLUDED.functional_group,
                            parent_company = EXCLUDED.parent_company,
                            osm_tags = EXCLUDED.osm_tags,
                            updated_at = NOW(),
                            updated_by = EXCLUDED.created_by
                    """, (
                        brand_id, canonical_name, synonyms, format,
                        influence_weight, functional_group, parent_company,
                        osm_tags, created_by
                    ))
                    
                    conn.commit()
                    
            self.stats['brands_added'] += 1
            logger.info(f"Додано новий бренд: {canonical_name} (ID: {brand_id})")
            
            # Зберігаємо в файл як backup
            self._save_custom_brands_to_file()
            
            return True
            
        except Exception as e:
            logger.error(f"Помилка додавання бренду: {e}")
            return False
    
    def _save_custom_brands_to_file(self):
        """Зберігає кастомні бренди у файл"""
        custom_brands_file = self.config_path / "custom_brands.json"
        
        data = {}
        for brand_id, brand_info in self.custom_brands.items():
            data[brand_id] = {
                'canonical_name': brand_info.canonical_name,
                'synonyms': brand_info.synonyms,
                'format': brand_info.format,
                'influence_weight': brand_info.influence_weight,
                'functional_group': brand_info.functional_group,
                'parent_company': brand_info.parent_company,
                'osm_tags': brand_info.osm_tags
            }
        
        with open(custom_brands_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def get_all_brands(self) -> Dict[str, BrandInfo]:
        """Повертає всі бренди (базові + кастомні)"""
        all_brands = {}
        all_brands.update(self.brand_dict.brands)
        all_brands.update(self.custom_brands)
        return all_brands
    
    def find_brand(self, name: str) -> Optional[Tuple[str, BrandInfo]]:
        """Шукає бренд (спочатку в кастомних, потім в базових)"""
        # Шукаємо в кастомних
        for brand_id, brand_info in self.custom_brands.items():
            if self._name_matches(name, brand_info):
                return brand_id, brand_info
        
        # Шукаємо в базових
        return self.brand_dict.find_brand_by_name(name)
    
    def _name_matches(self, name: str, brand_info: BrandInfo) -> bool:
        """Перевіряє чи збігається назва з брендом"""
        name_lower = name.lower().strip()
        
        # Перевіряємо канонічну назву
        if name_lower == brand_info.canonical_name.lower().strip():
            return True
        
        # Перевіряємо синоніми
        for synonym in brand_info.synonyms:
            if name_lower == synonym.lower().strip():
                return True
        
        return False
    
    def track_candidate(self, name: str, region: str, category: str):
        """Відстежує кандидата на новий бренд"""
        if name not in self.brand_candidates:
            self.brand_candidates[name] = BrandCandidate(
                name=name,
                frequency=1,
                first_seen=datetime.now(),
                last_seen=datetime.now(),
                locations=[region],
                categories={category}
            )
        else:
            candidate = self.brand_candidates[name]
            candidate.frequency += 1
            candidate.last_seen = datetime.now()
            if region not in candidate.locations:
                candidate.locations.append(region)
            candidate.categories.add(category)
        
        self.stats['candidates_found'] += 1
        
        # Зберігаємо в БД якщо частота достатня
        candidate = self.brand_candidates[name]
        if candidate.frequency >= 5:  # Поріг для збереження
            self._save_candidate_to_db(candidate)
    
    def _save_candidate_to_db(self, candidate: BrandCandidate):
        """Зберігає кандидата в БД (ВИПРАВЛЕНА ВЕРСІЯ)"""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    # Спочатку перевіряємо чи існує
                    cur.execute("""
                        SELECT candidate_id, frequency 
                        FROM osm_ukraine.brand_candidates 
                        WHERE name = %s
                    """, (candidate.name,))
                    
                    existing = cur.fetchone()
                    
                    if existing:
                        # Оновлюємо існуючий запис
                        cur.execute("""
                            UPDATE osm_ukraine.brand_candidates 
                            SET frequency = frequency + %s,
                                last_seen = %s,
                                locations = array_cat(locations, %s::text[]),
                                categories = array_cat(categories, %s::text[])
                            WHERE name = %s
                        """, (
                            candidate.frequency,
                            candidate.last_seen,
                            list(candidate.locations),
                            list(candidate.categories),
                            candidate.name
                        ))
                    else:
                        # Створюємо новий запис
                        cur.execute("""
                            INSERT INTO osm_ukraine.brand_candidates 
                            (name, frequency, first_seen, last_seen, locations, categories, status)
                            VALUES (%s, %s, %s, %s, %s, %s, 'new')
                        """, (
                            candidate.name,
                            candidate.frequency,
                            candidate.first_seen,
                            candidate.last_seen,
                            list(candidate.locations),
                            list(candidate.categories)
                        ))
                    
                    conn.commit()
        except Exception as e:
            logger.error(f"Помилка збереження кандидата {candidate.name}: {e}")

    def get_candidates_for_review(
        self, 
        status: Optional[str] = None,
        min_frequency: Optional[int] = None,
        min_confidence: Optional[float] = None,
        category: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Отримує кандидатів для review з різними фільтрами
        
        Args:
            status: Фільтр по статусу ('new', 'recommended', 'reviewing')
            min_frequency: Мінімальна частота появи
            min_confidence: Мінімальна впевненість
            category: Фільтр по категорії
            limit: Максимальна кількість результатів
        
        Returns:
            Список кандидатів з усіма полями
        """
        candidates = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Будуємо динамічний запит
                    query = """
                        SELECT 
                            candidate_id, name, frequency, 
                            first_seen, last_seen,
                            locations, categories,
                            status, confidence_score,
                            suggested_canonical_name,
                            suggested_functional_group,
                            suggested_influence_weight,
                            suggested_format,
                            recommendation_reason,
                            reviewed_at, reviewed_by
                        FROM osm_ukraine.brand_candidates
                        WHERE 1=1
                    """
                    params = []
                    
                    if status:
                        query += " AND status = %s"
                        params.append(status)
                    
                    if min_frequency:
                        query += " AND frequency >= %s"
                        params.append(min_frequency)
                    
                    if min_confidence:
                        query += " AND confidence_score >= %s"
                        params.append(min_confidence)
                    
                    if category:
                        query += " AND %s = ANY(categories)"
                        params.append(category)
                    
                    query += " ORDER BY frequency DESC, confidence_score DESC NULLS LAST"
                    
                    if limit:
                        query += " LIMIT %s"
                        params.append(limit)
                    
                    cur.execute(query, params)
                    
                    for row in cur:
                        candidates.append(dict(row))
                        
        except Exception as e:
            logger.error(f"Помилка отримання кандидатів: {e}")
        
        return candidates

    def batch_approve_candidates(
        self,
        filters: Dict[str, Any],
        action: str = 'approve',
        processed_by: str = 'system',
        batch_id: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Batch approval/rejection кандидатів
        
        Args:
            filters: Фільтри для вибору кандидатів
                - status: статус кандидата
                - min_frequency: мінімальна частота
                - min_confidence: мінімальна впевненість
                - candidate_ids: список конкретних ID
            action: 'approve' або 'reject'
            processed_by: хто обробляє
            batch_id: ID batch операції
        
        Returns:
            Статистика обробки
        """
        if batch_id is None:
            batch_id = str(uuid.uuid4())
        
        stats = {
            'total_processed': 0,
            'approved': 0,
            'rejected': 0,
            'errors': 0
        }
        
        try:
            # Отримуємо кандидатів за фільтрами
            if 'candidate_ids' in filters:
                candidates = self._get_candidates_by_ids(filters['candidate_ids'])
            else:
                candidates = self.get_candidates_for_review(**filters)
            
            logger.info(f"Знайдено {len(candidates)} кандидатів для {action}")
            
            with psycopg2.connect(self.db_connection_string) as conn:
                for candidate in candidates:
                    try:
                        if action == 'approve':
                            success = self._approve_single_candidate(
                                conn, candidate, processed_by, batch_id
                            )
                            if success:
                                stats['approved'] += 1
                        elif action == 'reject':
                            success = self._reject_single_candidate(
                                conn, candidate, processed_by, batch_id
                            )
                            if success:
                                stats['rejected'] += 1
                        
                        stats['total_processed'] += 1
                        
                    except Exception as e:
                        logger.error(f"Помилка обробки кандидата {candidate['name']}: {e}")
                        stats['errors'] += 1
                
                # Логуємо batch операцію
                self._log_batch_operation(conn, batch_id, action, filters, stats, processed_by)
                
                conn.commit()
                
        except Exception as e:
            logger.error(f"Помилка batch approval: {e}")
        
        return stats

    def _approve_single_candidate(
        self, 
        conn, 
        candidate: Dict[str, Any], 
        processed_by: str,
        batch_id: str
    ) -> bool:
        """Затверджує одного кандидата"""
        try:
            cur = conn.cursor()
            
            # Створюємо запис в custom_brands
            brand_id = candidate['name'].lower().replace(' ', '_').replace("'", '')
            
            cur.execute("""
                INSERT INTO osm_ukraine.custom_brands (
                    brand_id, 
                    canonical_name, 
                    synonyms,
                    format, 
                    influence_weight, 
                    functional_group,
                    created_by,
                    source,
                    source_candidate_id,
                    confidence_score
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, 'auto_approved', %s, %s
                )
                ON CONFLICT (brand_id) DO UPDATE SET
                    updated_at = NOW(),
                    updated_by = EXCLUDED.created_by
            """, (
                brand_id,
                candidate.get('suggested_canonical_name') or candidate['name'],
                [candidate['name']],  # Оригінальна назва як синонім
                candidate.get('suggested_format', 'магазин'),
                candidate.get('suggested_influence_weight', -0.5),
                candidate.get('suggested_functional_group', 'competitor'),
                processed_by,
                candidate['candidate_id'],
                candidate.get('confidence_score', 0.5)
            ))
            
            # Оновлюємо статус кандидата
            cur.execute("""
                UPDATE osm_ukraine.brand_candidates
                SET status = 'approved',
                    reviewed_at = NOW(),
                    reviewed_by = %s,
                    approved_brand_id = %s,
                    batch_id = %s,
                    processed_at = NOW()
                WHERE candidate_id = %s
            """, (processed_by, brand_id, batch_id, candidate['candidate_id']))
            
            cur.close()
            
            logger.info(f"✅ Затверджено бренд: {candidate['name']} -> {brand_id}")
            return True
            
        except Exception as e:
            logger.error(f"Помилка затвердження {candidate['name']}: {e}")
            return False

    def _reject_single_candidate(
        self, 
        conn, 
        candidate: Dict[str, Any], 
        processed_by: str,
        batch_id: str
    ) -> bool:
        """Відхиляє одного кандидата"""
        try:
            cur = conn.cursor()
            
            cur.execute("""
                UPDATE osm_ukraine.brand_candidates
                SET status = 'rejected',
                    reviewed_at = NOW(),
                    reviewed_by = %s,
                    batch_id = %s,
                    processed_at = NOW(),
                    rejection_reason = %s
                WHERE candidate_id = %s
            """, (
                processed_by, 
                batch_id,
                candidate.get('rejection_reason', 'Rejected by batch processing'),
                candidate['candidate_id']
            ))
            
            cur.close()
            
            logger.info(f"❌ Відхилено кандидата: {candidate['name']}")
            return True
            
        except Exception as e:
            logger.error(f"Помилка відхилення {candidate['name']}: {e}")
            return False

    def _get_candidates_by_ids(self, candidate_ids: List[str]) -> List[Dict[str, Any]]:
        """Отримує кандидатів за списком ID"""
        candidates = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM osm_ukraine.brand_candidates
                        WHERE candidate_id = ANY(%s)
                    """, (candidate_ids,))
                    
                    for row in cur:
                        candidates.append(dict(row))
                        
        except Exception as e:
            logger.error(f"Помилка отримання кандидатів за ID: {e}")
        
        return candidates

    def _log_batch_operation(
        self, 
        conn, 
        batch_id: str, 
        action: str, 
        filters: Dict[str, Any], 
        stats: Dict[str, int],
        processed_by: str
    ):
        """Логує batch операцію"""
        try:
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO osm_ukraine.brand_approval_log (
                    batch_id, action, filters_used, 
                    candidates_processed, candidates_approved, candidates_rejected,
                    processed_by
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                batch_id,
                action,
                Json(filters),
                stats['total_processed'],
                stats.get('approved', 0),
                stats.get('rejected', 0),
                processed_by
            ))
            
            cur.close()
            
        except Exception as e:
            logger.error(f"Помилка логування batch операції: {e}")

    def get_batch_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Отримує історію batch операцій"""
        history = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT * FROM osm_ukraine.brand_approval_log
                        ORDER BY processed_at DESC
                        LIMIT %s
                    """, (limit,))
                    
                    for row in cur:
                        history.append(dict(row))
                        
        except Exception as e:
            logger.error(f"Помилка отримання історії: {e}")
        
        return history
    
    def approve_candidate(
        self,
        candidate_name: str,
        brand_id: str,
        canonical_name: str,
        synonyms: List[str] = None,
        format: str = "магазин",
        influence_weight: float = -0.5,
        functional_group: str = "competitor",
        approved_by: str = "admin"
    ) -> bool:
        """Затверджує кандидата як новий бренд"""
        # Додаємо бренд
        success = self.add_brand(
            brand_id=brand_id,
            canonical_name=canonical_name,
            synonyms=synonyms + [candidate_name],  # Додаємо оригінальну назву як синонім
            format=format,
            influence_weight=influence_weight,
            functional_group=functional_group,
            created_by=approved_by
        )
        
        if success:
            # Оновлюємо статус кандидата
            try:
                with psycopg2.connect(self.db_connection_string) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE osm_ukraine.brand_candidates
                            SET status = 'approved',
                                reviewed_at = NOW(),
                                reviewed_by = %s,
                                approved_brand_id = %s
                            WHERE name = %s
                        """, (approved_by, brand_id, candidate_name))
                        conn.commit()
                        
                logger.info(f"Кандидат '{candidate_name}' затверджено як '{canonical_name}'")
                return True
            except Exception as e:
                logger.error(f"Помилка оновлення статусу кандидата: {e}")
        
        return False
    
    def get_statistics(self) -> Dict[str, any]:
        """Повертає статистику"""
        stats = self.stats.copy()
        stats['total_brands'] = len(self.get_all_brands())
        stats['base_brands'] = len(self.brand_dict.brands)
        stats['custom_brands'] = len(self.custom_brands)
        stats['pending_candidates'] = len([c for c in self.brand_candidates.values() if c.frequency >= 5])
        
        return stats


# Приклад використання
if __name__ == "__main__":
    # Створюємо менеджер
    manager = BrandManager("postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail")
    
    # Додаємо Піцца Дей як приклад
    success = manager.add_brand(
        brand_id='pizza_day',
        canonical_name='Піцца Дей',
        synonyms=['Pizza Day', 'ПІЦЦА ДЕЙ', 'піцца дей', 'Pizza Dei'],
        format='піцерія',
        influence_weight=-0.35,  # Конкурент для супермаркетів
        functional_group='competitor',
        parent_company='Pizza Day LLC',
        osm_tags=['amenity=restaurant', 'cuisine=pizza'],
        created_by='client_request'
    )
    
    if success:
        print("✅ Бренд 'Піцца Дей' успішно додано!")
    
    # Демонстрація пошуку бренду
    result = manager.find_brand("Pizza Day")
    if result:
        brand_id, brand_info = result
        print(f"Знайдено: {brand_info.canonical_name} (вплив: {brand_info.influence_weight})")
    
    # Демонстрація роботи з кандидатами
    print("\n📋 Демонстрація batch operations:")
    
    # Отримання кандидатів для review
    candidates = manager.get_candidates_for_review(status='new', limit=5)
    if candidates:
        print(f"Знайдено {len(candidates)} нових кандидатів:")
        for candidate in candidates:
            print(f"  - {candidate['name']} (частота: {candidate['frequency']}, регіони: {len(candidate.get('locations', []))})")
    
    # Приклад batch approval
    if candidates:
        filters = {
            'status': 'new',
            'min_frequency': 10,
            'min_confidence': 0.8
        }
        
        stats = manager.batch_approve_candidates(
            filters=filters,
            action='approve',
            processed_by='admin_demo'
        )
        
        print(f"\n📊 Результат batch approval:")
        print(f"  Оброблено: {stats['total_processed']}")
        print(f"  Затверджено: {stats['approved']}")
        print(f"  Відхилено: {stats['rejected']}")
        print(f"  Помилки: {stats['errors']}")
    
    # Історія операцій
    history = manager.get_batch_history(limit=3)
    if history:
        print(f"\n📚 Остання історія operations:")
        for entry in history:
            print(f"  - {entry['action']} at {entry['processed_at']} by {entry['processed_by']}")
    
    # Загальна статистика
    stats = manager.get_statistics()
    print(f"\n📈 Статистика менеджера:")
    print(f"  Всього брендів: {stats['total_brands']}")
    print(f"  Базових брендів: {stats['base_brands']}")
    print(f"  Кастомних брендів: {stats['custom_brands']}")
    print(f"  Кандидатів в очікуванні: {stats['pending_candidates']}")
    print(f"  Додано брендів: {stats['brands_added']}")
    print(f"  Знайдено кандидатів: {stats['candidates_found']}")