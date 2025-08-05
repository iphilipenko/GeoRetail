"""
Brand Manager
Динамічне управління брендами з можливістю додавання нових
"""

import json
import logging
from typing import Dict, List, Optional, Set, Tuple
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
            logger.warning(f"Не вдалося завантажити бренди з БД: {e}")
        
        return custom_brands
    
    def _load_brands_from_db(self) -> Dict[str, BrandInfo]:
        """Завантажує кастомні бренди з БД"""
        brands = {}
        
        with psycopg2.connect(self.db_connection_string) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Перевіряємо чи існує таблиця
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'osm_ukraine' 
                        AND table_name = 'custom_brands'
                    );
                """)
                
                if not cur.fetchone()['exists']:
                    # Створюємо таблицю якщо не існує
                    self._create_brands_table(conn)
                    return brands
                
                # Завантажуємо бренди
                cur.execute("""
                    SELECT brand_id, canonical_name, synonyms, format, 
                           influence_weight, functional_group, parent_company,
                           osm_tags, created_at, updated_at, created_by
                    FROM osm_ukraine.custom_brands
                    WHERE is_active = true
                """)
                
                for row in cur:
                    brands[row['brand_id']] = BrandInfo(
                        canonical_name=row['canonical_name'],
                        synonyms=row['synonyms'] or [],
                        format=row['format'],
                        influence_weight=float(row['influence_weight']),
                        functional_group=row['functional_group'],
                        parent_company=row['parent_company'],
                        osm_tags=row['osm_tags']
                    )
        
        logger.info(f"Завантажено {len(brands)} кастомних брендів з БД")
        return brands
    
    def _create_brands_table(self, conn):
        """Створює таблицю для кастомних брендів"""
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS osm_ukraine.custom_brands (
                    brand_id VARCHAR(100) PRIMARY KEY,
                    canonical_name VARCHAR(200) NOT NULL,
                    synonyms TEXT[] DEFAULT '{}',
                    format VARCHAR(100) NOT NULL,
                    influence_weight DECIMAL(3,2) NOT NULL CHECK (influence_weight BETWEEN -1.0 AND 1.0),
                    functional_group VARCHAR(50) NOT NULL,
                    parent_company VARCHAR(200),
                    osm_tags TEXT[],
                    
                    -- Metadata
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    created_by VARCHAR(100) DEFAULT 'system',
                    updated_by VARCHAR(100),
                    is_active BOOLEAN DEFAULT true,
                    notes TEXT
                );
                
                CREATE INDEX idx_custom_brands_active ON osm_ukraine.custom_brands(is_active);
                CREATE INDEX idx_custom_brands_canonical ON osm_ukraine.custom_brands(canonical_name);
            """)
            
            # Таблиця для кандидатів
            cur.execute("""
                CREATE TABLE IF NOT EXISTS osm_ukraine.brand_candidates (
                    candidate_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    name VARCHAR(200) NOT NULL,
                    frequency INTEGER DEFAULT 1,
                    first_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    last_seen TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    locations TEXT[] DEFAULT '{}',
                    categories TEXT[] DEFAULT '{}',
                    
                    -- Suggestions
                    suggested_canonical_name VARCHAR(200),
                    suggested_functional_group VARCHAR(50),
                    suggested_influence_weight DECIMAL(3,2),
                    confidence_score DECIMAL(3,2) DEFAULT 0.0,
                    
                    -- Status
                    status VARCHAR(20) DEFAULT 'new' CHECK (status IN ('new', 'reviewing', 'approved', 'rejected')),
                    reviewed_at TIMESTAMP WITH TIME ZONE,
                    reviewed_by VARCHAR(100),
                    rejection_reason TEXT,
                    
                    -- Зв'язок з доданим брендом
                    approved_brand_id VARCHAR(100) REFERENCES osm_ukraine.custom_brands(brand_id)
                );
                
                CREATE INDEX idx_brand_candidates_name ON osm_ukraine.brand_candidates(name);
                CREATE INDEX idx_brand_candidates_status ON osm_ukraine.brand_candidates(status);
                CREATE INDEX idx_brand_candidates_frequency ON osm_ukraine.brand_candidates(frequency DESC);
            """)
            
            conn.commit()
            logger.info("Створено таблиці для управління брендами")
    
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
        """Перевіряє чи назва відповідає бренду"""
        normalized_name = self._normalize_name(name)
        
        # Перевірка канонічної назви
        if normalized_name == self._normalize_name(brand_info.canonical_name):
            return True
        
        # Перевірка синонімів
        for synonym in brand_info.synonyms:
            if normalized_name == self._normalize_name(synonym):
                return True
        
        return False
    
    def _normalize_name(self, name: str) -> str:
        """Нормалізує назву"""
        if not name:
            return ""
        return name.lower().strip().replace("'", "").replace('"', '')
    
    def record_unknown_brand(
        self, 
        name: str, 
        region: str,
        category: Optional[str] = None,
        osm_tags: Optional[Dict[str, str]] = None
    ):
        """Записує невідомий бренд як кандидата"""
        if not name or len(name) < 3:
            return
        
        # Нормалізуємо назву
        normalized = name.strip()
        
        # Оновлюємо або створюємо кандидата
        if normalized in self.brand_candidates:
            candidate = self.brand_candidates[normalized]
            candidate.frequency += 1
            candidate.last_seen = datetime.now()
            if region not in candidate.locations:
                candidate.locations.append(region)
            if category and category not in candidate.categories:
                candidate.categories.add(category)
        else:
            candidate = BrandCandidate(
                name=normalized,
                frequency=1,
                first_seen=datetime.now(),
                last_seen=datetime.now(),
                locations=[region],
                categories={category} if category else set()
            )
            self.brand_candidates[normalized] = candidate
            self.stats['candidates_found'] += 1
        
        # Зберігаємо в БД якщо частота достатня
        if candidate.frequency >= 5:  # Поріг для збереження
            self._save_candidate_to_db(candidate)
    
    def _save_candidate_to_db(self, candidate: BrandCandidate):
        """Зберігає кандидата в БД"""
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO osm_ukraine.brand_candidates 
                        (name, frequency, first_seen, last_seen, locations, categories)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (name) DO UPDATE SET
                            frequency = EXCLUDED.frequency,
                            last_seen = EXCLUDED.last_seen,
                            locations = array_cat(brand_candidates.locations, EXCLUDED.locations),
                            categories = array_cat(brand_candidates.categories, EXCLUDED.categories)
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
            logger.error(f"Помилка збереження кандидата: {e}")
    
    def get_brand_candidates(self, min_frequency: int = 10) -> List[BrandCandidate]:
        """Повертає кандидатів на нові бренди"""
        candidates = []
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT name, frequency, first_seen, last_seen, 
                               locations, categories, confidence_score
                        FROM osm_ukraine.brand_candidates
                        WHERE frequency >= %s
                        AND status = 'new'
                        ORDER BY frequency DESC
                    """, (min_frequency,))
                    
                    for row in cur:
                        candidates.append(BrandCandidate(
                            name=row['name'],
                            frequency=row['frequency'],
                            first_seen=row['first_seen'],
                            last_seen=row['last_seen'],
                            locations=row['locations'] or [],
                            categories=set(row['categories'] or []),
                            confidence_score=float(row['confidence_score'] or 0)
                        ))
        except Exception as e:
            logger.error(f"Помилка отримання кандидатів: {e}")
        
        return candidates
    
    def approve_candidate(
        self,
        candidate_name: str,
        brand_id: str,
        canonical_name: str,
        synonyms: List[str],
        format: str,
        influence_weight: float,
        functional_group: str,
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
    
    # Пошук бренду
    result = manager.find_brand("Pizza Day")
    if result:
        brand_id, brand_info = result
        print(f"Знайдено: {brand_info.canonical_name} (вплив: {brand_info.influence_weight})")
    
    # Перевірка кандидатів
    candidates = manager.get_brand_candidates(min_frequency=5)
    if candidates:
        print(f"\n📋 Знайдено {len(candidates)} кандидатів на нові бренди:")
        for candidate in candidates[:5]:
            print(f"  - {candidate.name} (зустрічається {candidate.frequency} разів)")
    
    # Статистика
    stats = manager.get_statistics()
    print(f"\n📊 Статистика:")
    print(f"  Всього брендів: {stats['total_brands']}")
    print(f"  Кастомних: {stats['custom_brands']}")
    print(f"  Кандидатів: {stats['pending_candidates']}")