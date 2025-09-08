#!/usr/bin/env python3
"""
import_avrora_auto_fix.py
Скрипт імпорту з автоматичним виправленням проблем з H3 тригерами
"""

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from datetime import datetime
import re
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import sys
import json

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('import_avrora.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Креди проекту
DB_CONFIG = {
    'host': 'localhost',
    'database': 'georetail',
    'user': 'georetail_user',
    'password': 'georetail_secure_2024',
    'port': 5432
}

# Шляхи до файлів
DATA_DIR = Path('C:/projects/Avrora')
FILES = {
    'stores': DATA_DIR / 'Avrora.csv',
    'isochrones': DATA_DIR / 'avrora_isochrone_1.csv',
    'competitors': DATA_DIR / 'Competitors.csv'
}

# Мапа брендів
COMPETITOR_BRANDS = {
    'АТБ': ['АТБ', 'ATB', 'АТБ-Маркет'],
    'Сільпо': ['Сільпо', 'Silpo', 'СІЛЬПО'],
    'Наша Ряба': ['Наша Ряба', 'Наша Рукавичка', 'Ряба'],
    'Фора': ['Фора', 'Fora', 'ФОРА'],
    'Novus': ['Novus', 'Новус', 'NOVUS'],
    'Varus': ['Varus', 'Варус', 'VARUS'],
    'Еко-маркет': ['Еко', 'Eko', 'Еко-маркет', 'ЕКО'],
    'Коло': ['Коло', 'КОЛО'],
    'Фуршет': ['Фуршет', 'ФУРШЕТ'],
    'Metro': ['Metro', 'Метро', 'METRO'],
    'Ашан': ['Ашан', 'Auchan', 'АШАН'],
    'Велмарт': ['Велмарт', 'Velmart', 'ВЕЛМАРТ'],
    'Велика Кишеня': ['Велика Кишеня', 'ВК'],
}


class AvroraDataImporter:
    """Клас для імпорту даних мережі Avrora з автовиправленням"""
    
    def __init__(self):
        self.db_config = DB_CONFIG
        self.conn = None
        self.cur = None
        self.errors_log = []
        self.connect()
        self.fix_h3_triggers()  # Автоматично виправляємо проблеми
    
    def connect(self):
        """Підключення до БД"""
        try:
            logger.info("[CONNECTING] Connecting to database...")
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("[OK] Connected successfully")
        except psycopg2.Error as e:
            logger.error(f"[ERROR] Connection failed: {e}")
            raise
    
    def fix_h3_triggers(self):
        """Автоматичне виправлення проблем з H3 тригерами"""
        logger.info("[FIX] Checking and fixing H3 triggers...")
        
        try:
            # 1. Видаляємо проблемні тригери
            self.cur.execute("""
                DROP TRIGGER IF EXISTS trg_fill_geo_attributes ON avrora.stores CASCADE;
                DROP TRIGGER IF EXISTS before_insert_stores ON avrora.stores CASCADE;
                DROP TRIGGER IF EXISTS before_update_stores ON avrora.stores CASCADE;
            """)
            
            # 2. Видаляємо старі функції
            self.cur.execute("""
                DROP FUNCTION IF EXISTS avrora.fill_geo_attributes() CASCADE;
            """)
            
            # 3. Видаляємо H3 колонки якщо вони є (бо H3 може бути не встановлений)
            self.cur.execute("""
                ALTER TABLE avrora.stores DROP COLUMN IF EXISTS h3_7 CASCADE;
                ALTER TABLE avrora.stores DROP COLUMN IF EXISTS h3_8 CASCADE;
                ALTER TABLE avrora.stores DROP COLUMN IF EXISTS h3_9 CASCADE;
            """)
            
            # 4. Додаємо geom колонку якщо її немає
            self.cur.execute("""
                ALTER TABLE avrora.stores 
                ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);
            """)
            
            # 5. Створюємо простий тригер для геометрії
            self.cur.execute("""
                CREATE OR REPLACE FUNCTION avrora.update_geom()
                RETURNS TRIGGER AS $$
                BEGIN
                    IF NEW.lat IS NOT NULL AND NEW.lon IS NOT NULL THEN
                        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                
                CREATE TRIGGER trg_update_geom
                BEFORE INSERT OR UPDATE OF lat, lon ON avrora.stores
                FOR EACH ROW
                EXECUTE FUNCTION avrora.update_geom();
            """)
            
            self.conn.commit()
            logger.info("[OK] H3 triggers fixed successfully")
            
        except psycopg2.Error as e:
            logger.warning(f"[WARNING] Could not fix triggers: {e}")
            self.conn.rollback()
    
    def disconnect(self):
        """Відключення від БД"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info("[DISCONNECT] Disconnected from database")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.save_errors_log()
        self.disconnect()
    
    def save_errors_log(self):
        """Збереження логу помилок"""
        if self.errors_log:
            error_file = f"import_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(self.errors_log, f, ensure_ascii=False, indent=2, default=str)
            logger.warning(f"[WARNING] Errors saved to {error_file}")
    
    @staticmethod
    def validate_coordinates(lat: float, lon: float) -> bool:
        """Валідація координат для України"""
        if lat is None or lon is None:
            return False
        return 44 <= lat <= 52 and 22 <= lon <= 40
    
    @staticmethod
    def clean_numeric(value) -> Optional[float]:
        """Очищення числового значення"""
        if pd.isna(value):
            return None
        
        cleaned = str(value).replace(',', '.').replace(' ', '')
        
        try:
            result = float(cleaned)
            if abs(result) > 1e10:
                return None
            return result
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def parse_date(date_str: str) -> Optional[datetime]:
        """Парсинг дати"""
        if pd.isna(date_str):
            return None
        
        formats = [
            '%d.%m.%Y', '%Y-%m-%d', '%d/%m/%Y',
            '%d-%m-%Y', '%Y/%m/%d'
        ]
        
        date_str = str(date_str).strip()
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        return None
    
    @staticmethod
    def extract_coords_from_point(point_str: str) -> Optional[Tuple[float, float]]:
        """Витягування координат з POINT WKT"""
        if pd.isna(point_str):
            return None
        
        match = re.search(r'POINT\s*\(\s*([\d.-]+)\s+([\d.-]+)\s*\)', str(point_str))
        if match:
            try:
                lon = float(match.group(1))
                lat = float(match.group(2))
                return (lon, lat) if AvroraDataImporter.validate_coordinates(lat, lon) else None
            except ValueError:
                pass
        
        return None
    
    def extract_brand(self, name: str) -> str:
        """Визначення бренду конкурента"""
        if pd.isna(name):
            return 'Інші'
        
        name_upper = str(name).upper()
        
        for brand, patterns in COMPETITOR_BRANDS.items():
            for pattern in patterns:
                if pattern.upper() in name_upper:
                    return brand
        
        return 'Інші'
    
    def import_stores(self, csv_path: Path) -> int:
        """Імпорт магазинів"""
        logger.info(f"\n[IMPORT] Importing stores from {csv_path.name}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"File not found: {csv_path}")
        
        df = pd.read_csv(csv_path, encoding='utf-8')
        logger.info(f"  Read {len(df)} records")
        
        stores_data = []
        skipped = 0
        
        for idx, row in df.iterrows():
            try:
                # Витягуємо координати
                coords = self.extract_coords_from_point(row.get('geometry'))
                if coords:
                    lon, lat = coords
                else:
                    lon = self.clean_numeric(row.get('lon'))
                    lat = self.clean_numeric(row.get('lat'))
                
                if not self.validate_coordinates(lat, lon):
                    skipped += 1
                    continue
                
                store_data = {
                    'shop_id': str(row.get('shop_id', '')),
                    'name': f"Avrora #{row.get('shop_id', '')}",
                    'oblast': row.get('oblast'),
                    'city': row.get('city'),
                    'street': row.get('street'),
                    'opening_date': self.parse_date(row.get('opening_date')),
                    'format': row.get('format'),
                    'lat': lat,
                    'lon': lon,
                    'square_trade': self.clean_numeric(row.get('square_trade')),
                    'shop_square': self.clean_numeric(row.get('shop_square')),
                    'population_x10k': self.clean_numeric(row.get('population_x10K', row.get('population_x10k'))),
                    'avg_month_n_checks': self.clean_numeric(row.get('avg_month_n_checks')),
                    'population_h3_8': int(row.get('population_h3', 0)) if pd.notna(row.get('population_h3')) else None,
                    'neighbor_population_h3_8': int(row.get('neighbor_population_sum_h3', 0)) if pd.notna(row.get('neighbor_population_sum_h3')) else None
                }
                
                stores_data.append(store_data)
                
            except Exception as e:
                self.errors_log.append({'file': 'stores', 'row': idx, 'error': str(e)})
                skipped += 1
        
        # Вставка даних
        insert_query = """
            INSERT INTO avrora.stores (
                shop_id, name, oblast, city, street, opening_date, format,
                lat, lon, square_trade, shop_square, population_x10k,
                avg_month_n_checks, population_h3_8, neighbor_population_h3_8
            ) VALUES (
                %(shop_id)s, %(name)s, %(oblast)s, %(city)s, %(street)s,
                %(opening_date)s, %(format)s, %(lat)s, %(lon)s,
                %(square_trade)s, %(shop_square)s, %(population_x10k)s,
                %(avg_month_n_checks)s, %(population_h3_8)s, %(neighbor_population_h3_8)s
            )
            ON CONFLICT (shop_id) DO UPDATE SET
                name = EXCLUDED.name,
                oblast = EXCLUDED.oblast,
                city = EXCLUDED.city,
                street = EXCLUDED.street,
                opening_date = EXCLUDED.opening_date,
                format = EXCLUDED.format,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                square_trade = EXCLUDED.square_trade,
                shop_square = EXCLUDED.shop_square,
                population_x10k = EXCLUDED.population_x10k,
                avg_month_n_checks = EXCLUDED.avg_month_n_checks,
                population_h3_8 = EXCLUDED.population_h3_8,
                neighbor_population_h3_8 = EXCLUDED.neighbor_population_h3_8,
                updated_at = CURRENT_TIMESTAMP
        """
        
        try:
            execute_batch(self.cur, insert_query, stores_data, page_size=100)
            self.conn.commit()
            
            # Оновлюємо геометрію
            self.cur.execute("""
                UPDATE avrora.stores 
                SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326)
                WHERE geom IS NULL AND lat IS NOT NULL AND lon IS NOT NULL;
            """)
            self.conn.commit()
            
            logger.info(f"[OK] Imported {len(stores_data)} stores")
            if skipped > 0:
                logger.warning(f"[WARNING] Skipped {skipped} invalid records")
            return len(stores_data)
            
        except psycopg2.Error as e:
            self.conn.rollback()
            logger.error(f"[ERROR] Store import failed: {e}")
            raise
    
    def import_store_isochrones(self, csv_path: Path) -> int:
        """Імпорт ізохрон магазинів"""
        logger.info(f"\n[IMPORT] Importing isochrones from {csv_path.name}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"File not found: {csv_path}")
        
        df = pd.read_csv(csv_path, encoding='utf-8')
        logger.info(f"  Read {len(df)} records")
        
        isochrones_data = []
        skipped = 0
        
        for _, row in df.iterrows():
            try:
                shop_id = str(row.get('identifier', ''))
                lat = self.clean_numeric(row.get('lat'))
                lon = self.clean_numeric(row.get('lon'))
                
                if not shop_id or not self.validate_coordinates(lat, lon):
                    skipped += 1
                    continue
                
                # Отримуємо store_id
                self.cur.execute(
                    "SELECT store_id FROM avrora.stores WHERE shop_id = %s",
                    (shop_id,)
                )
                result = self.cur.fetchone()
                
                if not result:
                    skipped += 1
                    continue
                
                store_id = result['store_id']
                
                # Обробляємо полігони
                isochrone_configs = [
                    ('walk', 400, 'polygon_400'),
                    ('drive', 1500, 'polygon_drive')
                ]
                
                for mode, distance, polygon_col in isochrone_configs:
                    if polygon_col not in row or pd.isna(row[polygon_col]):
                        continue
                    
                    polygon_wkt = str(row[polygon_col])
                    
                    isochrone_data = {
                        'entity_type': 'store',
                        'entity_id': store_id,
                        'mode': mode,
                        'distance_meters': distance,
                        'polygon': polygon_wkt,
                        'center_lat': lat,
                        'center_lon': lon,
                        'bbox_min_lon': None,
                        'bbox_min_lat': None,
                        'bbox_max_lon': None,
                        'bbox_max_lat': None
                    }
                    
                    isochrones_data.append(isochrone_data)
                    
            except Exception as e:
                self.errors_log.append({'file': 'isochrones', 'row': _, 'error': str(e)})
                skipped += 1
        
        # Вставка ізохрон
        if isochrones_data:
            insert_query = """
                INSERT INTO avrora.isochrones (
                    entity_type, entity_id, mode, distance_meters,
                    polygon, center_lat, center_lon,
                    bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon,
                    area_sqm
                ) VALUES (
                    %(entity_type)s, %(entity_id)s, %(mode)s, %(distance_meters)s,
                    ST_GeomFromText(%(polygon)s, 4326),
                    %(center_lat)s, %(center_lon)s,
                    %(bbox_min_lat)s, %(bbox_min_lon)s,
                    %(bbox_max_lat)s, %(bbox_max_lon)s,
                    ST_Area(ST_GeomFromText(%(polygon)s, 4326)::geography)
                )
                ON CONFLICT (entity_type, entity_id, mode, distance_meters, is_current) 
                DO UPDATE SET
                    polygon = EXCLUDED.polygon,
                    area_sqm = EXCLUDED.area_sqm,
                    calculation_date = CURRENT_DATE
            """
            
            try:
                execute_batch(self.cur, insert_query, isochrones_data, page_size=50)
                self.conn.commit()
                logger.info(f"[OK] Imported {len(isochrones_data)} isochrones")
            except psycopg2.Error as e:
                self.conn.rollback()
                logger.error(f"[ERROR] Isochrones import failed: {e}")
                
        if skipped > 0:
            logger.warning(f"[WARNING] Skipped {skipped} records")
        
        return len(isochrones_data)
    
    def import_competitors(self, csv_path: Path) -> int:
        """Імпорт конкурентів"""
        logger.info(f"\n[IMPORT] Importing competitors from {csv_path.name}")
        
        if not csv_path.exists():
            raise FileNotFoundError(f"File not found: {csv_path}")
        
        df = pd.read_csv(csv_path, encoding='utf-8')
        logger.info(f"  Read {len(df)} records")
        
        competitors_count = 0
        isochrones_data = []
        skipped = 0
        
        for idx, row in df.iterrows():
            try:
                coords = self.extract_coords_from_point(row.get('geometry'))
                if not coords:
                    skipped += 1
                    continue
                
                lon, lat = coords
                name = str(row.get('conc_name', ''))
                brand = self.extract_brand(name)
                
                # Вставляємо конкурента
                insert_query = """
                    INSERT INTO avrora.competitors (name, brand, lat, lon)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (lat, lon) 
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        brand = EXCLUDED.brand,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING competitor_id
                """
                
                self.cur.execute(insert_query, (name, brand, lat, lon))
                competitor_id = self.cur.fetchone()['competitor_id']
                competitors_count += 1
                
                # Обробляємо ізохрону
                if 'polygon_conc_400' in row and pd.notna(row['polygon_conc_400']):
                    polygon_wkt = str(row['polygon_conc_400'])
                    
                    isochrone_data = {
                        'entity_type': 'competitor',
                        'entity_id': competitor_id,
                        'mode': 'walk',
                        'distance_meters': 400,
                        'polygon': polygon_wkt,
                        'center_lat': lat,
                        'center_lon': lon,
                        'bbox_min_lon': None,
                        'bbox_min_lat': None,
                        'bbox_max_lon': None,
                        'bbox_max_lat': None
                    }
                    
                    isochrones_data.append(isochrone_data)
                    
            except Exception as e:
                self.conn.rollback()
                skipped += 1
        
        # Вставка ізохрон конкурентів
        if isochrones_data:
            insert_query = """
                INSERT INTO avrora.isochrones (
                    entity_type, entity_id, mode, distance_meters,
                    polygon, center_lat, center_lon,
                    bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon,
                    area_sqm
                ) VALUES (
                    %(entity_type)s, %(entity_id)s, %(mode)s, %(distance_meters)s,
                    ST_GeomFromText(%(polygon)s, 4326),
                    %(center_lat)s, %(center_lon)s,
                    %(bbox_min_lat)s, %(bbox_min_lon)s,
                    %(bbox_max_lat)s, %(bbox_max_lon)s,
                    ST_Area(ST_GeomFromText(%(polygon)s, 4326)::geography)
                )
                ON CONFLICT (entity_type, entity_id, mode, distance_meters, is_current) 
                DO NOTHING
            """
            
            try:
                execute_batch(self.cur, insert_query, isochrones_data, page_size=50)
                self.conn.commit()
                logger.info(f"[OK] Imported {len(isochrones_data)} competitor isochrones")
            except psycopg2.Error as e:
                self.conn.rollback()
                logger.error(f"[ERROR] Competitor isochrones import failed: {e}")
        
        # Статистика
        self.cur.execute("SELECT COUNT(*) as cnt FROM avrora.competitors")
        total_competitors = self.cur.fetchone()['cnt']
        
        logger.info(f"[OK] Added {competitors_count} new competitors")
        logger.info(f"[OK] Total competitors in DB: {total_competitors}")
        if skipped > 0:
            logger.warning(f"[WARNING] Skipped {skipped} records")
        
        return total_competitors
    
    def get_statistics(self):
        """Статистика імпорту"""
        logger.info("\n" + "="*60)
        logger.info("IMPORT STATISTICS")
        logger.info("="*60)
        
        # Магазини
        self.cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT format) as formats,
                COUNT(DISTINCT city) as cities
            FROM avrora.stores
            WHERE is_active = true
        """)
        stores = self.cur.fetchone()
        
        logger.info("\n[STORES]:")
        logger.info(f"  Total: {stores['total']}")
        logger.info(f"  Formats: {stores['formats']}")
        logger.info(f"  Cities: {stores['cities']}")
        
        # Конкуренти
        self.cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT brand) as brands
            FROM avrora.competitors
            WHERE is_active = true
        """)
        comp = self.cur.fetchone()
        
        logger.info("\n[COMPETITORS]:")
        logger.info(f"  Total: {comp['total']}")
        logger.info(f"  Brands: {comp['brands']}")
        
        # Ізохрони
        self.cur.execute("""
            SELECT 
                entity_type,
                mode,
                distance_meters,
                COUNT(*) as count
            FROM avrora.isochrones
            WHERE is_current = true
            GROUP BY entity_type, mode, distance_meters
            ORDER BY entity_type, mode, distance_meters
        """)
        
        logger.info("\n[ISOCHRONES]:")
        for row in self.cur.fetchall():
            logger.info(f"  {row['entity_type']:<12} {row['mode']:<6} {row['distance_meters']:>5}m: {row['count']}")
        
        logger.info("="*60)


def main():
    """Головна функція"""
    logger.info("="*60)
    logger.info("AVRORA DATA IMPORT WITH AUTO-FIX")
    logger.info("="*60)
    
    # Перевірка файлів
    missing_files = []
    for file_type, file_path in FILES.items():
        if not file_path.exists():
            missing_files.append(str(file_path))
        else:
            logger.info(f"[OK] Found {file_type}: {file_path.name}")
    
    if missing_files:
        logger.error("[ERROR] Missing files:")
        for f in missing_files:
            logger.error(f"  - {f}")
        sys.exit(1)
    
    try:
        with AvroraDataImporter() as importer:
            # Імпорт даних
            logger.info("\n[STAGE 1/3] Importing stores...")
            importer.import_stores(FILES['stores'])
            
            logger.info("\n[STAGE 2/3] Importing isochrones...")
            importer.import_store_isochrones(FILES['isochrones'])
            
            logger.info("\n[STAGE 3/3] Importing competitors...")
            importer.import_competitors(FILES['competitors'])
            
            # Статистика
            importer.get_statistics()
            
            logger.info("\n[SUCCESS] IMPORT COMPLETED!")
            
    except Exception as e:
        logger.error(f"\n[FAILED] Import failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()