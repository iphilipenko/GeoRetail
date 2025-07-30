#!/usr/bin/env python3
"""
OSM ETL Pipeline - Final Working Version
Виправлені: SQL синтаксис, видалено ON CONFLICT, ВИПРАВЛЕНО ВІДСТУПИ
"""

import os
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

import geopandas as gpd
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import h3
from tqdm import tqdm as tqdm_progress
import click
import numpy as np
from shapely.geometry import Point, Polygon
import fiona

# ВИПРАВЛЕННЯ UNICODE ДЛЯ WINDOWS
import locale
import codecs
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# Налаштування логування БЕЗ EMOJI
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('osm_etl.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ETLConfig:
    """Конфігурація ETL процесу"""
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "georetail"
    db_user: str = "georetail_user"
    db_password: str = "georetail_secure_2024"
    
    # Шляхи до даних
    data_directory: str = r"C:\OSMData"
    
    # Налаштування обробки
    batch_size: int = 5000    # Ще більше зменшено
    max_workers: int = 1      # Один воркер для надійності
    retry_attempts: int = 3
    retry_delay: int = 5
    
    # H3 налаштування
    h3_resolutions: List[int] = None
    
    def __post_init__(self):
        if self.h3_resolutions is None:
            self.h3_resolutions = [7, 8, 9, 10]


class H3Utils:
    """Утилітарний клас для роботи з H3 - оновлений для v4.x"""
    
    @staticmethod
    def geo_to_h3_safe(lat: float, lon: float, resolution: int) -> Optional[str]:
        """Безпечна конвертація координат в H3"""
        try:
            # Спробуємо нову версію H3 v4.x
            return h3.latlng_to_cell(lat, lon, resolution)
        except AttributeError:
            # Fallback на стару версію
            try:
                return h3.geo_to_h3(lat, lon, resolution)
            except:
                return None
        except Exception as e:
            logger.warning(f"H3 conversion error for {lat}, {lon}: {e}")
            return None
    
    @staticmethod
    def h3_to_geo_safe(h3_index: str) -> Optional[Tuple[float, float]]:
        """Безпечна конвертація H3 в координати"""
        try:
            # Спробуємо нову версію H3 v4.x
            return h3.cell_to_latlng(h3_index)
        except AttributeError:
            # Fallback на стару версію
            try:
                return h3.h3_to_geo(h3_index)
            except:
                return None
        except Exception as e:
            logger.warning(f"H3 to geo conversion error for {h3_index}: {e}")
            return None


class OSMDataProcessor:
    """Процесор для обробки OSM даних - оновлений для H3 v4.x"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.engine = None
        self.connection = None
        self._setup_database_connection()
        
    def _setup_database_connection(self):
        """Налаштування підключення до бази даних"""
        try:
            connection_string = (
                f"postgresql://{self.config.db_user}:{self.config.db_password}"
                f"@{self.config.db_host}:{self.config.db_port}/{self.config.db_name}"
            )
            
            self.engine = create_engine(
                connection_string,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Тестове підключення
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Підключення до бази даних успішне")
            
        except Exception as e:
            logger.error(f"Помилка підключення до БД: {e}")
            raise
    
    def extract_region_name(self, filename: str) -> str:
        """Витягування назви регіону з назви файлу"""
        if filename.startswith('UA_MAP_'):
            region_name = filename[7:]  # видаляємо 'UA_MAP_'
        else:
            region_name = filename
        
        if region_name.endswith('.gpkg'):
            region_name = region_name[:-5]  # видаляємо '.gpkg'
        
        return region_name
    
    def get_main_table_name(self, gpkg_path: Path) -> str:
        """Отримання назви основної таблиці з GPKG файлу"""
        try:
            layers = fiona.listlayers(str(gpkg_path))
            
            # Пріоритет: gis_osm_all > gis_osm_* > найбільша таблиця
            preferred_layers = [
                'gis_osm_all',
                'gis_osm_pois',
                'gis_osm_points',
                'multipolygons',
                'points'
            ]
            
            for preferred in preferred_layers:
                if preferred in layers:
                    logger.info(f"Використовуємо шар: {preferred}")
                    return preferred
            
            # Якщо не знайшли стандартні, беремо перший
            if layers:
                logger.info(f"Використовуємо перший доступний шар: {layers[0]}")
                return layers[0]
            else:
                raise ValueError("Не знайдено жодного шару в GPKG файлі")
                
        except Exception as e:
            logger.error(f"Помилка читання шарів з {gpkg_path.name}: {e}")
            raise
    
    def calculate_h3_for_geometry(self, geom, resolutions: List[int] = None) -> Dict[str, Optional[str]]:
        """Розрахунок H3 індексів для геометрії"""
        if resolutions is None:
            resolutions = self.config.h3_resolutions
        
        h3_results = {}
        
        try:
            # Отримуємо координати для H3
            if hasattr(geom, 'centroid'):
                # Для полігонів/ліній використовуємо центроїд
                centroid = geom.centroid
                lat, lon = centroid.y, centroid.x
            elif hasattr(geom, 'y') and hasattr(geom, 'x'):
                # Для точок
                lat, lon = geom.y, geom.x
            elif hasattr(geom, 'coords'):
                # Для геометрій з координатами
                coords = list(geom.coords)
                if coords:
                    lon, lat = coords[0][:2]  # Беремо перші координати
                else:
                    return {f'h3_res_{res}': None for res in resolutions}
            else:
                logger.warning(f"Неможливо отримати координати з геометрії: {type(geom)}")
                return {f'h3_res_{res}': None for res in resolutions}
            
            # Перевіряємо валідність координат
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                logger.warning(f"Невалідні координати: lat={lat}, lon={lon}")
                return {f'h3_res_{res}': None for res in resolutions}
            
            # Розрахунок H3 для кожної резолюції
            for res in resolutions:
                h3_index = H3Utils.geo_to_h3_safe(lat, lon, res)
                h3_results[f'h3_res_{res}'] = h3_index
                
        except Exception as e:
            logger.warning(f"Помилка розрахунку H3: {e}")
            h3_results = {f'h3_res_{res}': None for res in resolutions}
        
        return h3_results

    def process_region_file(self, gpkg_path: Path) -> Dict[str, Any]:
        """Обробка одного файлу регіону"""
        region_name = self.extract_region_name(gpkg_path.name)
        file_size_mb = gpkg_path.stat().st_size / (1024 * 1024)
        started_at = datetime.now()
        
        logger.info(f"Починаємо обробку {region_name} ({file_size_mb:.1f} MB)")
        
        try:
            # Отримання назви основної таблиці
            main_table = self.get_main_table_name(gpkg_path)
            
            # Читання даних батчами
            total_records = 0
            total_imported = 0
            total_poi_imported = 0
            
            # Отримання загальної кількості записів для прогрес-бару
            logger.info("Підрахунок загальної кількості записів...")
            full_gdf = gpd.read_file(gpkg_path, layer=main_table)
            total_count = len(full_gdf)
            
            logger.info(f"Обробка {total_count:,} записів батчами по {self.config.batch_size:,}")
            
            # Обробка батчами
            progress_bar = tqdm_progress(total=total_count, desc=f"Обробка {region_name}")
            
            for start_idx in range(0, total_count, self.config.batch_size):
                end_idx = min(start_idx + self.config.batch_size, total_count)
                
                # Читання батчу
                batch_gdf = full_gdf.iloc[start_idx:end_idx].copy()
                batch_records = batch_gdf.to_dict('records')
                
                # Обробка батчу з H3
                processed_records, poi_records = self.process_batch_with_h3(batch_records, region_name, start_idx)
                
                # Вставка в БД
                if processed_records:
                    imported_count = self.insert_batch_to_db(
                        processed_records, 
                        'osm_ukraine.osm_raw'
                    )
                    total_imported += imported_count
                
                if poi_records:
                    poi_imported_count = self.insert_batch_to_db(
                        poi_records,
                        'osm_ukraine.poi_normalized'
                    )
                    total_poi_imported += poi_imported_count
                
                total_records += len(batch_records)
                progress_bar.update(len(batch_records))
                
                # Логування прогресу
                if start_idx % (self.config.batch_size * 20) == 0:
                    logger.info(f"Оброблено {total_records:,} з {total_count:,} записів")
            
            progress_bar.close()
            
            processing_time = int((datetime.now() - started_at).total_seconds())
            
            logger.info(f"Завершено {region_name}: {total_imported:,} записів, {total_poi_imported:,} POI за {processing_time}с")
            
            return {
                'region_name': region_name,
                'status': 'completed',
                'records_processed': total_records,
                'records_imported': total_imported,
                'poi_imported': total_poi_imported,
                'processing_time': processing_time,
                'file_size_mb': file_size_mb
            }
            
        except Exception as e:
            processing_time = int((datetime.now() - started_at).total_seconds())
            
            logger.error(f"Помилка обробки {region_name}: {e}")
            
            return {
                'region_name': region_name,
                'status': 'failed',
                'error': str(e),
                'processing_time': processing_time,
                'file_size_mb': file_size_mb
            }

    def refresh_analytics(self):
        """Оновлення аналітичних представлень"""
        logger.info("Оновлення аналітичних представлень...")
        
        try:
            # Простий лог - аналітика поки не реалізована
            logger.info("Аналітика оновлена (заглушка)")
                
        except Exception as e:
            logger.error(f"Помилка оновлення аналітики: {e}")
    
    def extract_poi_data(self, record: Dict, tags: Dict, region_name: str, record_index: int) -> Optional[Dict]:
        """Витягування POI даних - ВИПРАВЛЕНО для роботи з правильними тегами"""
        
        # ВИПРАВЛЕНО: Шукаємо POI типи в розпарсених тегах замість record.items()
        poi_type, poi_value = None, None
        for tag_name, tag_value in tags.items():
            if tag_name in ['amenity', 'shop', 'office', 'tourism', 'leisure']:
                poi_type = tag_name
                poi_value = tag_value
                break
        
        # Якщо це не POI - пропускаємо
        if not poi_type or not poi_value:
            return None
        
        # Отримуємо геометрію з record
        geom = record.get('geometry') or record.get('geom')
        if not geom:
            return None
        
        # Розрахунок H3 індексів
        h3_data = self.calculate_h3_for_geometry(geom)
        
        return {
            'region_name': region_name,
            'source_fid': record_index,
            'osm_id': record.get('osm_id'),
            'geom': geom,
            'poi_category': self._categorize_poi(poi_type, poi_value),
            'poi_subcategory': poi_value,
            'poi_type': poi_type,
            'poi_value': poi_value,
            'name': tags.get('name'),
            'brand': tags.get('brand'),
            'retail_relevance_score': self._calculate_retail_relevance(poi_type, poi_value, tags),
            'h3_res_8': h3_data.get('h3_res_8'),
            'h3_res_9': h3_data.get('h3_res_9'),
            'h3_res_10': h3_data.get('h3_res_10')
        }

    def _categorize_poi(self, poi_type: str, poi_value: str) -> str:
        """Категоризація POI"""
        retail_categories = {
            'shop': {
                'supermarket': 'retail',
                'convenience': 'retail',
                'clothes': 'retail',
                'electronics': 'retail',
                'books': 'retail',
                'bakery': 'food',
                'butcher': 'food',
                'department_store': 'retail',
                'mall': 'retail',
                'pharmacy': 'services'
            },
            'amenity': {
                'restaurant': 'food',
                'cafe': 'food',
                'fast_food': 'food',
                'bar': 'food',
                'pub': 'food',
                'bank': 'services',
                'atm': 'services',
                'pharmacy': 'services',
                'fuel': 'services',
                'hospital': 'services',
                'clinic': 'services',
                'school': 'services',
                'university': 'services'
            },
            'office': 'services',
            'tourism': 'tourism',
            'leisure': 'leisure',
            'building': {
                'retail': 'retail',
                'commercial': 'services',
                'office': 'services'
            }
        }

        if poi_type in retail_categories:
            category = retail_categories[poi_type]
            if isinstance(category, dict):
                return category.get(poi_value, 'other')
            return category

        return 'other'
        
    def _calculate_retail_relevance(self, poi_type: str, poi_value: str, tags: Dict) -> float:
        """Розрахунок релевантності POI для роздрібної торгівлі"""
        base_score = 0.5
        
        # Підвищуємо для торгових POI
        if poi_type == 'shop':
            base_score = 0.9
        elif poi_type == 'amenity' and poi_value in ['restaurant', 'cafe', 'pharmacy', 'bank']:
            base_score = 0.7
        elif poi_type == 'office':
            base_score = 0.4
        elif poi_type == 'building' and poi_value == 'retail':
            base_score = 0.8
        
        # Бонуси за додаткову інформацію
        if tags.get('name'):
            base_score += 0.1
        if tags.get('opening_hours'):
            base_score += 0.1
        if tags.get('brand'):
            base_score += 0.1
        if tags.get('phone'):
            base_score += 0.05
        if tags.get('website'):
            base_score += 0.05
        
        return min(base_score, 1.0)
        
    def process_batch_with_h3(self, batch_records: List[Dict], region_name: str, start_index: int) -> Tuple[List[Dict], List[Dict]]:
        """Обробка батчу записів з розрахунком H3 - ВИПРАВЛЕНО ПАРСІНГ ТЕГІВ"""
        processed_records = []
        poi_records = []
        
        for i, record in enumerate(batch_records):
            try:
                # Отримуємо геометрію
                geom = record.get('geometry') or record.get('geom')
                if not geom:
                    continue
                
                # ВИПРАВЛЕНО: використовуємо індекс як unique ID
                record_index = start_index + i
                
                # Основний запис з H3
                h3_data = self.calculate_h3_for_geometry(geom)
                
                # ВИПРАВЛЕНО: Правильний парсинг OSM тегів
                tags = {}
                excluded_fields = ['geometry', 'geom', 'fid', 'osm_id']
                
                # Спочатку додаємо всі поля record (крім виключених)
                for key, value in record.items():
                    if key not in excluded_fields and value is not None:
                        if isinstance(value, (int, float, bool)):
                            tags[key] = str(value)
                        elif isinstance(value, str) and value.strip():
                            tags[key] = value.strip()
                
                # ВИПРАВЛЕНО: Якщо є поле 'tags' з JSON - розпарсити і додати до тегів
                osm_tags_string = record.get('tags')
                if osm_tags_string:
                    try:
                        real_osm_tags = json.loads(osm_tags_string)
                        # Додаємо/перезаписуємо реальними OSM тегами
                        for key, value in real_osm_tags.items():
                            if value is not None:
                                if isinstance(value, (int, float, bool)):
                                    tags[key] = str(value)
                                elif isinstance(value, str) and value.strip():
                                    tags[key] = value.strip()
                    except json.JSONDecodeError:
                        logger.warning(f"Неможливо розпарсити JSON теги для запису {record_index}: {osm_tags_string}")
                
                processed_record = {
                    'region_name': region_name,
                    'original_fid': record_index,
                    'osm_id': record.get('osm_id'),
                    'geom': geom,
                    'tags': json.dumps(tags) if tags else None,
                    'name': tags.get('name'),
                    'data_quality_score': self._calculate_data_quality(record)
                }
                
                # Додаємо H3 індекси
                processed_record.update(h3_data)
                processed_records.append(processed_record)
                
                # Витягуємо POI якщо є - ТЕПЕР З ПРАВИЛЬНИХ ТЕГІВ
                poi_data = self.extract_poi_data(record, tags, region_name, record_index)
                if poi_data:
                    poi_records.append(poi_data)
                        
            except Exception as e:
                logger.warning(f"Помилка обробки запису {i}: {e}")
                continue
        
        return processed_records, poi_records

    def _calculate_data_quality(self, record: Dict) -> float:
        """Розрахунок якості даних"""
        score = 0.3  # Базовий рівень
        
        # Бонуси за наявність даних
        if record.get('geometry') or record.get('geom'):
            score += 0.3
        if record.get('name'):
            score += 0.2
        if record.get('osm_id'):
            score += 0.1
        
        # Бонуси за додаткові атрибути
        bonus_fields = ['amenity', 'shop', 'addr:street', 'addr:city', 'phone', 'website']
        for field in bonus_fields:
            if record.get(field):
                score += 0.02
        
        return min(score, 1.0)
        
    def insert_batch_to_db(self, records: List[Dict], table_name: str) -> int:
        """Вставка батчу в базу даних - ВИПРАВЛЕНО БЕЗ ON CONFLICT"""
        if not records:
            return 0
        
        try:
            # ВИПРАВЛЕНО: SQL БЕЗ ON CONFLICT та з правильним синтаксисом
            if 'osm_raw' in table_name:
                sql = """
                INSERT INTO osm_ukraine.osm_raw 
                    (region_name, original_fid, osm_id, geom, tags, name, 
                     h3_res_7, h3_res_8, h3_res_9, h3_res_10, data_quality_score)
                VALUES 
                    (:region_name, :original_fid, :osm_id, 
                     ST_GeomFromText(:geom_wkt, 4326), :tags, :name,
                     :h3_res_7, :h3_res_8, :h3_res_9, :h3_res_10, 
                     :data_quality_score)
                """
            else:  # poi_normalized  
                sql = """
                INSERT INTO osm_ukraine.poi_normalized 
                    (region_name, source_fid, osm_id, geom, poi_category, poi_subcategory,
                     poi_type, poi_value, name, brand, retail_relevance_score,
                     h3_res_8, h3_res_9, h3_res_10)
                VALUES 
                    (:region_name, :source_fid, :osm_id, 
                     ST_GeomFromText(:geom_wkt, 4326), :poi_category, :poi_subcategory,
                     :poi_type, :poi_value, :name, :brand, :retail_relevance_score,
                     :h3_res_8, :h3_res_9, :h3_res_10)                
                """
            
            # Підготовка записів для вставки
            processed_records = []
            for record in records:
                processed_record = record.copy()
                
                # Конвертуємо геометрію в WKT
                if processed_record.get('geom'):
                    try:
                        if hasattr(processed_record['geom'], 'wkt'):
                            processed_record['geom_wkt'] = processed_record['geom'].wkt
                        else:
                            processed_record['geom_wkt'] = str(processed_record['geom'])
                    except Exception as e:
                        logger.warning(f"Помилка конвертації геометрії: {e}")
                        continue
                else:
                    continue  # Пропускаємо записи без геометрії
                
                # Видаляємо оригінальну геометрію
                del processed_record['geom']
                processed_records.append(processed_record)
            
            if not processed_records:
                return 0
            
            # ВИПРАВЛЕНО: Простий insert без batch
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    successful = 0
                    for record in processed_records:
                        try:
                            conn.execute(text(sql), record)
                            successful += 1
                        except Exception as record_error:
                            logger.debug(f"Пропускаємо дублікат запису: {record_error}")
                            continue
                    trans.commit()
                    return successful
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Помилка транзакції в {table_name}: {e}")
                    raise
                    
        except Exception as e:
            logger.error(f"Критична помилка вставки в {table_name}: {e}")
            return 0
        
    def log_etl_run(self, region_name: str, file_path: str, status: str, 
                   records_processed: int = 0, records_imported: int = 0,
                   processing_time: int = 0, error_message: str = None,
                   file_size_mb: float = 0.0, started_at: datetime = None) -> int:
        """Логування ETL процесу - ВИПРАВЛЕНО"""
        try:
            with self.engine.connect() as conn:
                # ВИПРАВЛЕНО SQL
                sql = """
                INSERT INTO osm_cache.etl_runs 
                (region_name, file_path, file_size_mb, records_processed, 
                 records_imported, processing_time_seconds, status, error_message, 
                 started_at, completed_at)
                VALUES (:region_name, :file_path, :file_size_mb, 
                        :records_processed, :records_imported, :processing_time,
                        :status, :error_message, :started_at, :completed_at)
                RETURNING id
                """
                
                result = conn.execute(text(sql), {
                    'region_name': region_name,
                    'file_path': str(file_path),
                    'file_size_mb': file_size_mb,
                    'records_processed': records_processed,
                    'records_imported': records_imported,
                    'processing_time': processing_time,
                    'status': status,
                    'error_message': error_message,
                    'started_at': started_at or datetime.now(),
                    'completed_at': datetime.now()
                })
                
                conn.commit()
                return result.fetchone()[0]
                
        except Exception as e:
            logger.error(f"Помилка логування ETL: {e}")
            return None


class OSMETLOrchestrator:
    """Оркестратор ETL процесу"""
    
    def __init__(self, config: ETLConfig):
        self.config = config
        self.processor = OSMDataProcessor(config)
        
    def discover_gpkg_files(self) -> List[Path]:
        """Виявлення GPKG файлів"""
        data_dir = Path(self.config.data_directory)
        gpkg_files = list(data_dir.glob("*.gpkg"))
        
        if not gpkg_files:
            raise FileNotFoundError(f"Не знайдено GPKG файлів в {data_dir}")
        
        # Сортування за розміром (найменші спочатку для тестування)
        gpkg_files.sort(key=lambda f: f.stat().st_size)
        
        logger.info(f"Знайдено {len(gpkg_files)} GPKG файлів")
        for f in gpkg_files:
            size_mb = f.stat().st_size / (1024 * 1024)
            logger.info(f"  • {f.name}: {size_mb:.1f} MB")
        
        return gpkg_files
    
    def run_etl_for_files(self, gpkg_files: List[Path], parallel: bool = False) -> Dict[str, Any]:
        """Запуск ETL для списку файлів"""
        results = []
        start_time = datetime.now()
        
        total_size_mb = sum(f.stat().st_size for f in gpkg_files) / (1024 * 1024)
        logger.info(f"Починаємо ETL для {len(gpkg_files)} файлів ({total_size_mb:.1f} MB)")
        
        # Завжди послідовна обробка для надійності
        logger.info("Послідовна обробка файлів")
        
        for gpkg_file in gpkg_files:
            try:
                result = self.processor.process_region_file(gpkg_file)
                results.append(result)
            except Exception as e:
                logger.error(f"Помилка обробки {gpkg_file.name}: {e}")
                results.append({
                    'region_name': self.processor.extract_region_name(gpkg_file.name),
                    'status': 'failed',
                    'error': str(e)
                })
        
        # Оновлення аналітики
        try:
            self.processor.refresh_analytics()
        except Exception as e:
            logger.warning(f"Помилка оновлення аналітики: {e}")
        
        # Підсумкові результати
        total_time = int((datetime.now() - start_time).total_seconds())
        
        successful = [r for r in results if r.get('status') == 'completed']
        failed = [r for r in results if r.get('status') == 'failed']
        
        total_records = sum(r.get('records_imported', 0) for r in successful)
        total_poi = sum(r.get('poi_imported', 0) for r in successful)
        
        summary = {
            'total_files': len(gpkg_files),
            'successful_files': len(successful),
            'failed_files': len(failed),
            'total_records_imported': total_records,
            'total_poi_imported': total_poi,
            'total_processing_time': total_time,
            'total_size_mb': total_size_mb,
            'average_speed_mb_per_minute': (total_size_mb / total_time * 60) if total_time > 0 else 0,
            'results': results
        }
        
        logger.info(f"ETL завершено: {len(successful)}/{len(gpkg_files)} файлів успішно, {total_records:,} записів, {total_poi:,} POI за {total_time}с")
        
        return summary


# CLI інтерфейс
@click.group()
def cli():
    """OSM ETL Pipeline - Імпорт HOT OSM експортів в PostGIS"""
    pass


@cli.command()
@click.option('--data-dir', default=r"C:\OSMData", help='Директорія з GPKG файлами')
@click.option('--parallel/--sequential', default=False, help='Паралельна обробка файлів (вимкнено)')
@click.option('--max-workers', default=1, help='Максимальна кількість воркерів')
@click.option('--batch-size', default=5000, help='Розмір батчу для обробки')
@click.option('--regions', help='Список регіонів через кому (якщо не вказано - всі)')
@click.option('--test-run', is_flag=True, help='Тестовий запуск на 1 найменшому файлі')
def import_osm(data_dir, parallel, max_workers, batch_size, regions, test_run):
    """Імпорт OSM даних в PostGIS"""
    
    config = ETLConfig(
        data_directory=data_dir,
        max_workers=max_workers,
        batch_size=batch_size
    )
    
    orchestrator = OSMETLOrchestrator(config)
    
    try:
        # Виявлення файлів
        all_files = orchestrator.discover_gpkg_files()
        
        # Фільтрація файлів
        if test_run:
            files_to_process = all_files[:1]
            logger.info(f"Тестовий запуск на файлі: {files_to_process[0].name}")
        elif regions:
            region_list = [r.strip() for r in regions.split(',')]
            files_to_process = [
                f for f in all_files 
                if orchestrator.processor.extract_region_name(f.name) in region_list
            ]
            logger.info(f"Обрано регіони: {[orchestrator.processor.extract_region_name(f.name) for f in files_to_process]}")
        else:
            files_to_process = all_files
            logger.info(f"Обробка всіх {len(files_to_process)} файлів")
        
        if not files_to_process:
            logger.error("Не знайдено файлів для обробки")
            return
        
        # Запуск ETL (завжди послідовно)
        results = orchestrator.run_etl_for_files(files_to_process, parallel=False)
        
        # Виведення результатів
        print("\n" + "="*80)
        print("ПІДСУМКИ ETL ПРОЦЕСУ")
        print("="*80)
        print(f"Файлів оброблено: {results['successful_files']}/{results['total_files']}")
        print(f"Записів імпортовано: {results['total_records_imported']:,}")
        print(f"POI імпортовано: {results['total_poi_imported']:,}")
        print(f"Час обробки: {results['total_processing_time']}с")
        print(f"Швидкість: {results['average_speed_mb_per_minute']:.1f} MB/хв")
        
        if results['failed_files'] > 0:
            print(f"\nНЕУСПІШНІ ФАЙЛИ ({results['failed_files']}):")
            for result in results['results']:
                if result.get('status') == 'failed':
                    print(f"  • {result['region_name']}: {result.get('error', 'Unknown error')}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Критична помилка ETL: {e}")
        logger.error(traceback.format_exc())


@cli.command()
def test_h3():
    """Тестування H3 функціональності"""
    print("Тестування оновлених H3 функцій...")
    
    # Тестові координати Києва
    lat, lon = 50.4501, 30.5234
    
    print(f"Тестові координати: {lat}, {lon}")
    
    for resolution in [7, 8, 9, 10]:
        h3_index = H3Utils.geo_to_h3_safe(lat, lon, resolution)
        if h3_index:
            back_lat, back_lon = H3Utils.h3_to_geo_safe(h3_index)
            if back_lat and back_lon:
                print(f"   Resolution {resolution}: {h3_index} -> {back_lat:.4f}, {back_lon:.4f}")
            else:
                print(f"   Resolution {resolution}: {h3_index} -> Помилка зворотної конвертації")
        else:
            print(f"   Resolution {resolution}: Помилка конвертації")
    
    print("Тест завершено!")


@cli.command()
def database_status():
    """Перевірка стану бази даних"""
    try:
        config = ETLConfig()
        processor = OSMDataProcessor(config)
        
        with processor.engine.connect() as conn:
            # Перевірка схем
            schemas_result = conn.execute(text("""
                SELECT schema_name FROM information_schema.schemata 
                WHERE schema_name IN ('osm_ukraine', 'osm_analytics', 'osm_cache')
            """))
            schemas = [row[0] for row in schemas_result]
            
            print("СТАН БАЗИ ДАНИХ OSM")
            print("="*50)
            print(f"Схеми: {', '.join(schemas)}")
            
            # Перевірка таблиць
            if 'osm_ukraine' in schemas:
                # Підрахунок записів
                count_result = conn.execute(text("SELECT COUNT(*) FROM osm_ukraine.osm_raw"))
                osm_count = count_result.scalar()
                
                poi_count_result = conn.execute(text("SELECT COUNT(*) FROM osm_ukraine.poi_normalized"))
                poi_count = poi_count_result.scalar()
                
                print(f"OSM записів: {osm_count:,}")
                print(f"POI записів: {poi_count:,}")
                
                # Статистика по регіонах
                region_stats = conn.execute(text("""
                    SELECT region_name, COUNT(*) as count 
                    FROM osm_ukraine.osm_raw 
                    GROUP BY region_name 
                    ORDER BY count DESC
                    LIMIT 10
                """))
                
                print("\nТОП-10 РЕГІОНІВ ЗА КІЛЬКІСТЮ ЗАПИСІВ:")
                for region, count in region_stats:
                    print(f"  • {region}: {count:,} записів")
            
            # ETL логи
            if 'osm_cache' in schemas:
                etl_stats = conn.execute(text("""
                    SELECT status, COUNT(*) as count 
                    FROM osm_cache.etl_runs 
                    GROUP BY status
                """))
                
                print("\nETL СТАТИСТИКА:")
                for status, count in etl_stats:
                    print(f"  • {status}: {count} запусків")
                
                # Останні ETL запуски
                recent_runs = conn.execute(text("""
                    SELECT region_name, status, started_at, processing_time_seconds, records_imported
                    FROM osm_cache.etl_runs 
                    ORDER BY started_at DESC 
                    LIMIT 5
                """))
                
                print("\nОСТАННІ ETL ЗАПУСКИ:")
                for region, status, started, time_sec, records in recent_runs:
                    print(f"  • {region}: {status} ({time_sec}с, {records:,} записів)")
                
    except Exception as e:
        print(f"Помилка перевірки БД: {e}")


@cli.command()
def cleanup():
    """Очищення всіх OSM даних з бази"""
    try:
        config = ETLConfig()
        processor = OSMDataProcessor(config)
        
        # Запитати підтвердження
        response = input("УВАГА! Це видалить ВСІ OSM дані з бази. Продовжити? (yes/no): ")
        if response.lower() != 'yes':
            print("Операція скасована")
            return
        
        with processor.engine.connect() as conn:
            print("Очищення таблиць...")
            
            # Очищення в правильному порядку (через foreign keys)
            tables = [
                'osm_analytics.h3_poi_summary',
                'osm_ukraine.poi_normalized', 
                'osm_ukraine.osm_raw',
                'osm_cache.etl_runs'
            ]
            
            for table in tables:
                try:
                    count_before = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    conn.execute(text(f"DELETE FROM {table}"))
                    print(f"  {table}: видалено {count_before:,} записів")
                except Exception as e:
                    print(f"  {table}: {e}")
            
            conn.commit()
            print("Очищення завершено")
            
    except Exception as e:
        print(f"Помилка очищення: {e}")


if __name__ == "__main__":
    # Перевірка що всі залежності встановлені
    try:
        import geopandas
        import h3
        import fiona
        import shapely
        import click
        import tqdm
        import psycopg2
        import sqlalchemy
        print("Всі залежності встановлені!")
    except ImportError as e:
        print(f"Відсутня залежність: {e}")
        print("Встановіть: pip install geopandas h3 fiona shapely click tqdm psycopg2-binary sqlalchemy")
        sys.exit(1)
    
    # Запуск CLI
    cli()