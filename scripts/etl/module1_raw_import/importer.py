#!/usr/bin/env python3
"""
Модуль 1: Raw Data Import - Основний клас імпортера
Спрощений але надійний імпорт OSM GPKG файлів в PostGIS
"""

import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm

from .config import ImportConfig
from .utils import (
    setup_logging, H3Utils, extract_region_name, 
    validate_gpkg_file, get_file_size_mb, format_duration
)


class RawDataImporter:
    """Основний клас для імпорту сирих OSM даних"""
    
    def __init__(self, config: ImportConfig):
        self.config = config
        self.logger = setup_logging(config.log_level, config.log_file)
        self.h3_utils = H3Utils()
        
        # Ініціалізація DB connection
        try:
            self.engine = create_engine(
                config.connection_string,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            self._test_connection()
        except Exception as e:
            self.logger.error(f"Помилка підключення до БД: {e}")
            raise
    
    def _test_connection(self):
        """Тестування підключення до БД"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                self.logger.info("Підключення до БД успішне")
        except Exception as e:
            self.logger.error(f"Не вдалося підключитися до БД: {e}")
            raise
    
    def discover_files(self, data_dir: Optional[str] = None) -> List[Path]:
        """Пошук GPKG файлів в директорії"""
        from .utils import discover_gpkg_files
        
        directory = data_dir or self.config.data_directory
        return discover_gpkg_files(directory, self.logger)
    
    def validate_file(self, file_path: Path) -> bool:
        """Валідація GPKG файлу"""
        return validate_gpkg_file(file_path, self.logger)
    
    def _prepare_record(self, record: dict, region_name: str) -> Optional[dict]:
        """Підготовка запису для вставки в БД - ТОЧНА ЛОГІКА З РОБОЧОГО КОДУ"""
        try:
            # Геометрія
            geom = record.get('geom') or record.get('geometry')
            if not geom:
                return None
            
            # Парсимо JSON теги
            import json
            tags_dict = {}
            if record.get('tags'):
                try:
                    if isinstance(record['tags'], str):
                        tags_dict = json.loads(record['tags'])
                    elif isinstance(record['tags'], dict):
                        tags_dict = record['tags']
                except Exception:
                    tags_dict = {}
            
            # H3 розрахунки - ТОЧНО ЯК В ОРИГІНАЛІ
            h3_data = self._calculate_h3_for_geometry(geom)
            
            # Підготовка тегів - ТОЧНО ЯК В ОРИГІНАЛІ  
            tags = {}
            excluded_fields = ['geometry', 'geom', 'fid', 'osm_id']
            for key, value in record.items():
                if key not in excluded_fields and value is not None:
                    # Конвертуємо в строку для JSON
                    if isinstance(value, (int, float, bool)):
                        tags[key] = str(value)
                    elif isinstance(value, str) and value.strip():
                        tags[key] = value.strip()
            
            # ТОЧНА СТРУКТУРА З РОБОЧОГО КОДУ
            processed_record = {
                'region_name': region_name,
                'original_fid': 0,  # Використовуємо індекс як FID
                'osm_id': record.get('osm_id'),
                'geom': geom,  # ВАЖЛИВО: геометрія залишається як об'єкт
                'tags': json.dumps(tags) if tags else None,
                'name': tags.get('name'),
                'data_quality_score': self._calculate_data_quality_original(record)
            }
            
            # Додаємо H3 індекси
            processed_record.update(h3_data)
            
            return processed_record
            
        except Exception as e:
            self.logger.debug(f"Помилка підготовки запису: {e}")
            return None
    
    def _calculate_h3_for_geometry(self, geom) -> dict:
        """Розрахунок H3 індексів для геометрії - ТОЧНО ЯК В ОРИГІНАЛІ"""
        h3_results = {}
        resolutions = [7, 8, 9, 10]
        
        try:
            # Отримуємо координати для H3
            if hasattr(geom, 'centroid'):
                # Для полігонів/ліній використовуємо центроїд
                centroid = geom.centroid
                lat, lon = centroid.y, centroid.x
            elif hasattr(geom, 'y') and hasattr(geom, 'x'):
                # Для точок
                lat, lon = geom.y, geom.x
            else:
                return {f'h3_res_{res}': None for res in resolutions}
            
            # Перевіряємо валідність координат
            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                return {f'h3_res_{res}': None for res in resolutions}
            
            # Розрахунок H3 для кожної резолюції
            for res in resolutions:
                h3_index = self.h3_utils.geo_to_h3_safe(lat, lon, res)
                h3_results[f'h3_res_{res}'] = h3_index
                
        except Exception:
            h3_results = {f'h3_res_{res}': None for res in resolutions}
        
        return h3_results
    
    def _calculate_data_quality_original(self, record: dict) -> float:
        """Розрахунок якості даних - ТОЧНО ЯК В ОРИГІНАЛІ"""
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
    
    def _calculate_data_quality(self, record: dict, tags_dict: dict) -> float:
        """Розрахунок якості даних"""
        score = 0.3  # Базовий рівень
        
        # Бонуси за наявність даних
        if record.get('geometry') or record.get('geom'):
            score += 0.3
        if tags_dict.get('name'):
            score += 0.2
        if record.get('osm_id'):
            score += 0.1
        
        # Бонуси за додаткові атрибути в тегах
        bonus_fields = ['amenity', 'shop', 'addr:street', 'addr:city', 'phone', 'website']
        for field in bonus_fields:
            if tags_dict.get(field):
                score += 0.02
        
        return min(score, 1.0)
    
    def _insert_batch(self, records: List[dict]) -> int:
        """Вставка батчу записів в БД - ТОЧНИЙ РОБОЧИЙ SQL"""
        if not records:
            return 0
        
        # ТОЧНИЙ SQL З РОБОЧОГО КОДУ
        sql = """
        INSERT INTO osm_ukraine.osm_raw 
        (region_name, original_fid, osm_id, geom, tags, name, 
         h3_res_7, h3_res_8, h3_res_9, h3_res_10, data_quality_score)
        VALUES (:region_name, :original_fid, :osm_id, 
                ST_GeomFromText(:geom_wkt, 4326), :tags, :name,
                :h3_res_7, :h3_res_8, :h3_res_9, :h3_res_10, 
                :data_quality_score)
        """
        
        # ТОЧНА ЛОГІКА З РОБОЧОГО КОДУ
        try:
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
                        self.logger.warning(f"Помилка конвертації геометрії: {e}")
                        continue
                else:
                    continue  # Пропускаємо записи без геометрії
                
                # Видаляємо оригінальну геометрію
                del processed_record['geom']
                processed_records.append(processed_record)
            
            if not processed_records:
                return 0
            
            # ТОЧНА ВСТАВКА З РОБОЧОГО КОДУ
            with self.engine.connect() as conn:
                trans = conn.begin()
                try:
                    successful = 0
                    for record in processed_records:
                        try:
                            conn.execute(text(sql), record)
                            successful += 1
                        except Exception as record_error:
                            self.logger.debug(f"Пропускаємо дублікат запису: {record_error}")
                            continue
                    trans.commit()
                    return successful
                except Exception as e:
                    trans.rollback()
                    self.logger.error(f"Помилка транзакції: {e}")
                    raise
                    
        except Exception as e:
            self.logger.error(f"Критична помилка вставки: {e}")
            return 0
    
    def _log_etl_run(self, region_name: str, file_path: str, status: str, 
                     records_processed: int = 0, records_imported: int = 0,
                     processing_time: int = 0, error_message: str = None,
                     file_size_mb: float = 0.0) -> int:
        """Логування ETL процесу в БД"""
        try:
            sql = """
            INSERT INTO osm_cache.etl_runs 
            (region_name, file_path, file_size_mb, records_processed, 
             records_imported, processing_time_seconds, status, error_message, 
             started_at, completed_at)
            VALUES (
                :region_name, :file_path, :file_size_mb, :records_processed,
                :records_imported, :processing_time, :status, :error_message,
                :started_at, :completed_at
            )
            RETURNING id
            """
            
            now = datetime.now()
            params = {
                'region_name': region_name,
                'file_path': file_path,
                'file_size_mb': file_size_mb,
                'records_processed': records_processed,
                'records_imported': records_imported,
                'processing_time': processing_time,
                'status': status,
                'error_message': error_message,
                'started_at': now,
                'completed_at': now
            }
            
            with self.engine.connect() as conn:
                result = conn.execute(text(sql), params)
                conn.commit()
                return result.fetchone()[0]
                
        except Exception as e:
            self.logger.warning(f"Не вдалося записати лог в БД: {e}")
            return 0
    
    def import_file(self, file_path: Path, region_name_override: str = None) -> Dict[str, Any]:
        """Імпорт одного GPKG файлу"""
        start_time = time.time()
        region_name = region_name_override or extract_region_name(file_path.name)
        file_size_mb = get_file_size_mb(file_path)
        
        self.logger.info(f"Початок імпорту: {file_path.name} ({file_size_mb:.1f} MB)")
        
        # Валідація файлу
        if not self.validate_file(file_path):
            error_msg = f"Файл не пройшов валідацію: {file_path}"
            self.logger.error(error_msg)
            self._log_etl_run(region_name, str(file_path), 'failed', 
                            error_message=error_msg, file_size_mb=file_size_mb)
            return {
                'region_name': region_name,
                'status': 'failed',
                'error': error_msg,
                'records_imported': 0,
                'processing_time': int(time.time() - start_time)
            }
        
        try:
            # Читання файлу
            self.logger.info(f"Читання файлу: {file_path.name}")
            gdf = gpd.read_file(file_path)
            
            total_records = len(gdf)
            self.logger.info(f"Знайдено {total_records:,} записів")
            
            if total_records == 0:
                self.logger.warning(f"Файл порожній: {file_path.name}")
                self._log_etl_run(region_name, str(file_path), 'completed',
                                records_processed=0, records_imported=0,
                                processing_time=int(time.time() - start_time),
                                file_size_mb=file_size_mb)
                return {
                    'region_name': region_name,
                    'status': 'completed',
                    'records_imported': 0,
                    'processing_time': int(time.time() - start_time)
                }
            
            # Обробка батчами
            total_imported = 0
            batch_size = self.config.batch_size
            
            self.logger.info(f"Початок обробки батчами по {batch_size} записів")
            
            with tqdm(total=total_records, desc=f"Імпорт {region_name}") as pbar:
                for i in range(0, total_records, batch_size):
                    batch_df = gdf.iloc[i:i + batch_size]
                    
                    self.logger.debug(f"Обробка батчу {i//batch_size + 1}: записи {i}-{i+len(batch_df)}")
                    
                    # Підготовка записів
                    prepared_records = []
                    for _, record in batch_df.iterrows():
                        prepared = self._prepare_record(record.to_dict(), region_name)
                        if prepared:
                            prepared_records.append(prepared)
                    
                    self.logger.debug(f"Батч {i//batch_size + 1}: підготовлено {len(prepared_records)} з {len(batch_df)} записів")
                    
                    # Вставка в БД
                    if prepared_records:
                        imported = self._insert_batch(prepared_records)
                        total_imported += imported
                        self.logger.debug(f"Батч {i//batch_size + 1}: вставлено {imported} записів")
                    else:
                        self.logger.warning(f"Батч {i//batch_size + 1}: жоден запис не підготовлений")
                    
                    pbar.update(len(batch_df))
                    
                    # Логування прогресу кожні 20 батчів
                    if (i // batch_size) % 20 == 0 and i > 0:
                        self.logger.info(f"Прогрес: {total_imported:,} записів вставлено з {i + len(batch_df):,} оброблених")
            
            processing_time = int(time.time() - start_time)
            
            # Логування успішного імпорту
            self._log_etl_run(region_name, str(file_path), 'completed',
                            records_processed=total_records,
                            records_imported=total_imported,
                            processing_time=processing_time,
                            file_size_mb=file_size_mb)
            
            self.logger.info(f"Завершено імпорт {file_path.name}: "
                           f"{total_imported:,}/{total_records:,} записів за {format_duration(processing_time)}")
            
            return {
                'region_name': region_name,
                'status': 'completed',
                'records_processed': total_records,
                'records_imported': total_imported,
                'processing_time': processing_time,
                'file_size_mb': file_size_mb
            }
            
        except Exception as e:
            processing_time = int(time.time() - start_time)
            error_msg = f"Помилка імпорту {file_path.name}: {str(e)}"
            
            self.logger.error(error_msg)
            self._log_etl_run(region_name, str(file_path), 'failed',
                            processing_time=processing_time,
                            error_message=str(e),
                            file_size_mb=file_size_mb)
            
            return {
                'region_name': region_name,
                'status': 'failed',
                'error': str(e),
                'records_imported': 0,
                'processing_time': processing_time
            }
    
    def run_import(self, data_dir: Optional[str] = None, 
                   regions: Optional[List[str]] = None,
                   test_run: bool = False) -> Dict[str, Any]:
        """Головний метод для запуску імпорту"""
        start_time = time.time()
        
        self.logger.info("=== Початок імпорту OSM даних ===")
        
        # Виявлення файлів
        files = self.discover_files(data_dir)
        if not files:
            self.logger.error("Файли для імпорту не знайдені")
            return {
                'total_files': 0,
                'successful_files': 0,
                'failed_files': 0,
                'total_records_imported': 0,
                'total_processing_time': 0,
                'results': []
            }
        
        # Фільтрація по регіонах
        if regions:
            regions_lower = [r.lower() for r in regions]
            files = [f for f in files if any(r in f.name.lower() for r in regions_lower)]
            self.logger.info(f"Фільтрація по регіонах {regions}: {len(files)} файлів")
        
        # Тестовий запуск - тільки найменший файл
        if test_run:
            files = files[:1]
            self.logger.info(f"Тестовий запуск: {files[0].name}")
        
        # Обробка файлів
        results = []
        total_size_mb = sum(get_file_size_mb(f) for f in files)
        
        self.logger.info(f"Буде оброблено {len(files)} файлів, загальний розмір: {total_size_mb:.1f} MB")
        
        for file_path in files:
            # ВИПРАВЛЕННЯ: правильне витягування назви регіону
            region_name = extract_region_name(file_path.name)
            self.logger.info(f"Витягнута назва регіону: '{region_name}' з файлу: {file_path.name}")
            
            result = self.import_file(file_path, region_name_override=region_name)
            results.append(result)
        
        # Підсумкові результати
        total_time = int(time.time() - start_time)
        successful = [r for r in results if r.get('status') == 'completed']
        failed = [r for r in results if r.get('status') == 'failed']
        
        total_records = sum(r.get('records_imported', 0) for r in successful)
        
        summary = {
            'total_files': len(files),
            'successful_files': len(successful),
            'failed_files': len(failed),
            'total_records_imported': total_records,
            'total_processing_time': total_time,
            'total_size_mb': total_size_mb,
            'average_speed_mb_per_minute': (total_size_mb / total_time * 60) if total_time > 0 else 0,
            'results': results
        }
        
        self.logger.info(f"=== Імпорт завершено ===")
        self.logger.info(f"Файлів: {len(successful)}/{len(files)} успішно")
        self.logger.info(f"Записів: {total_records:,}")
        self.logger.info(f"Час: {format_duration(total_time)}")
        
        if failed:
            self.logger.warning(f"Файлів з помилками: {len(failed)}")
            for fail in failed:
                self.logger.warning(f"  {fail['region_name']}: {fail.get('error', 'Unknown error')}")
        
        return summary