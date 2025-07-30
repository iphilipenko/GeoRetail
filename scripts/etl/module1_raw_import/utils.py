#!/usr/bin/env python3
"""
Модуль 1: Raw Data Import - Допоміжні функції
Утилітарні функції для роботи з GPKG файлами та H3
"""

import logging
import sys
import codecs
from pathlib import Path
from typing import Optional, List, Tuple
import h3
import geopandas as gpd
import fiona
from shapely.geometry import Point


def setup_logging(log_level: str = "INFO", log_file: str = "module1_import.log") -> logging.Logger:
    """Налаштування логування для модуля"""
    
    # ВИПРАВЛЕННЯ UNICODE ДЛЯ WINDOWS
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())
    
    # Створення logger
    logger = logging.getLogger('module1_raw_import')
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Очищення існуючих handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Форматування
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


class H3Utils:
    """Утилітарний клас для роботи з H3 - підтримка різних версій"""
    
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
        except Exception:
            return None
    
    @staticmethod
    def h3_to_geo_safe(h3_index: str) -> Optional[Tuple[float, float]]:
        """Безпечна конвертація H3 в координати"""
        try:
            # Нова версія
            return h3.cell_to_latlng(h3_index)
        except AttributeError:
            # Стара версія
            try:
                return h3.h3_to_geo(h3_index)
            except:
                return None
        except Exception:
            return None


def extract_region_name(filename: str) -> str:
    """Витягування назви регіону з імені GPKG файлу"""
    name = Path(filename).stem
    
    # Видаляємо суфікси типу _hotosm, _export тощо
    for suffix in ['_hotosm', '_export', '_osm', '_ukraine']:
        if suffix in name.lower():
            name = name.lower().replace(suffix, '')
            break
    
    return name.strip('_').title()


def extract_region_name(filename: str) -> str:
    """Витягування назви регіону з імені GPKG файлу"""
    name = Path(filename).stem
    
    # Видаляємо префікс UA_MAP_ якщо є
    if name.startswith('UA_MAP_'):
        name = name[7:]  # видаляємо 'UA_MAP_'
    
    return name


def validate_gpkg_file(file_path: Path, logger: logging.Logger) -> bool:
    """Базова валідація GPKG файлу"""
    
    if not file_path.exists():
        logger.error(f"Файл не існує: {file_path}")
        return False
    
    if file_path.suffix.lower() != '.gpkg':
        logger.error(f"Неправильний тип файлу: {file_path}")
        return False
    
    try:
        # Спроба відкрити файл
        with fiona.open(file_path, layer=0) as src:
            if len(src) == 0:
                logger.warning(f"Файл порожній: {file_path}")
                return False
                
        logger.debug(f"Файл валідний: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Помилка валідації файлу {file_path}: {e}")
        return False


def get_file_size_mb(file_path: Path) -> float:
    """Отримання розміру файлу в МБ"""
    try:
        return file_path.stat().st_size / (1024 * 1024)
    except Exception:
        return 0.0


def discover_gpkg_files(data_directory: str, logger: logging.Logger) -> List[Path]:
    """Пошук всіх GPKG файлів в директорії"""
    
    data_path = Path(data_directory)
    
    if not data_path.exists():
        logger.error(f"Директорія не існує: {data_directory}")
        return []
    
    # Пошук GPKG файлів
    gpkg_files = list(data_path.glob("*.gpkg"))
    
    if not gpkg_files:
        logger.warning(f"GPKG файли не знайдені в {data_directory}")
        return []
    
    # Сортування по розміру (найменші спочатку для тестування)
    gpkg_files.sort(key=lambda f: f.stat().st_size)
    
    logger.info(f"Знайдено {len(gpkg_files)} GPKG файлів")
    for file_path in gpkg_files:
        size_mb = get_file_size_mb(file_path)
        logger.debug(f"  {file_path.name}: {size_mb:.1f} MB")
    
    return gpkg_files


def format_duration(seconds: int) -> str:
    """Форматування тривалості в читабельний вигляд"""
    if seconds < 60:
        return f"{seconds}с"
    elif seconds < 3600:
        minutes = seconds // 60
        seconds = seconds % 60
        return f"{minutes}хв {seconds}с"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours}год {minutes}хв"