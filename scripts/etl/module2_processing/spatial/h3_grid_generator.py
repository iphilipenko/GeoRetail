#!/usr/bin/env python3
"""
H3 Grid Generator на основі існуючих H3 індексів з osm_ukraine.osm_raw
Частина Модуля 2: Data Processing & Normalization

Цей скрипт створює H3 геометрії для існуючих H3 індексів з таблиці сирих даних
"""

import json
import logging
import sys
from dataclasses import dataclass
from typing import Dict, List, Set, Tuple
from pathlib import Path

import h3
import psycopg2
from psycopg2.extras import execute_batch
from shapely.geometry import Polygon, Point

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class H3Cell:
    """Структура для зберігання H3 комірки з геометрією"""
    h3_index: str
    resolution: int
    geom_wkt: str          # PostGIS POLYGON
    geojson_geometry: dict # GeoJSON для Qlik Sense
    center_lat: float
    center_lon: float
    area_km2: float
    ukraine_region: str = None


class H3GridFromExistingData:
    """
    Генератор H3 сітки на основі існуючих H3 індексів з osm_raw
    """
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.resolutions = [7, 8, 9, 10]
        
    def extract_existing_h3_indexes(self) -> Dict[int, Set[str]]:
        """
        Витягує всі унікальні H3 індекси з osm_ukraine.osm_raw
        
        Returns:
            Dict[int, Set[str]]: Словник {resolution: {h3_index, ...}}
        """
        logger.info("🔍 Витягування існуючих H3 індексів з osm_ukraine.osm_raw")
        
        h3_indexes = {res: set() for res in self.resolutions}
        
        with psycopg2.connect(self.db_connection_string) as conn:
            with conn.cursor() as cursor:
                
                for resolution in self.resolutions:
                    column_name = f"h3_res_{resolution}"
                    
                    logger.info(f"📊 Обробка {column_name}...")
                    
                    # Витягуємо всі унікальні H3 індекси для цього resolution
                    query = f"""
                        SELECT DISTINCT {column_name} 
                        FROM osm_ukraine.osm_raw 
                        WHERE {column_name} IS NOT NULL 
                          AND {column_name} != ''
                    """
                    
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    # Додаємо до множини
                    for row in results:
                        h3_index = row[0].strip()
                        if h3_index:  # Додаткова перевірка на порожні рядки
                            h3_indexes[resolution].add(h3_index)
                    
                    logger.info(f"   ✅ Знайдено {len(h3_indexes[resolution]):,} унікальних H3 індексів")
        
        # Підсумкова статистика
        total_indexes = sum(len(indexes) for indexes in h3_indexes.values())
        logger.info(f"🎉 Загалом витягнуто {total_indexes:,} унікальних H3 індексів")
        
        return h3_indexes
    
    def generate_geometries_for_h3_indexes(self, h3_indexes: Dict[int, Set[str]]) -> Dict[int, List[H3Cell]]:
        """
        Створює геометрії для існуючих H3 індексів
        
        Args:
            h3_indexes: Словник з H3 індексами по resolutions
            
        Returns:
            Dict[int, List[H3Cell]]: Словник з H3Cell об'єктами
        """
        logger.info("🗺️ Створення геометрій для H3 індексів")
        
        all_cells = {}
        total_cells = 0
        
        for resolution, indexes in h3_indexes.items():
            if not indexes:
                logger.warning(f"⚠️ Немає H3 індексів для resolution {resolution}")
                all_cells[resolution] = []
                continue
                
            logger.info(f"🔢 Обробка resolution {resolution}: {len(indexes):,} індексів")
            
            cells = []
            processed = 0
            
            for h3_index in indexes:
                try:
                    # Валідуємо H3 індекс
                    if not h3.is_valid_cell(h3_index):
                        logger.warning(f"⚠️ Невалідний H3 індекс: {h3_index}")
                        continue
                        
                    # Перевіряємо resolution
                    actual_resolution = h3.get_resolution(h3_index)
                    if actual_resolution != resolution:
                        logger.warning(f"⚠️ Неочікуваний resolution для {h3_index}: {actual_resolution} != {resolution}")
                        continue
                    
                    # Створюємо H3Cell
                    cell = self._create_h3_cell(h3_index, resolution)
                    cells.append(cell)
                    
                    processed += 1
                    if processed % 5000 == 0:
                        logger.info(f"   ⚙️ Оброблено {processed:,}/{len(indexes):,} індексів")
                        
                except Exception as e:
                    logger.error(f"❌ Помилка обробки H3 індексу {h3_index}: {e}")
                    continue
            
            all_cells[resolution] = cells
            total_cells += len(cells)
            logger.info(f"✅ Resolution {resolution}: {len(cells):,} валідних комірок")
        
        logger.info(f"🎉 Створення геометрій завершено! Всього комірок: {total_cells:,}")
        return all_cells
    
    def _create_h3_cell(self, h3_index: str, resolution: int) -> H3Cell:
        """
        Створює H3Cell об'єкт з повною геометрією
        
        Args:
            h3_index: H3 індекс комірки
            resolution: Рівень деталізації
            
        Returns:
            H3Cell: Об'єкт з геометрією та метаданими
        """
        # Отримуємо boundary coordinates (H3 v4+ API)
        boundary_coords = h3.cell_to_boundary(h3_index)
        # Конвертуємо в список координат
        boundary_list = [(lat, lon) for lat, lon in boundary_coords]
        
        # Створюємо PostGIS WKT геометрію (lon, lat порядок для WKT)
        wkt_coords = [(lon, lat) for lat, lon in boundary_list]
        wkt_coords.append(wkt_coords[0])  # Замикаємо полігон
        geom_wkt = f"POLYGON(({', '.join([f'{lon} {lat}' for lon, lat in wkt_coords])}))"
        
        # Створюємо GeoJSON геометрію для візуалізації (lon, lat порядок)
        geojson_coords = [[lon, lat] for lat, lon in boundary_list]
        geojson_coords.append(geojson_coords[0])  # Замикаємо полігон
        geojson_geometry = {
            "type": "Polygon",
            "coordinates": [geojson_coords]
        }
        
        # Отримуємо центр комірки (H3 v4+ API)
        center_lat, center_lon = h3.cell_to_latlng(h3_index)
        
        # Розраховуємо площу (H3 v4+ API)
        area_km2 = h3.average_hexagon_area(resolution, unit='km^2')
        
        return H3Cell(
            h3_index=h3_index,
            resolution=resolution,
            geom_wkt=geom_wkt,
            geojson_geometry=geojson_geometry,
            center_lat=center_lat,
            center_lon=center_lon,
            area_km2=area_km2
        )
    
    def save_to_database(self, h3_cells: Dict[int, List[H3Cell]]):
        """
        Зберігає H3 сітку в базу даних
        
        Args:
            h3_cells: Словник з H3 комірками по resolutions
        """
        logger.info("💾 Початок збереження H3 сітки в базу даних")
        
        with psycopg2.connect(self.db_connection_string) as conn:
            with conn.cursor() as cursor:
                # Створюємо таблицю якщо не існує
                self._create_h3_grid_table(cursor)
                
                # Очищуємо існуючі дані
                logger.info("🗑️ Очищення існуючих H3 даних")
                cursor.execute("TRUNCATE TABLE osm_ukraine.h3_grid")
                
                # Зберігаємо дані по resolutions
                total_saved = 0
                for resolution, cells in h3_cells.items():
                    if not cells:
                        logger.info(f"⏭️ Пропускаємо resolution {resolution}: немає даних")
                        continue
                        
                    logger.info(f"💾 Збереження resolution {resolution}: {len(cells):,} комірок")
                    
                    # Підготовляємо дані для batch insert
                    batch_data = []
                    for cell in cells:
                        batch_data.append((
                            cell.h3_index,
                            cell.resolution,
                            cell.geom_wkt,
                            json.dumps(cell.geojson_geometry),
                            f"POINT({cell.center_lon} {cell.center_lat})",
                            cell.area_km2
                        ))
                    
                    # Batch insert
                    insert_query = """
                        INSERT INTO osm_ukraine.h3_grid 
                        (h3_index, resolution, geom, geojson_geometry, center_point, area_km2)
                        VALUES (%s, %s, ST_GeomFromText(%s, 4326), %s, ST_GeomFromText(%s, 4326), %s)
                    """
                    
                    execute_batch(cursor, insert_query, batch_data, page_size=1000)
                    total_saved += len(cells)
                    logger.info(f"   ✅ Збережено {len(cells):,} комірок")
                
                conn.commit()
                logger.info(f"🎉 Успішно збережено {total_saved:,} H3 комірок в базу даних")
    
    def _create_h3_grid_table(self, cursor):
        """Створює таблицю h3_grid якщо не існує"""
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS osm_ukraine.h3_grid (
            h3_index VARCHAR(15) PRIMARY KEY,
            resolution INTEGER NOT NULL,
            geom GEOMETRY(POLYGON, 4326) NOT NULL,
            geojson_geometry JSONB,
            center_point GEOMETRY(POINT, 4326) NOT NULL,
            area_km2 DECIMAL(8,4) NOT NULL,
            ukraine_region VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            
            CONSTRAINT h3_grid_resolution_check CHECK (resolution BETWEEN 7 AND 10)
        );
        
        -- Створюємо індекси
        CREATE INDEX IF NOT EXISTS idx_h3_grid_resolution 
            ON osm_ukraine.h3_grid (resolution);
            
        CREATE INDEX IF NOT EXISTS idx_h3_grid_geom 
            ON osm_ukraine.h3_grid USING GIST (geom);
            
        CREATE INDEX IF NOT EXISTS idx_h3_grid_center 
            ON osm_ukraine.h3_grid USING GIST (center_point);
        """
        
        cursor.execute(create_table_sql)
        logger.info("📋 Таблиця h3_grid створена/перевірена")
    
    def generate_statistics(self) -> Dict[str, any]:
        """
        Генерує статистику по створеній H3 сітці
        
        Returns:
            Dict: Статистика по resolutions
        """
        logger.info("📊 Генерація статистики H3 сітки")
        
        stats = {}
        
        with psycopg2.connect(self.db_connection_string) as conn:
            with conn.cursor() as cursor:
                # Загальна статистика
                cursor.execute("""
                    SELECT 
                        resolution,
                        COUNT(*) as cell_count,
                        SUM(area_km2) as total_area_km2,
                        AVG(area_km2) as avg_cell_area,
                        MIN(area_km2) as min_cell_area,
                        MAX(area_km2) as max_cell_area
                    FROM osm_ukraine.h3_grid 
                    GROUP BY resolution 
                    ORDER BY resolution
                """)
                
                resolution_stats = {}
                for row in cursor.fetchall():
                    resolution_stats[row[0]] = {
                        'cell_count': row[1],
                        'total_area_km2': float(row[2]),
                        'avg_cell_area': float(row[3]),
                        'min_cell_area': float(row[4]),
                        'max_cell_area': float(row[5])
                    }
                
                stats['by_resolution'] = resolution_stats
                
                # Загальна кількість
                cursor.execute("SELECT COUNT(*) FROM osm_ukraine.h3_grid")
                stats['total_cells'] = cursor.fetchone()[0]
                
                # Покриття території (беремо resolution 9 для оцінки)
                cursor.execute("""
                    SELECT SUM(area_km2) FROM osm_ukraine.h3_grid 
                    WHERE resolution = 9
                """)
                result = cursor.fetchone()[0]
                stats['ukraine_coverage_km2'] = float(result) if result else 0.0
        
        return stats
    
    def print_statistics(self, stats: Dict[str, any]):
        """Виводить статистику в красивому форматі"""
        
        print("\n" + "="*60)
        print("📊 СТАТИСТИКА H3 СІТКИ УКРАЇНИ (на основі OSM даних)")
        print("="*60)
        print(f"🗺️ Загальна кількість комірок: {stats['total_cells']:,}")
        print(f"🇺🇦 Покриття України (res 9): {stats['ukraine_coverage_km2']:,.0f} км²")
        print("\n📏 По рівнях деталізації:")
        
        for resolution, res_stats in stats['by_resolution'].items():
            print(f"\n   Resolution {resolution}:")
            print(f"     • Комірок: {res_stats['cell_count']:,}")
            print(f"     • Середня площа: {res_stats['avg_cell_area']:.2f} км²")
            print(f"     • Загальна площа: {res_stats['total_area_km2']:,.0f} км²")
        
        print("\n" + "="*60)
        print("💡 Це геометрії для H3 індексів, які реально є в OSM даних!")
        print("="*60)

    def run_full_process(self):
        """Запускає повний процес створення H3 Grid"""
        try:
            # 1. Витягуємо існуючі H3 індекси
            h3_indexes = self.extract_existing_h3_indexes()
            
            # 2. Створюємо геометрії
            h3_cells = self.generate_geometries_for_h3_indexes(h3_indexes)
            
            # 3. Зберігаємо в базу даних
            self.save_to_database(h3_cells)
            
            # 4. Генеруємо та виводимо статистику
            stats = self.generate_statistics()
            self.print_statistics(stats)
            
            logger.info("✅ H3 Grid Generator завершено успішно!")
            
        except Exception as e:
            logger.error(f"❌ Помилка: {e}")
            sys.exit(1)


def main():
    """Головна функція для запуску генерації H3 сітки"""
    
    # Конфігурація бази даних
    DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"
    
    # Створюємо генератор та запускаємо процес
    generator = H3GridFromExistingData(DB_CONNECTION_STRING)
    generator.run_full_process()


if __name__ == "__main__":
    main()