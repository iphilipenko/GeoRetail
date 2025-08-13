#!/usr/bin/env python3
"""
Масова обробка всієї таблиці osm_raw за допомогою V3 процесора
Fixed version без емодзі для Windows
"""

import subprocess
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from datetime import datetime
import sys
import os

# Налаштування логування для Windows
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'mass_processing_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Налаштування кодування для Windows консолі
if os.name == 'nt':  # Windows
    os.environ['PYTHONIOENCODING'] = 'utf-8'

DB_CONNECTION_STRING = "postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail"

class MassProcessingManager:
    """Менеджер масової обробки osm_raw"""
    
    def __init__(self):
        self.connection_string = DB_CONNECTION_STRING
        self.stats = {
            'total_processed': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'start_time': datetime.now()
        }
    
    def get_processing_stats(self):
        """Отримання статистики обробки"""
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Загальна кількість записів
            cur.execute("SELECT COUNT(*) as total FROM osm_ukraine.osm_raw")
            total_records = cur.fetchone()['total']
            
            # Потенційно релевантні записи
            cur.execute("""
                SELECT COUNT(*) as relevant 
                FROM osm_ukraine.osm_raw
                WHERE tags IS NOT NULL
                AND (
                    tags::text LIKE '%shop%' 
                    OR tags::text LIKE '%amenity%'
                    OR tags::text LIKE '%highway%'
                    OR tags::text LIKE '%public_transport%'
                    OR tags::text LIKE '%railway%'
                    OR tags::text LIKE '%brand%'
                )
            """)
            relevant_records = cur.fetchone()['relevant']
            
            # Вже оброблені записи
            cur.execute("""
                SELECT COUNT(*) as processed 
                FROM osm_ukraine.poi_processed 
                WHERE osm_raw_id IS NOT NULL
            """)
            processed_records = cur.fetchone()['processed']
            
            # Залишилось обробити
            remaining = relevant_records - processed_records
            
            return {
                'total_records': total_records,
                'relevant_records': relevant_records,
                'processed_records': processed_records,
                'remaining_records': remaining,
                'completion_rate': (processed_records / relevant_records) * 100 if relevant_records > 0 else 0
            }
            
        finally:
            cur.close()
            conn.close()
    
    def get_regions_list(self):
        """Отримання списку регіонів для обробки"""
        conn = psycopg2.connect(self.connection_string)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            cur.execute("""
                SELECT 
                    region_name,
                    COUNT(*) as total_records,
                    COUNT(CASE WHEN tags IS NOT NULL 
                          AND (tags::text LIKE '%shop%' OR tags::text LIKE '%amenity%'
                               OR tags::text LIKE '%highway%' OR tags::text LIKE '%public_transport%'
                               OR tags::text LIKE '%railway%' OR tags::text LIKE '%brand%')
                          THEN 1 END) as relevant_records
                FROM osm_ukraine.osm_raw
                GROUP BY region_name
                HAVING COUNT(CASE WHEN tags IS NOT NULL 
                             AND (tags::text LIKE '%shop%' OR tags::text LIKE '%amenity%'
                                  OR tags::text LIKE '%highway%' OR tags::text LIKE '%public_transport%'
                                  OR tags::text LIKE '%railway%' OR tags::text LIKE '%brand%')
                             THEN 1 END) > 0
                ORDER BY relevant_records DESC
            """)
            
            regions = cur.fetchall()
            return regions
            
        finally:
            cur.close()
            conn.close()
    
    def run_processing_batch(self, region=None, limit=50000, batch_size=5000):
        """Запуск одного батча обробки"""
        cmd = [
            'python', 'process_entities_v3.py',
            '--limit', str(limit),
            '--batch-size', str(batch_size)
        ]
        
        if region:
            cmd.extend(['--region', region])
        
        try:
            logger.info(f"[LAUNCH] Starting processing: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2 години timeout
            
            if result.returncode == 0:
                logger.info(f"[SUCCESS] Batch completed successfully")
                if result.stdout:
                    logger.info(f"STDOUT: {result.stdout}")
                self.stats['successful_batches'] += 1
                return True
            else:
                logger.error(f"[ERROR] Batch failed with return code {result.returncode}")
                if result.stderr:
                    logger.error(f"STDERR: {result.stderr}")
                self.stats['failed_batches'] += 1
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"[TIMEOUT] Batch exceeded 1 hour timeout")
            self.stats['failed_batches'] += 1
            return False
        except Exception as e:
            logger.error(f"[EXCEPTION] Unexpected error: {e}")
            self.stats['failed_batches'] += 1
            return False
    
    def process_all_regions(self, limit_per_region=50000, batch_size=5000):
        """Обробка всіх регіонів по черзі"""
        logger.info("[REGIONS] Starting processing of all regions")
        
        # Отримуємо список регіонів
        regions = self.get_regions_list()
        logger.info(f"[REGIONS] Found {len(regions)} regions for processing")
        
        for i, region_info in enumerate(regions, 1):
            region_name = region_info['region_name']
            relevant_records = region_info['relevant_records']
            
            logger.info(f"\n[REGION] {i}/{len(regions)}: {region_name} ({relevant_records:,} relevant records)")
            
            # Обробляємо регіон поки не закінчаться дані
            rounds = 0
            while rounds < 10:  # Максимум 10 раундів на регіон (500K записів)
                rounds += 1
                logger.info(f"  [ROUND] {rounds} for {region_name}")
                
                success = self.run_processing_batch(
                    region=region_name, 
                    limit=limit_per_region, 
                    batch_size=batch_size
                )
                
                if not success:
                    logger.warning(f"  [WARNING] Round {rounds} for {region_name} failed, moving to next region")
                    break
                
                # Перевіряємо чи залишилися дані
                stats = self.get_processing_stats()
                if stats['remaining_records'] == 0:
                    logger.info("[COMPLETE] All records processed!")
                    break
                
                # Короткий відпочинок між батчами
                time.sleep(5)
        
        self.print_final_stats()
    
    def process_universal_batches(self, limit_per_batch=100000, batch_size=10000, max_batches=50):
        """Обробка великими універсальними батчами (без регіональних обмежень)"""
        logger.info("[UNIVERSAL] Starting universal processing with large batches")
        
        for batch_num in range(1, max_batches + 1):
            logger.info(f"\n[BATCH] Universal batch {batch_num}/{max_batches}")
            
            success = self.run_processing_batch(
                region=None,
                limit=limit_per_batch,
                batch_size=batch_size
            )
            
            if not success:
                logger.warning(f"[WARNING] Batch {batch_num} failed")
            
            # Перевіряємо прогрес
            stats = self.get_processing_stats()
            logger.info(f"[PROGRESS] {stats['completion_rate']:.1f}% complete ({stats['remaining_records']:,} remaining)")
            
            if stats['remaining_records'] == 0:
                logger.info("[COMPLETE] All records processed!")
                break
            
            # Відпочинок між батчами
            time.sleep(10)
        
        self.print_final_stats()
    
    def print_final_stats(self):
        """Виведення фінальної статистики"""
        duration = datetime.now() - self.stats['start_time']
        final_stats = self.get_processing_stats()
        
        logger.info("\n" + "="*60)
        logger.info("[FINAL STATS] MASS PROCESSING COMPLETED")
        logger.info("="*60)
        logger.info(f"[TIME] Duration: {duration}")
        logger.info(f"[SUCCESS] Successful batches: {self.stats['successful_batches']}")
        logger.info(f"[FAILED] Failed batches: {self.stats['failed_batches']}")
        logger.info(f"[TOTAL] Total records in osm_raw: {final_stats['total_records']:,}")
        logger.info(f"[RELEVANT] Relevant records: {final_stats['relevant_records']:,}")
        logger.info(f"[PROCESSED] Processed: {final_stats['processed_records']:,}")
        logger.info(f"[REMAINING] Remaining: {final_stats['remaining_records']:,}")
        logger.info(f"[PROGRESS] Progress: {final_stats['completion_rate']:.1f}%")

def main():
    """Головна функція"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Mass processing of osm_raw')
    parser.add_argument('--mode', choices=['regions', 'universal'], default='regions', 
                       help='Processing mode: by regions or universal batches')
    parser.add_argument('--limit', type=int, default=50000, help='Records per batch')
    parser.add_argument('--batch-size', type=int, default=5000, help='Internal batch size')
    parser.add_argument('--stats-only', action='store_true', help='Show statistics only')
    
    args = parser.parse_args()
    
    manager = MassProcessingManager()
    
    if args.stats_only:
        # Тільки статистика
        stats = manager.get_processing_stats()
        regions = manager.get_regions_list()
        
        print("\n[CURRENT STATS]")
        print(f"Total records: {stats['total_records']:,}")
        print(f"Relevant records: {stats['relevant_records']:,}")
        print(f"Processed: {stats['processed_records']:,}")
        print(f"Remaining: {stats['remaining_records']:,}")
        print(f"Progress: {stats['completion_rate']:.1f}%")
        
        print(f"\n[REGIONS] ({len(regions)} total):")
        for region in regions[:10]:  # Топ 10
            print(f"  {region['region_name']}: {region['relevant_records']:,} relevant")
    
    elif args.mode == 'regions':
        manager.process_all_regions(args.limit, args.batch_size)
    elif args.mode == 'universal':
        manager.process_universal_batches(args.limit, args.batch_size)

if __name__ == "__main__":
    main()