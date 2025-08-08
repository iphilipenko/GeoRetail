#!/usr/bin/env python3
"""
Batch Approval CLI Tool
CLI інтерфейс для batch затвердження, відхилення та перегляду brand candidates
"""

import sys
import argparse
import logging
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from psycopg2.extras import RealDictCursor

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


class BatchApprovalTool:
    """CLI інструмент для batch операцій з brand candidates"""
    
    def __init__(self, db_connection_string: str):
        self.db_connection_string = db_connection_string
        self.brand_manager = BrandManager(db_connection_string)
        
        logger.info("✅ BatchApprovalTool ініціалізовано")
    
    def list_candidates(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Показати список кандидатів за фільтрами"""
        logger.info(f"🔍 Пошук кандидатів з фільтрами: {filters}")
        
        try:
            candidates = self.brand_manager.get_candidates_for_review(**filters)
            
            if not candidates:
                print("📝 Не знайдено кандидатів за вказаними фільтрами")
                return []
            
            # Показуємо результати
            print(f"\n📋 ЗНАЙДЕНО {len(candidates)} КАНДИДАТІВ:")
            print("=" * 80)
            print(f"{'#':<3} {'Name':<25} {'Status':<12} {'Freq':<6} {'Regions':<8} {'Conf':<6} {'Group':<12}")
            print("-" * 80)
            
            for i, candidate in enumerate(candidates, 1):
                regions_count = len(candidate.get('locations', []))
                conf = candidate.get('confidence_score') or 0.0
                group = candidate.get('suggested_functional_group', 'N/A')[:11]
                
                print(f"{i:<3} {candidate['name'][:24]:<25} {candidate['status']:<12} "
                      f"{candidate['frequency']:<6} {regions_count:<8} {conf:<6.3f} {group:<12}")
            
            print("=" * 80)
            return candidates
            
        except Exception as e:
            logger.error(f"Помилка отримання кандидатів: {e}")
            return []
    
    def approve_candidates(self, filters: Dict[str, Any], processed_by: str = "admin") -> Dict[str, int]:
        """Batch затвердження кандидатів"""
        logger.info(f"✅ Затвердження кандидатів з фільтрами: {filters}")
        
        # Спочатку показуємо що будемо затверджувати
        candidates = self.brand_manager.get_candidates_for_review(**filters)
        
        if not candidates:
            print("📝 Не знайдено кандидатів для затвердження")
            return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 0}
        
        print(f"\n⚠️  ЗАТВЕРДЖЕННЯ {len(candidates)} КАНДИДАТІВ:")
        print("Це створить нові бренди в custom_brands таблиці!")
        
        # Показуємо перші 5 для підтвердження
        for i, candidate in enumerate(candidates[:5], 1):
            regions_count = len(candidate.get('locations', []))
            print(f"   {i}. \"{candidate['name']}\" → {candidate.get('suggested_canonical_name', 'N/A')} "
                  f"({candidate['frequency']} locations, {regions_count} regions)")
        
        if len(candidates) > 5:
            print(f"   ... і ще {len(candidates) - 5} кандидатів")
        
        # Підтвердження
        try:
            confirm = input(f"\n❓ Продовжити затвердження? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                print("❌ Операцію скасовано")
                return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 0}
        except KeyboardInterrupt:
            print("\n❌ Операцію скасовано користувачем")
            return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 0}
        
        # Виконуємо batch approval
        try:
            result = self.brand_manager.batch_approve_candidates(
                filters=filters,
                action='approve',
                processed_by=processed_by
            )
            
            print(f"\n✅ РЕЗУЛЬТАТ ЗАТВЕРДЖЕННЯ:")
            print(f"   Оброблено: {result['total_processed']}")
            print(f"   Затверджено: {result['approved']}")
            print(f"   Помилки: {result['errors']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка batch approval: {e}")
            print(f"❌ Помилка затвердження: {e}")
            return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 1}
    
    def reject_candidates(self, filters: Dict[str, Any], processed_by: str = "admin") -> Dict[str, int]:
        """Batch відхилення кандидатів"""
        logger.info(f"❌ Відхилення кандидатів з фільтрами: {filters}")
        
        # Спочатку показуємо що будемо відхиляти
        candidates = self.brand_manager.get_candidates_for_review(**filters)
        
        if not candidates:
            print("📝 Не знайдено кандидатів для відхилення")
            return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 0}
        
        print(f"\n⚠️  ВІДХИЛЕННЯ {len(candidates)} КАНДИДАТІВ:")
        
        # Показуємо перші 5 для підтвердження
        for i, candidate in enumerate(candidates[:5], 1):
            regions_count = len(candidate.get('locations', []))
            print(f"   {i}. \"{candidate['name']}\" "
                  f"({candidate['frequency']} locations, {regions_count} regions)")
        
        if len(candidates) > 5:
            print(f"   ... і ще {len(candidates) - 5} кандидатів")
        
        # Підтвердження
        try:
            confirm = input(f"\n❓ Продовжити відхилення? (y/N): ").strip().lower()
            if confirm not in ['y', 'yes']:
                print("❌ Операцію скасовано")
                return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 0}
        except KeyboardInterrupt:
            print("\n❌ Операцію скасовано користувачем")
            return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 0}
        
        # Виконуємо batch rejection
        try:
            result = self.brand_manager.batch_approve_candidates(
                filters=filters,
                action='reject',
                processed_by=processed_by
            )
            
            print(f"\n❌ РЕЗУЛЬТАТ ВІДХИЛЕННЯ:")
            print(f"   Оброблено: {result['total_processed']}")
            print(f"   Відхилено: {result['rejected']}")
            print(f"   Помилки: {result['errors']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Помилка batch rejection: {e}")
            print(f"❌ Помилка відхилення: {e}")
            return {'total_processed': 0, 'approved': 0, 'rejected': 0, 'errors': 1}
    
    def show_statistics(self):
        """Показати загальну статистику"""
        logger.info("📊 Отримання статистики")
        
        try:
            with psycopg2.connect(self.db_connection_string) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Статистика по статусах
                    cur.execute("""
                        SELECT status, COUNT(*) as count,
                               AVG(confidence_score) as avg_confidence,
                               AVG(frequency) as avg_frequency
                        FROM osm_ukraine.brand_candidates
                        GROUP BY status
                        ORDER BY count DESC
                    """)
                    
                    status_stats = cur.fetchall()
                    
                    # Топ кандидатів по впевненості
                    cur.execute("""
                        SELECT name, status, frequency, confidence_score,
                               array_length(locations, 1) as regions_count
                        FROM osm_ukraine.brand_candidates
                        WHERE status IN ('approved', 'reviewing')
                        ORDER BY confidence_score DESC, frequency DESC
                        LIMIT 10
                    """)
                    
                    top_candidates = cur.fetchall()
                    
                    # Недавня активність
                    cur.execute("""
                        SELECT reviewed_by, COUNT(*) as count,
                               MAX(reviewed_at) as last_activity
                        FROM osm_ukraine.brand_candidates
                        WHERE reviewed_at IS NOT NULL
                        GROUP BY reviewed_by
                        ORDER BY last_activity DESC
                        LIMIT 5
                    """)
                    
                    recent_activity = cur.fetchall()
            
            # Виводимо статистику
            print("\n📊 СТАТИСТИКА BRAND CANDIDATES")
            print("=" * 60)
            
            print("\n📈 ПО СТАТУСАХ:")
            for stat in status_stats:
                avg_conf = stat['avg_confidence'] or 0
                avg_freq = stat['avg_frequency'] or 0
                print(f"   {stat['status']:<12}: {stat['count']:>5} candidates "
                      f"(avg conf: {avg_conf:.3f}, avg freq: {avg_freq:.1f})")
            
            if top_candidates:
                print(f"\n🏆 ТОП КАНДИДАТІВ ЗА ВПЕВНЕНІСТЮ:")
                for i, candidate in enumerate(top_candidates, 1):
                    regions = candidate['regions_count'] or 0
                    print(f"   {i:2}. \"{candidate['name'][:30]}\" - {candidate['status']} "
                          f"(conf: {candidate['confidence_score']:.3f}, "
                          f"freq: {candidate['frequency']}, regions: {regions})")
            
            if recent_activity:
                print(f"\n⏰ НЕДАВНЯ АКТИВНІСТЬ:")
                for activity in recent_activity:
                    last_time = activity['last_activity'].strftime("%Y-%m-%d %H:%M")
                    print(f"   {activity['reviewed_by']:<20}: {activity['count']:>3} операцій "
                          f"(останні: {last_time})")
            
            print("=" * 60)
            
        except Exception as e:
            logger.error(f"Помилка отримання статистики: {e}")
            print(f"❌ Помилка отримання статистики: {e}")
    
    def show_batch_history(self, limit: int = 10):
        """Показати історію batch операцій"""
        logger.info(f"📚 Отримання історії batch операцій (limit: {limit})")
        
        try:
            history = self.brand_manager.get_batch_history(limit=limit)
            
            if not history:
                print("📝 Немає історії batch операцій")
                return
            
            print(f"\n📚 ІСТОРІЯ BATCH ОПЕРАЦІЙ (останні {len(history)}):")
            print("=" * 80)
            print(f"{'Date':<16} {'Action':<15} {'By':<20} {'Processed':<10} {'Approved':<9} {'Rejected':<9}")
            print("-" * 80)
            
            for entry in history:
                date_str = entry['processed_at'].strftime("%Y-%m-%d %H:%M")
                processed_by = entry['processed_by'][:19]
                
                print(f"{date_str:<16} {entry['action']:<15} {processed_by:<20} "
                      f"{entry['candidates_processed']:<10} "
                      f"{entry['candidates_approved'] or 0:<9} "
                      f"{entry['candidates_rejected'] or 0:<9}")
            
            print("=" * 80)
            
        except Exception as e:
            logger.error(f"Помилка отримання історії: {e}")
            print(f"❌ Помилка отримання історії: {e}")


def create_parser() -> argparse.ArgumentParser:
    """Створення CLI parser"""
    parser = argparse.ArgumentParser(
        description="Batch Approval Tool для управління brand candidates",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Приклади використання:

  # Показати всі кандидатів зі статусом 'approved'
  python batch_approval.py list --status approved

  # Затвердити кандидатів з високою впевненістю
  python batch_approval.py approve --status approved --min-confidence 0.8

  # Відхилити кандидатів з низькою впевненістю  
  python batch_approval.py reject --status rejected --max-confidence 0.3

  # Показати кандидатів для review з категорією 'retail'
  python batch_approval.py list --status reviewing --category retail

  # Показати загальну статистику
  python batch_approval.py stats

  # Показати історію операцій
  python batch_approval.py history
        """
    )
    
    # Основні команди
    subparsers = parser.add_subparsers(dest='action', help='Доступні дії')
    
    # List команда
    list_parser = subparsers.add_parser('list', help='Показати список кандидатів')
    list_parser.add_argument('--status', choices=['new', 'approved', 'reviewing', 'rejected'], 
                           help='Фільтр по статусу')
    list_parser.add_argument('--min-confidence', type=float, help='Мінімальна впевненість')
    list_parser.add_argument('--max-confidence', type=float, help='Максимальна впевненість')
    list_parser.add_argument('--min-frequency', type=int, help='Мінімальна частота')
    list_parser.add_argument('--category', help='Фільтр по категорії')
    list_parser.add_argument('--limit', type=int, default=50, help='Максимальна кількість результатів')
    
    # Approve команда
    approve_parser = subparsers.add_parser('approve', help='Затвердити кандидатів')
    approve_parser.add_argument('--status', choices=['approved', 'reviewing'], required=True,
                              help='Статус кандидатів для затвердження')
    approve_parser.add_argument('--min-confidence', type=float, help='Мінімальна впевненість')
    approve_parser.add_argument('--min-frequency', type=int, help='Мінімальна частота')
    approve_parser.add_argument('--category', help='Фільтр по категорії')
    approve_parser.add_argument('--processed-by', default='admin', help='Хто обробляє')
    
    # Reject команда
    reject_parser = subparsers.add_parser('reject', help='Відхилити кандидатів')
    reject_parser.add_argument('--status', choices=['rejected', 'reviewing'], required=True,
                             help='Статус кандидатів для відхилення')
    reject_parser.add_argument('--max-confidence', type=float, help='Максимальна впевненість')
    reject_parser.add_argument('--max-frequency', type=int, help='Максимальна частота')
    reject_parser.add_argument('--category', help='Фільтр по категорії')
    reject_parser.add_argument('--processed-by', default='admin', help='Хто обробляє')
    
    # Stats команда
    subparsers.add_parser('stats', help='Показати загальну статистику')
    
    # History команда
    history_parser = subparsers.add_parser('history', help='Показати історію batch операцій')
    history_parser.add_argument('--limit', type=int, default=10, help='Кількість записів')
    
    return parser


def build_filters(args) -> Dict[str, Any]:
    """Побудова фільтрів з аргументів CLI"""
    filters = {}
    
    if hasattr(args, 'status') and args.status:
        filters['status'] = args.status
    
    if hasattr(args, 'min_confidence') and args.min_confidence is not None:
        filters['min_confidence'] = args.min_confidence
    
    if hasattr(args, 'max_confidence') and args.max_confidence is not None:
        # Для max_confidence використовуємо workaround
        filters['max_confidence'] = args.max_confidence
    
    if hasattr(args, 'min_frequency') and args.min_frequency is not None:
        filters['min_frequency'] = args.min_frequency
    
    if hasattr(args, 'max_frequency') and args.max_frequency is not None:
        filters['max_frequency'] = args.max_frequency
    
    if hasattr(args, 'category') and args.category:
        filters['category'] = args.category
    
    if hasattr(args, 'limit') and args.limit:
        filters['limit'] = args.limit
    
    return filters


def main():
    """Головна функція"""
    parser = create_parser()
    args = parser.parse_args()
    
    if not args.action:
        parser.print_help()
        return 1
    
    try:
        # Створюємо tool
        tool = BatchApprovalTool(DB_CONNECTION_STRING)
        
        if args.action == 'list':
            filters = build_filters(args)
            tool.list_candidates(filters)
            
        elif args.action == 'approve':
            filters = build_filters(args)
            result = tool.approve_candidates(filters, args.processed_by)
            
        elif args.action == 'reject':
            filters = build_filters(args)
            result = tool.reject_candidates(filters, args.processed_by)
            
        elif args.action == 'stats':
            tool.show_statistics()
            
        elif args.action == 'history':
            tool.show_batch_history(args.limit)
        
        return 0
        
    except KeyboardInterrupt:
        print("\n❌ Операцію скасовано користувачем")
        return 1
    except Exception as e:
        logger.error(f"💥 Фатальна помилка: {e}")
        print(f"❌ Помилка: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)