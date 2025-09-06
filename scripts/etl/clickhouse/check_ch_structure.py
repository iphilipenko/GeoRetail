"""
scripts/etl/clickhouse/check_ch_structure.py
Діагностичний скрипт для перевірки структури таблиць ClickHouse
Показує всі поля таблиці admin_analytics та їх типи
"""

import logging
from config import CH_CONFIG
from utils import ClickHouseConnector

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_table_structure():
    """
    Перевіряє структуру таблиці admin_analytics в ClickHouse
    """
    logger.info("=" * 60)
    logger.info("🔍 ПЕРЕВІРКА СТРУКТУРИ ТАБЛИЦІ admin_analytics")
    logger.info("=" * 60)
    
    ch = ClickHouseConnector(CH_CONFIG)
    
    with ch.connect():
        # Отримуємо структуру таблиці
        query = """
        SELECT 
            name as column_name,
            type as data_type,
            comment
        FROM system.columns
        WHERE database = 'geo_analytics' 
          AND table = 'admin_analytics'
        ORDER BY position
        """
        
        columns = ch.client.execute(query)
        
        logger.info(f"\n📊 Знайдено {len(columns)} колонок:\n")
        
        # Групуємо колонки по категоріях
        categories = {
            'Ідентифікація': [],
            'Базові характеристики': [],
            'Населення': [],
            'Економічна активність': [],
            'POI та конкуренція': [],
            'Транспорт': [],
            'Індекси': [],
            'Bins': [],
            'Bivariate': [],
            'Метадані': []
        }
        
        # Класифікуємо колонки
        for col_name, col_type, comment in columns:
            if col_name in ['admin_id', 'admin_level', 'admin_name', 'admin_name_uk', 'parent_id', 'osm_id']:
                categories['Ідентифікація'].append((col_name, col_type))
            elif col_name in ['area_km2', 'perimeter_km', 'hex_count_r7', 'hex_count_r8', 'hex_count_r9', 'hex_count_r10']:
                categories['Базові характеристики'].append((col_name, col_type))
            elif 'population' in col_name or 'residential' in col_name or 'building' in col_name:
                categories['Населення'].append((col_name, col_type))
            elif 'bank' in col_name or 'mcc' in col_name or 'economic' in col_name:
                categories['Економічна активність'].append((col_name, col_type))
            elif 'poi' in col_name or 'competitor' in col_name or 'retail' in col_name or 'food' in col_name or 'services' in col_name or 'brand' in col_name:
                categories['POI та конкуренція'].append((col_name, col_type))
            elif 'road' in col_name or 'transport' in col_name or 'connectivity' in col_name or 'accessibility' in col_name:
                categories['Транспорт'].append((col_name, col_type))
            elif '_score' in col_name or '_index' in col_name and '_bin' not in col_name:
                categories['Індекси'].append((col_name, col_type))
            elif '_bin' in col_name or 'tercile' in col_name or 'quintile' in col_name:
                categories['Bins'].append((col_name, col_type))
            elif 'bivariate' in col_name:
                categories['Bivariate'].append((col_name, col_type))
            else:
                categories['Метадані'].append((col_name, col_type))
        
        # Виводимо по категоріях
        for category, cols in categories.items():
            if cols:
                logger.info(f"\n{category} ({len(cols)} полів):")
                for col_name, col_type in cols:
                    logger.info(f"  - {col_name}: {col_type}")
        
        # Перевіряємо наявність критичних полів
        logger.info("\n" + "=" * 60)
        logger.info("🔎 ПЕРЕВІРКА КРИТИЧНИХ ПОЛІВ:")
        logger.info("=" * 60)
        
        all_columns = [col[0] for col in columns]
        
        # Поля, які можуть бути проблемними
        fields_to_check = [
            'unique_brands_count',
            'retail_count', 
            'food_count',
            'services_count',
            'transport_count',
            'commercial_activity_score',
            'retail_potential_score',
            'transport_accessibility_score'
        ]
        
        missing_fields = []
        existing_fields = []
        
        for field in fields_to_check:
            if field in all_columns:
                existing_fields.append(field)
                logger.info(f"✅ {field} - ІСНУЄ")
            else:
                missing_fields.append(field)
                logger.warning(f"❌ {field} - ВІДСУТНЄ")
        
        # Підсумок
        logger.info("\n" + "=" * 60)
        logger.info("📋 ПІДСУМОК:")
        logger.info("=" * 60)
        logger.info(f"Всього полів в таблиці: {len(columns)}")
        logger.info(f"Існуючих критичних полів: {len(existing_fields)}")
        logger.info(f"Відсутніх критичних полів: {len(missing_fields)}")
        
        if missing_fields:
            logger.info("\n⚠️ УВАГА! Ці поля потрібно додати в ClickHouse або видалити з ETL:")
            for field in missing_fields:
                logger.info(f"  - {field}")
                
            # Генеруємо SQL для додавання полів
            logger.info("\n💡 SQL для додавання відсутніх полів:")
            logger.info("-" * 40)
            for field in missing_fields:
                if 'count' in field or 'total' in field:
                    data_type = 'UInt32'
                elif 'score' in field or 'index' in field:
                    data_type = 'Float32'
                else:
                    data_type = 'Nullable(Float32)'
                    
                logger.info(f"ALTER TABLE geo_analytics.admin_analytics ADD COLUMN IF NOT EXISTS {field} {data_type};")
        else:
            logger.info("\n✅ Всі критичні поля присутні!")
        
        return missing_fields, existing_fields


def check_data_in_table():
    """
    Перевіряє наявність даних в таблиці
    """
    logger.info("\n" + "=" * 60)
    logger.info("📊 ПЕРЕВІРКА ДАНИХ В ТАБЛИЦІ")
    logger.info("=" * 60)
    
    ch = ClickHouseConnector(CH_CONFIG)
    
    with ch.connect():
        # Кількість записів
        count = ch.client.execute("SELECT COUNT(*) FROM geo_analytics.admin_analytics")[0][0]
        logger.info(f"\nКількість записів: {count}")
        
        if count > 0:
            # Статистика по рівнях
            stats = ch.client.execute("""
                SELECT 
                    admin_level,
                    COUNT(*) as cnt
                FROM geo_analytics.admin_analytics
                GROUP BY admin_level
                ORDER BY admin_level
            """)
            
            logger.info("\nРозподіл по рівнях:")
            for level, cnt in stats:
                logger.info(f"  Рівень {level}: {cnt} записів")


def main():
    """Головна функція"""
    missing_fields, existing_fields = check_table_structure()
    check_data_in_table()
    
    # Повертаємо код виходу в залежності від результату
    if missing_fields:
        logger.warning(f"\n⚠️ Знайдено {len(missing_fields)} відсутніх полів!")
        return 1
    else:
        logger.info("\n✅ Структура таблиці відповідає вимогам ETL!")
        return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())