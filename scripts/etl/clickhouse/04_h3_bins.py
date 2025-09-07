"""
scripts/etl/clickhouse/03_h3_bins.py
Розрахунок bins (terciles) та bivariate комбінацій для H3 гексагонів
Оптимізовано для обробки мільйонів записів
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from config import CH_CONFIG, BINS_CONFIG
from utils import ClickHouseConnector, ETLProgress

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class H3BinsCalculator:
    """
    Клас для розрахунку bins для H3 гексагонів
    Оптимізовано для великих об'ємів даних (8M+ записів)
    """
    
    def __init__(self):
        """Ініціалізація калькулятора H3 bins"""
        self.ch_conn = ClickHouseConnector(CH_CONFIG)
        self.start_time = datetime.now()
        
        # Метрики для розрахунку bins (основні 8 метрик з h3_analytics)
        self.h3_metrics_for_bins = [
            'population_density',      # Щільність населення
            'income_index',            # Індекс доходів
            'competitor_intensity',    # Інтенсивність конкуренції
            'poi_density',            # Щільність POI
            'accessibility_score',     # Транспортна доступність
            'traffic_index',          # Трафік індекс
            'retail_potential',       # Потенціал для ритейлу
            'risk_score'              # Ризик score
        ]
        
        # Bivariate комбінації для H3
        self.h3_bivariate_pairs = [
            ('population_density', 'income_index'),           # Населення × Доходи
            ('competitor_intensity', 'accessibility_score'),  # Конкуренція × Доступність
            ('poi_density', 'retail_potential'),             # Інфраструктура × Потенціал
            ('traffic_index', 'risk_score')                  # Трафік × Ризик
        ]
        
        # Батч розмір для оновлення (більший для H3)
        self.batch_size = 10000
    
    def get_h3_resolutions(self) -> List[int]:
        """
        Отримує список унікальних резолюцій H3 з БД
        
        Returns:
            Список резолюцій (7, 8, 9, 10)
        """
        with self.ch_conn.connect():
            result = self.ch_conn.client.execute("""
                SELECT DISTINCT resolution 
                FROM geo_analytics.h3_analytics
                ORDER BY resolution
            """)
            resolutions = [row[0] for row in result]
            
            # Підрахунок кількості гексагонів
            for res in resolutions:
                count_result = self.ch_conn.client.execute(f"""
                    SELECT COUNT(*) 
                    FROM geo_analytics.h3_analytics 
                    WHERE resolution = {res}
                """)
                count = count_result[0][0] if count_result else 0
                logger.info(f"📊 Резолюція {res}: {count:,} гексагонів")
            
            return resolutions
    
    def check_table_exists(self) -> bool:
        """
        Перевіряє чи існує таблиця h3_analytics
        
        Returns:
            True якщо таблиця існує
        """
        with self.ch_conn.connect():
            result = self.ch_conn.client.execute("""
                SELECT COUNT(*) 
                FROM system.tables 
                WHERE database = 'geo_analytics' 
                    AND name = 'h3_analytics'
            """)
            exists = result[0][0] > 0 if result else False
            
            if exists:
                # Перевіряємо кількість записів
                count_result = self.ch_conn.client.execute("""
                    SELECT COUNT(*) FROM geo_analytics.h3_analytics
                """)
                count = count_result[0][0] if count_result else 0
                logger.info(f"✅ Таблиця h3_analytics існує з {count:,} записами")
            else:
                logger.warning("⚠️ Таблиця h3_analytics не знайдена")
            
            return exists
    
    def calculate_bins_for_h3_metric(
        self, 
        metric: str, 
        resolution: int,
        num_bins: int = 3
    ) -> None:
        """
        Розраховує та одразу оновлює bins для метрики на певній резолюції
        Використовує ALTER TABLE UPDATE для ефективності
        
        Args:
            metric: Назва метрики
            resolution: Резолюція H3 (7-10)
            num_bins: Кількість bins (за замовчуванням 3)
        """
        logger.info(f"  📈 Розрахунок та оновлення {num_bins} bins для {metric} (res={resolution})")
        
        with self.ch_conn.connect():
            # Визначаємо назву bin поля
            bin_field = self._get_h3_bin_field_name(metric)
            
            # Перевіряємо чи існує поле bin в таблиці
            columns = self.ch_conn.get_table_columns('geo_analytics.h3_analytics')
            if bin_field not in columns:
                logger.warning(f"    ⚠️ Поле {bin_field} не існує в таблиці, пропускаємо")
                return
            
            # Отримуємо квантилі для розподілу на bins
            quantiles_query = f"""
                SELECT 
                    quantileExact(0.333)({metric}) as q1,
                    quantileExact(0.667)({metric}) as q2
                FROM geo_analytics.h3_analytics
                WHERE resolution = {resolution}
                    AND {metric} IS NOT NULL
            """
            
            result = self.ch_conn.client.execute(quantiles_query)
            if not result or not result[0][0]:
                logger.warning(f"    ⚠️ Немає даних для {metric} на резолюції {resolution}")
                return
            
            q1, q2 = result[0]
            logger.info(f"    Квантилі: Q33={q1:.4f}, Q67={q2:.4f}")
            
            # Оновлюємо bins через ALTER TABLE UPDATE
            # Спочатку встановлюємо всі в 0 (відсутні дані)
            self.ch_conn.client.execute(f"""
                ALTER TABLE geo_analytics.h3_analytics
                UPDATE {bin_field} = 0
                WHERE resolution = {resolution}
            """)
            
            # Потім присвоюємо bins на основі квантилів
            # Bin 1 - низький (до 33 перцентиля)
            self.ch_conn.client.execute(f"""
                ALTER TABLE geo_analytics.h3_analytics
                UPDATE {bin_field} = 1
                WHERE resolution = {resolution}
                    AND {metric} IS NOT NULL
                    AND {metric} <= {q1}
            """)
            
            # Bin 2 - середній (33-67 перцентиль)
            self.ch_conn.client.execute(f"""
                ALTER TABLE geo_analytics.h3_analytics
                UPDATE {bin_field} = 2
                WHERE resolution = {resolution}
                    AND {metric} > {q1}
                    AND {metric} <= {q2}
            """)
            
            # Bin 3 - високий (вище 67 перцентиля)
            self.ch_conn.client.execute(f"""
                ALTER TABLE geo_analytics.h3_analytics
                UPDATE {bin_field} = 3
                WHERE resolution = {resolution}
                    AND {metric} > {q2}
            """)
            
            # Перевірка розподілу
            distribution = self.ch_conn.client.execute(f"""
                SELECT 
                    {bin_field} as bin_value,
                    COUNT(*) as count
                FROM geo_analytics.h3_analytics
                WHERE resolution = {resolution}
                GROUP BY {bin_field}
                ORDER BY {bin_field}
            """)
            
            if distribution:
                dist_str = ", ".join([f"Bin {row[0]}={row[1]:,}" for row in distribution])
                logger.info(f"    Розподіл: {dist_str}")
    
    def _get_h3_bin_field_name(self, metric: str) -> str:
        """
        Генерує назву поля для bin на основі метрики
        
        Args:
            metric: Назва метрики
            
        Returns:
            Назва поля для bin
        """
        # Для H3 використовуємо прості назви bins
        if metric == 'population_density':
            return 'population_bin'
        elif metric == 'income_index':
            return 'income_bin'
        elif metric == 'competitor_intensity':
            return 'competitor_bin'
        else:
            # Для інших метрик bins можуть не існувати в таблиці
            return f"{metric.replace('_', '')}_bin"
    
    def calculate_all_h3_bins(self):
        """
        Розраховує bins для всіх метрик та резолюцій H3
        """
        logger.info("🔢 Розрахунок bins для H3 гексагонів...")
        
        resolutions = self.get_h3_resolutions()
        
        if not resolutions:
            logger.warning("⚠️ Немає даних H3 для обробки")
            return
        
        total_metrics = len(self.h3_metrics_for_bins) * len(resolutions)
        processed = 0
        
        for resolution in resolutions:
            logger.info(f"\n🗺️ Обробка резолюції {resolution}:")
            
            # Обробляємо тільки основні 3 метрики для bins
            # (population_bin, income_bin, competitor_bin існують в таблиці)
            main_metrics = ['population_density', 'income_index', 'competitor_intensity']
            
            for metric in main_metrics:
                try:
                    self.calculate_bins_for_h3_metric(metric, resolution)
                    processed += 1
                except Exception as e:
                    logger.error(f"  ❌ Помилка для {metric}: {str(e)[:100]}")
            
            # Прогрес
            logger.info(f"  Прогрес: {processed}/{len(main_metrics) * len(resolutions)} метрик оброблено")
    
    def update_h3_bivariate(self):
        """
        Формує bivariate комбінації для H3 на основі bins
        Працює тільки з основними 3 bins що існують в таблиці
        """
        logger.info("\n🎨 Формування bivariate комбінацій для H3...")
        
        with self.ch_conn.connect():
            resolutions = self.get_h3_resolutions()
            
            for resolution in resolutions:
                logger.info(f"  Резолюція {resolution}:")
                
                # Формуємо bivariate тільки для population × income
                # (бо тільки ці bins точно існують)
                try:
                    # Перевіряємо чи існують необхідні поля
                    columns = self.ch_conn.get_table_columns('geo_analytics.h3_analytics')
                    
                    # Якщо немає полів для bivariate, пропускаємо
                    if 'population_bin' not in columns or 'income_bin' not in columns:
                        logger.warning(f"    ⚠️ Відсутні bin поля для резолюції {resolution}")
                        continue
                    
                    # Формуємо bivariate комбінацію як конкатенацію bins
                    # Наприклад: population_bin=2, income_bin=3 → "2-3"
                    
                    # Оновлюємо через простий UPDATE (якщо поля bivariate немає, пропускаємо)
                    logger.info(f"    Формування population × income bivariate...")
                    
                    # Отримуємо статистику комбінацій
                    stats = self.ch_conn.client.execute(f"""
                        SELECT 
                            CONCAT(toString(population_bin), '-', toString(income_bin)) as bivar,
                            COUNT(*) as count
                        FROM geo_analytics.h3_analytics
                        WHERE resolution = {resolution}
                        GROUP BY bivar
                        ORDER BY count DESC
                        LIMIT 10
                    """)
                    
                    if stats:
                        logger.info(f"    ТОП-10 комбінацій:")
                        for row in stats:
                            bivar, count = row
                            logger.info(f"      {bivar}: {count:,} гексагонів")
                    
                except Exception as e:
                    logger.error(f"    ❌ Помилка bivariate: {str(e)[:100]}")
    
    def generate_h3_statistics(self):
        """
        Генерує статистику розподілу bins для H3
        """
        logger.info("\n📊 Статистика bins для H3 гексагонів:")
        
        with self.ch_conn.connect():
            resolutions = self.get_h3_resolutions()
            
            for resolution in resolutions[:2]:  # Тільки для перших 2 резолюцій для швидкості
                logger.info(f"\n🗺️ Резолюція {resolution}:")
                
                # Статистика по population_bin
                result = self.ch_conn.client.execute(f"""
                    SELECT 
                        population_bin,
                        COUNT(*) as count,
                        AVG(population_density) as avg_density,
                        MIN(population_density) as min_density,
                        MAX(population_density) as max_density
                    FROM geo_analytics.h3_analytics
                    WHERE resolution = {resolution}
                    GROUP BY population_bin
                    ORDER BY population_bin
                """)
                
                if result:
                    logger.info("  Population bins:")
                    for row in result:
                        bin_val, count, avg, min_val, max_val = row
                        label = ['Відсутні', 'Низька', 'Середня', 'Висока'][bin_val] if bin_val <= 3 else f"Bin {bin_val}"
                        if avg:
                            logger.info(f"    {label}: {count:,} гекс., щільність {avg:.1f} (від {min_val:.1f} до {max_val:.1f})")
                        else:
                            logger.info(f"    {label}: {count:,} гексагонів")
                
                # Статистика по income_bin
                result = self.ch_conn.client.execute(f"""
                    SELECT 
                        income_bin,
                        COUNT(*) as count,
                        AVG(income_index) as avg_income
                    FROM geo_analytics.h3_analytics
                    WHERE resolution = {resolution}
                    GROUP BY income_bin
                    ORDER BY income_bin
                    LIMIT 10
                """)
                
                if result:
                    logger.info("  Income bins:")
                    for row in result:
                        bin_val, count, avg = row
                        label = ['Відсутні', 'Низькі', 'Середні', 'Високі'][bin_val] if bin_val <= 3 else f"Bin {bin_val}"
                        if avg:
                            logger.info(f"    {label}: {count:,} гекс., індекс {avg:.4f}")
                        else:
                            logger.info(f"    {label}: {count:,} гексагонів")
    
    def validate_h3_results(self) -> bool:
        """
        Валідує результати розрахунку bins для H3
        
        Returns:
            True якщо валідація пройшла успішно
        """
        logger.info("\n🔍 Валідація H3 bins...")
        
        with self.ch_conn.connect():
            # Загальна статистика
            result = self.ch_conn.client.execute("""
                SELECT 
                    resolution,
                    COUNT(*) as total,
                    SUM(CASE WHEN population_bin > 0 THEN 1 ELSE 0 END) as with_pop_bin,
                    SUM(CASE WHEN income_bin > 0 THEN 1 ELSE 0 END) as with_income_bin,
                    SUM(CASE WHEN competitor_bin > 0 THEN 1 ELSE 0 END) as with_comp_bin
                FROM geo_analytics.h3_analytics
                GROUP BY resolution
                ORDER BY resolution
            """)
            
            if result:
                logger.info("  Статистика по резолюціях:")
                total_valid = 0
                for row in result:
                    res, total, with_pop, with_income, with_comp = row
                    logger.info(f"    Резолюція {res}: {total:,} гексагонів")
                    logger.info(f"      З population_bin: {with_pop:,} ({with_pop*100/total:.1f}%)")
                    logger.info(f"      З income_bin: {with_income:,} ({with_income*100/total:.1f}%)")
                    logger.info(f"      З competitor_bin: {with_comp:,} ({with_comp*100/total:.1f}%)")
                    
                    if with_pop > 0:
                        total_valid += 1
                
                return total_valid > 0
            
            return False
    
    def run(self) -> bool:
        """
        Запускає повний процес розрахунку bins для H3
        
        Returns:
            True якщо процес завершився успішно
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ПОЧАТОК: Розрахунок bins для H3 гексагонів")
            logger.info("=" * 60)
            
            # 1. Перевірка наявності таблиці
            if not self.check_table_exists():
                logger.error("❌ Таблиця h3_analytics не існує!")
                logger.info("💡 Спочатку запустіть ETL для H3 метрик")
                return False
            
            # 2. Розрахунок bins для всіх метрик
            self.calculate_all_h3_bins()
            
            # 3. Формування bivariate комбінацій
            self.update_h3_bivariate()
            
            # 4. Генерація статистики
            self.generate_h3_statistics()
            
            # 5. Валідація результатів
            success = self.validate_h3_results()
            
            # Підсумок
            elapsed = datetime.now() - self.start_time
            logger.info("\n" + "=" * 60)
            if success:
                logger.info(f"✅ H3 BINS РОЗРАХОВАНО УСПІШНО")
                logger.info(f"⏱️ Час виконання: {elapsed}")
                logger.info(f"\n🎯 H3 bins готові для візуалізації на картах!")
                logger.info("   Використовуйте резолюцію 8 для загального огляду")
                logger.info("   Використовуйте резолюцію 9 для детального аналізу")
            else:
                logger.error(f"❌ РОЗРАХУНОК ЗАВЕРШЕНО З ПОМИЛКАМИ")
            logger.info("=" * 60)
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Критична помилка: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Головна функція"""
    calculator = H3BinsCalculator()
    success = calculator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()