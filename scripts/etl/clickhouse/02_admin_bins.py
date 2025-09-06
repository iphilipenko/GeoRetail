"""
scripts/etl/clickhouse/02_admin_bins.py
Розрахунок bins (terciles) та bivariate комбінацій для адміністративних одиниць
Використовує ClickHouse функції для швидких розрахунків
"""

import logging
import sys
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from config import CH_CONFIG, BINS_CONFIG, BIVARIATE_COMBINATIONS
from utils import ClickHouseConnector, ETLProgress

# Налаштування логування
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AdminBinsCalculator:
    """
    Клас для розрахунку bins (квантилів) та bivariate комбінацій
    Працює безпосередньо в ClickHouse для максимальної швидкості
    """
    
    def __init__(self):
        """Ініціалізація калькулятора bins"""
        self.ch_conn = ClickHouseConnector(CH_CONFIG)
        self.start_time = datetime.now()
        
        # Метрики для розрахунку bins (з config.py)
        self.metrics_for_bins = BINS_CONFIG.get('metrics_for_bins', [
            'population_density',
            'economic_activity_index', 
            'competitor_density',
            'transport_accessibility_score'
        ])
        
        # Bivariate комбінації (з config.py)
        self.bivariate_pairs = BIVARIATE_COMBINATIONS or [
            ('population_density', 'economic_activity_index'),
            ('competitor_density', 'transport_accessibility_score'),
            ('poi_density', 'retail_potential_score'),
            ('residential_coverage', 'commercial_activity_score')
        ]
        
    def get_admin_levels(self) -> List[int]:
        """
        Отримує список унікальних admin_level з БД
        
        Returns:
            Список рівнів адмінодиниць
        """
        with self.ch_conn.connect():
            result = self.ch_conn.client.execute("""
                SELECT DISTINCT admin_level 
                FROM geo_analytics.admin_analytics
                ORDER BY admin_level
            """)
            levels = [row[0] for row in result]
            logger.info(f"📊 Знайдено {len(levels)} рівнів адмінодиниць: {levels}")
            return levels
    
    def calculate_bins_for_metric(
        self, 
        metric: str, 
        admin_level: int, 
        num_bins: int = 3
    ) -> Dict[str, int]:
        """
        Розраховує bins для однієї метрики на певному admin_level
        Використовує ClickHouse функцію ntile для розподілу
        
        Args:
            metric: Назва метрики
            admin_level: Рівень адмінодиниці
            num_bins: Кількість bins (за замовчуванням 3 для terciles)
            
        Returns:
            Словник {admin_id: bin_value}
        """
        logger.info(f"  📈 Розрахунок {num_bins} bins для {metric} (level={admin_level})")
        
        with self.ch_conn.connect():
            # Використовуємо ntile для автоматичного розподілу на bins
            # Тільки NULL значення отримують bin=0, нулі - це валідні дані
            query = f"""
                SELECT 
                    admin_id,
                    CASE 
                        WHEN {metric} IS NULL THEN 0
                        ELSE ntile({num_bins}) OVER (
                            PARTITION BY admin_level 
                            ORDER BY {metric}
                        )
                    END as bin_value
                FROM geo_analytics.admin_analytics
                WHERE admin_level = {admin_level}
            """
            
            result = self.ch_conn.client.execute(query)
            bins_dict = {row[0]: row[1] for row in result}
            
            # Статистика розподілу
            distribution = {}
            for bin_val in bins_dict.values():
                distribution[bin_val] = distribution.get(bin_val, 0) + 1
            
            logger.info(f"    Розподіл: {distribution}")
            return bins_dict
    
    def calculate_all_bins(self) -> Dict[Tuple[int, int], Dict[str, int]]:
        """
        Розраховує bins для всіх метрик та рівнів
        
        Returns:
            Словник {(admin_level, admin_id): {metric_bin: value, ...}}
        """
        logger.info("🔢 Розрахунок bins для всіх метрик...")
        all_bins = {}
        
        admin_levels = self.get_admin_levels()
        
        for level in admin_levels:
            logger.info(f"\n🏛️ Обробка рівня {level}:")
            
            # Отримуємо всі admin_id для цього рівня
            with self.ch_conn.connect():
                result = self.ch_conn.client.execute(f"""
                    SELECT admin_id 
                    FROM geo_analytics.admin_analytics
                    WHERE admin_level = {level}
                """)
                admin_ids = [row[0] for row in result]
            
            # Ініціалізуємо структуру для збереження bins
            for admin_id in admin_ids:
                all_bins[(level, admin_id)] = {}
            
            # Розраховуємо bins для кожної метрики
            for metric in self.metrics_for_bins:
                # Перевіряємо чи існує метрика в таблиці
                if not self._check_metric_exists(metric):
                    logger.warning(f"  ⚠️ Метрика {metric} не знайдена в таблиці")
                    continue
                
                bins = self.calculate_bins_for_metric(metric, level, num_bins=3)
                
                # Зберігаємо результати
                for admin_id, bin_value in bins.items():
                    if (level, admin_id) in all_bins:
                        # Формуємо назву поля для bin
                        bin_field = self._get_bin_field_name(metric)
                        all_bins[(level, admin_id)][bin_field] = bin_value
        
        return all_bins
    
    def _get_bin_field_name(self, metric: str) -> str:
        """
        Генерує назву поля для bin на основі метрики
        
        Args:
            metric: Назва метрики
            
        Returns:
            Назва поля для bin
        """
        # Мапінг метрик на назви bin полів
        mapping = {
            'population_density': 'population_bin',
            'economic_activity_index': 'economic_bin',
            'competitor_density': 'competitor_bin',
            'transport_accessibility_score': 'accessibility_bin',
            'poi_density': 'infrastructure_bin',
            'retail_potential_score': 'retail_bin',
            'residential_coverage': 'residential_bin',
            'commercial_activity_score': 'commercial_bin'
        }
        return mapping.get(metric, f"{metric.replace('_', '')}_bin")
    
    def _check_metric_exists(self, metric: str) -> bool:
        """
        Перевіряє чи існує метрика в таблиці
        
        Args:
            metric: Назва метрики
            
        Returns:
            True якщо метрика існує
        """
        with self.ch_conn.connect():
            columns = self.ch_conn.get_table_columns('geo_analytics.admin_analytics')
            return metric in columns
    
    def calculate_bivariate_combinations(
        self, 
        bins_data: Dict[Tuple[int, int], Dict[str, int]]
    ) -> Dict[Tuple[int, int], Dict[str, str]]:
        """
        Розраховує bivariate комбінації на основі bins
        
        Args:
            bins_data: Словник з bins для кожної адмінодиниці
            
        Returns:
            Словник з bivariate комбінаціями
        """
        logger.info("\n🎨 Розрахунок bivariate комбінацій...")
        bivariate_data = {}
        
        for (level, admin_id), bins in bins_data.items():
            bivariate_data[(level, admin_id)] = {}
            
            for metric1, metric2 in self.bivariate_pairs:
                # Отримуємо назви bin полів
                bin1_field = self._get_bin_field_name(metric1)
                bin2_field = self._get_bin_field_name(metric2)
                
                # Отримуємо значення bins (0 якщо відсутнє)
                bin1_value = bins.get(bin1_field, 0)
                bin2_value = bins.get(bin2_field, 0)
                
                # Формуємо код комбінації (формат: "1-2")
                bivar_code = f"{bin1_value}-{bin2_value}"
                
                # Формуємо назву bivariate поля
                bivar_field = self._get_bivariate_field_name(metric1, metric2)
                bivariate_data[(level, admin_id)][bivar_field] = bivar_code
        
        return bivariate_data
    
    def _get_bivariate_field_name(self, metric1: str, metric2: str) -> str:
        """
        Генерує назву поля для bivariate комбінації
        
        Args:
            metric1: Перша метрика
            metric2: Друга метрика
            
        Returns:
            Назва bivariate поля
        """
        # Мапінг пар метрик на назви bivariate полів
        mapping = {
            ('population_density', 'economic_activity_index'): 'bivar_pop_economic',
            ('competitor_density', 'transport_accessibility_score'): 'bivar_comp_infrastructure', 
            ('poi_density', 'retail_potential_score'): 'bivar_access_economic',
            ('residential_coverage', 'commercial_activity_score'): 'bivar_urban_competitor'
        }
        
        # Пробуємо знайти в мапінгу
        if (metric1, metric2) in mapping:
            return mapping[(metric1, metric2)]
        elif (metric2, metric1) in mapping:
            return mapping[(metric2, metric1)]
        else:
            # Генеруємо назву автоматично
            m1_short = metric1.split('_')[0][:3]
            m2_short = metric2.split('_')[0][:3]
            return f"bivar_{m1_short}_{m2_short}"
    
    def update_clickhouse_records(
        self,
        bins_data: Dict[Tuple[int, int], Dict[str, int]],
        bivariate_data: Dict[Tuple[int, int], Dict[str, str]]
    ) -> int:
        """
        Оновлює записи в ClickHouse з розрахованими bins та bivariate
        
        Args:
            bins_data: Дані bins
            bivariate_data: Дані bivariate комбінацій
            
        Returns:
            Кількість оновлених записів
        """
        logger.info("\n📤 Оновлення записів в ClickHouse...")
        
        with self.ch_conn.connect():
            total_updated = 0
            progress = ETLProgress(len(bins_data), "Оновлення bins")
            
            # Групуємо оновлення по admin_level для ефективності
            updates_by_level = {}
            
            for (level, admin_id), bins in bins_data.items():
                if level not in updates_by_level:
                    updates_by_level[level] = []
                
                # Об'єднуємо bins та bivariate дані
                update_data = {**bins}
                if (level, admin_id) in bivariate_data:
                    update_data.update(bivariate_data[(level, admin_id)])
                
                update_data['admin_id'] = admin_id
                updates_by_level[level].append(update_data)
            
            # Виконуємо оновлення для кожного рівня
            for level, updates in updates_by_level.items():
                logger.info(f"  Оновлення рівня {level}: {len(updates)} записів")
                
                if not updates:
                    continue
                
                # Отримуємо список полів для оновлення
                sample_update = updates[0]
                fields_to_update = [k for k in sample_update.keys() if k != 'admin_id']
                
                # Формуємо SQL для batch update
                for batch_start in range(0, len(updates), 100):
                    batch = updates[batch_start:batch_start + 100]
                    
                    # Використовуємо ALTER TABLE UPDATE для масового оновлення
                    for field in fields_to_update:
                        if field.endswith('_bin') or field.startswith('bivar_'):
                            values_clause = []
                            for record in batch:
                                admin_id = record['admin_id']
                                value = record.get(field, 0) if field.endswith('_bin') else record.get(field, '')
                                
                                if isinstance(value, str):
                                    values_clause.append(f"admin_id = {admin_id} THEN '{value}'")
                                else:
                                    values_clause.append(f"admin_id = {admin_id} THEN {value}")
                            
                            if values_clause:
                                update_sql = f"""
                                    ALTER TABLE geo_analytics.admin_analytics
                                    UPDATE {field} = CASE
                                        WHEN {' WHEN '.join(values_clause)}
                                        ELSE {field}
                                    END
                                    WHERE admin_level = {level} 
                                        AND admin_id IN ({','.join(str(r['admin_id']) for r in batch)})
                                """
                                
                                try:
                                    self.ch_conn.client.execute(update_sql)
                                except Exception as e:
                                    logger.warning(f"    ⚠️ Помилка оновлення {field}: {str(e)[:100]}")
                    
                    total_updated += len(batch)
                    progress.update(len(batch))
            
            # Примітка: updated_at не можна оновлювати, бо це ключова колонка
            # В ClickHouse ключові колонки (ORDER BY) не можна змінювати через UPDATE
            
            logger.info(f"✅ Оновлено {total_updated} записів")
            
            # Перевіряємо що bins дійсно оновлені
            check_result = self.ch_conn.client.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN population_bin > 0 THEN 1 ELSE 0 END) as with_bins
                FROM geo_analytics.admin_analytics
            """)
            
            if check_result:
                total, with_bins = check_result[0]
                logger.info(f"📊 Перевірка: {with_bins}/{total} записів мають bins")
            
            return total_updated
    
    def generate_statistics(self):
        """
        Генерує статистику розподілу bins та bivariate комбінацій
        """
        logger.info("\n📊 Статистика bins та bivariate розподілу:")
        
        with self.ch_conn.connect():
            # Статистика по bins для кожного рівня
            for level in self.get_admin_levels():
                logger.info(f"\n🏛️ Рівень {level}:")
                
                # Статистика по кожному bin
                for metric in ['population_bin', 'economic_bin', 'competitor_bin', 'accessibility_bin']:
                    # Визначаємо правильне поле для середнього значення
                    if metric == 'population_bin':
                        avg_field = 'population_density'
                    elif metric == 'economic_bin':
                        avg_field = 'economic_activity_index'
                    elif metric == 'competitor_bin':
                        avg_field = 'competitor_density'
                    elif metric == 'accessibility_bin':
                        avg_field = 'transport_accessibility_score'
                    else:
                        avg_field = 'NULL'
                    
                    result = self.ch_conn.client.execute(f"""
                        SELECT 
                            {metric} as bin_value,
                            COUNT(*) as count,
                            AVG({avg_field}) as avg_value
                        FROM geo_analytics.admin_analytics
                        WHERE admin_level = {level}
                        GROUP BY {metric}
                        ORDER BY {metric}
                    """)
                    
                    if result:
                        logger.info(f"  {metric}:")
                        for row in result:
                            bin_val, count, avg_val = row
                            label = ['Відсутні дані', 'Низький', 'Середній', 'Високий'][bin_val] if bin_val <= 3 else f"Bin {bin_val}"
                            if avg_val:
                                logger.info(f"    {label} (bin={bin_val}): {count} од., середнє={avg_val:.2f}")
                            else:
                                logger.info(f"    {label} (bin={bin_val}): {count} од.")
                
                # Статистика по bivariate комбінаціям
                logger.info(f"  Bivariate комбінації:")
                result = self.ch_conn.client.execute(f"""
                    SELECT 
                        bivar_pop_economic,
                        COUNT(*) as count
                    FROM geo_analytics.admin_analytics
                    WHERE admin_level = {level}
                        AND bivar_pop_economic != ''
                    GROUP BY bivar_pop_economic
                    ORDER BY count DESC
                    LIMIT 10
                """)
                
                if result:
                    for row in result:
                        bivar_code, count = row
                        logger.info(f"    {bivar_code}: {count} од.")
    
    def validate_results(self) -> bool:
        """
        Валідує результати розрахунку bins
        
        Returns:
            True якщо валідація пройшла успішно
        """
        logger.info("\n🔍 Валідація результатів...")
        
        with self.ch_conn.connect():
            # Перевірка наявності bins
            result = self.ch_conn.client.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN population_bin > 0 THEN 1 ELSE 0 END) as with_pop_bin,
                    SUM(CASE WHEN economic_bin > 0 THEN 1 ELSE 0 END) as with_econ_bin,
                    SUM(CASE WHEN bivar_pop_economic != '' THEN 1 ELSE 0 END) as with_bivar
                FROM geo_analytics.admin_analytics
            """)
            
            if result:
                total, with_pop, with_econ, with_bivar = result[0]
                logger.info(f"  Всього записів: {total}")
                logger.info(f"  З population_bin: {with_pop} ({with_pop*100/total:.1f}%)")
                logger.info(f"  З economic_bin: {with_econ} ({with_econ*100/total:.1f}%)")
                logger.info(f"  З bivariate: {with_bivar} ({with_bivar*100/total:.1f}%)")
                
                # Перевірка розподілу по bins (має бути приблизно рівномірний)
                for bin_field in ['population_bin', 'economic_bin']:
                    result = self.ch_conn.client.execute(f"""
                        SELECT 
                            {bin_field},
                            COUNT(*) as cnt,
                            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
                        FROM geo_analytics.admin_analytics
                        WHERE {bin_field} > 0
                        GROUP BY {bin_field}
                        ORDER BY {bin_field}
                    """)
                    
                    logger.info(f"\n  Розподіл {bin_field}:")
                    for row in result:
                        bin_val, count, percentage = row
                        logger.info(f"    Bin {bin_val}: {count} ({percentage:.1f}%)")
                        
                        # Для terciles очікуємо ~33% в кожному bin
                        if 20 < percentage < 45:
                            logger.info(f"      ✅ Розподіл в нормі")
                        else:
                            logger.warning(f"      ⚠️ Нерівномірний розподіл")
                
                return with_pop > 0 and with_econ > 0
            
            return False
    
    def run(self) -> bool:
        """
        Запускає повний процес розрахунку bins
        
        Returns:
            True якщо процес завершився успішно
        """
        try:
            logger.info("=" * 60)
            logger.info("🚀 ПОЧАТОК: Розрахунок bins та bivariate комбінацій")
            logger.info("=" * 60)
            
            # 1. Розрахунок bins для всіх метрик
            bins_data = self.calculate_all_bins()
            
            if not bins_data:
                logger.error("❌ Не вдалося розрахувати bins")
                return False
            
            logger.info(f"✅ Розраховано bins для {len(bins_data)} адмінодиниць")
            
            # 2. Розрахунок bivariate комбінацій
            bivariate_data = self.calculate_bivariate_combinations(bins_data)
            logger.info(f"✅ Розраховано bivariate для {len(bivariate_data)} адмінодиниць")
            
            # 3. Оновлення записів в ClickHouse
            updated = self.update_clickhouse_records(bins_data, bivariate_data)
            
            # 4. Генерація статистики
            self.generate_statistics()
            
            # 5. Валідація результатів
            success = self.validate_results()
            
            # Підсумок
            elapsed = datetime.now() - self.start_time
            logger.info("\n" + "=" * 60)
            if success:
                logger.info(f"✅ BINS РОЗРАХОВАНО УСПІШНО")
                logger.info(f"⏱️ Час виконання: {elapsed}")
                logger.info(f"📊 Оновлено записів: {updated}")
                logger.info(f"\n🎯 Bins готові для візуалізації на картах!")
                logger.info("   Формат bivariate: '0-0' до '3-3' (4×4 матриця)")
                logger.info("   0 = відсутні дані, 1 = низький, 2 = середній, 3 = високий")
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
    calculator = AdminBinsCalculator()
    success = calculator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()