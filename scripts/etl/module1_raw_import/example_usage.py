#!/usr/bin/env python3
"""
Приклад використання Модуля 1: Raw Data Import
"""

from module1_raw_import import ImportConfig, RawDataImporter

def main():
    """Приклад використання модуля"""
    
    # Варіант 1: Використання з дефолтною конфігурацією
    config = ImportConfig()
    importer = RawDataImporter(config)
    
    # Запуск імпорту всіх файлів
    result = importer.run_import()
    print(f"Імпортовано {result['total_records_imported']} записів")
    
    # Варіант 2: Використання з налаштованою конфігурацією
    config = ImportConfig(
        data_directory=r"D:\OSMData",
        batch_size=10000,
        log_level="DEBUG"
    )
    importer = RawDataImporter(config)
    
    # Тестовий запуск
    result = importer.run_import(test_run=True)
    
    # Варіант 3: Імпорт конкретних регіонів
    result = importer.run_import(
        regions=['kyiv', 'lviv'],
        test_run=False
    )
    
    # Варіант 4: Використання environment variables
    config = ImportConfig.from_env()
    importer = RawDataImporter(config)
    result = importer.run_import()

if __name__ == "__main__":
    main()