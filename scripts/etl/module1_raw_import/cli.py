#!/usr/bin/env python3
"""
Модуль 1: Raw Data Import - CLI інтерфейс
Простий командний інтерфейс для імпорту OSM даних
"""

import sys
from pathlib import Path

import click

from .config import ImportConfig
from .importer import RawDataImporter


@click.group()
def cli():
    """Модуль 1: Raw Data Import - Імпорт OSM GPKG файлів в PostGIS"""
    pass


@cli.command()
@click.option('--data-dir', 
              default=None,
              help='Директорія з GPKG файлами (за замовчуванням з конфігурації)')
@click.option('--batch-size', 
              default=5000, 
              help='Розмір батчу для обробки')
@click.option('--regions', 
              help='Список регіонів через кому (якщо не вказано - всі)')
@click.option('--test-run', 
              is_flag=True, 
              help='Тестовий запуск на 1 найменшому файлі')
@click.option('--log-level',
              default='INFO',
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR']),
              help='Рівень логування')
def import_data(data_dir, batch_size, regions, test_run, log_level):
    """Імпорт OSM даних в PostGIS"""
    
    try:
        # Створення конфігурації
        config = ImportConfig.from_env()
        config.batch_size = batch_size
        config.log_level = log_level
        
        if data_dir:
            config.data_directory = data_dir
        
        # Створення імпортера
        importer = RawDataImporter(config)
        
        # Підготовка параметрів
        regions_list = None
        if regions:
            regions_list = [r.strip() for r in regions.split(',')]
        
        # Запуск імпорту
        result = importer.run_import(
            data_dir=data_dir,
            regions=regions_list,
            test_run=test_run
        )
        
        # Виведення результатів
        if result['successful_files'] > 0:
            click.echo(f"\n✅ Успішно завершено!")
            click.echo(f"   Файлів: {result['successful_files']}/{result['total_files']}")
            click.echo(f"   Записів: {result['total_records_imported']:,}")
            click.echo(f"   Час: {result['total_processing_time']}с")
            
            if result['failed_files'] > 0:
                click.echo(f"\n⚠️ Файлів з помилками: {result['failed_files']}")
        else:
            click.echo(f"\n❌ Імпорт не вдався")
            if result['failed_files'] > 0:
                click.echo(f"   Файлів з помилками: {result['failed_files']}")
        
        # Exit код
        sys.exit(0 if result['failed_files'] == 0 else 1)
        
    except KeyboardInterrupt:
        click.echo("\n🛑 Імпорт перервано користувачем")
        sys.exit(2)
    except Exception as e:
        click.echo(f"\n💥 Критична помилка: {e}")
        sys.exit(3)


@cli.command()
@click.option('--data-dir', 
              default=None,
              help='Директорія з GPKG файлами')
def list_files(data_dir):
    """Показати список доступних GPKG файлів"""
    
    try:
        config = ImportConfig.from_env()
        if data_dir:
            config.data_directory = data_dir
            
        importer = RawDataImporter(config)
        files = importer.discover_files(data_dir)
        
        if not files:
            click.echo("GPKG файли не знайдені")
            return
        
        click.echo(f"\nЗнайдено {len(files)} GPKG файлів:")
        click.echo("-" * 60)
        
        total_size = 0
        for file_path in files:
            size_mb = file_path.stat().st_size / (1024 * 1024)
            total_size += size_mb
            
            region = file_path.stem
            click.echo(f"{region:<30} {size_mb:>8.1f} MB")
        
        click.echo("-" * 60)
        click.echo(f"{'Загальний розмір:':<30} {total_size:>8.1f} MB")
        
    except Exception as e:
        click.echo(f"Помилка: {e}")
        sys.exit(1)


@cli.command()
def test_connection():
    """Тестування підключення до бази даних"""
    
    try:
        config = ImportConfig.from_env()
        
        click.echo("Тестування підключення до БД...")
        click.echo(f"Host: {config.db_host}:{config.db_port}")
        click.echo(f"Database: {config.db_name}")
        click.echo(f"User: {config.db_user}")
        
        importer = RawDataImporter(config)
        click.echo("✅ Підключення успішне!")
        
    except Exception as e:
        click.echo(f"❌ Помилка підключення: {e}")
        sys.exit(1)


if __name__ == '__main__':
    cli()