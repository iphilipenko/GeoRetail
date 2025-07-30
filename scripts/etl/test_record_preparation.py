#!/usr/bin/env python3
"""
Швидкий тест підготовки записів
"""

from module1_raw_import import ImportConfig, RawDataImporter
import geopandas as gpd
from pathlib import Path

def test_record_preparation():
    """Тест підготовки записів"""
    
    print("🧪 Тест підготовки записів")
    
    try:
        # Створення імпортера
        config = ImportConfig(batch_size=100)
        importer = RawDataImporter(config)
        print("✅ Імпортер створено")
        
        # Читання файлу
        file_path = Path(r"C:\OSMData\UA_MAP_Kherson.gpkg")
        print(f"📖 Читання файлу: {file_path}")
        
        gdf = gpd.read_file(file_path)
        print(f"✅ Прочитано {len(gdf):,} записів")
        
        # Тест підготовки перших 5 записів
        print("\n🔍 Тест підготовки записів:")
        successful = 0
        
        for i in range(5):
            print(f"\n--- Запис {i+1} ---")
            record = gdf.iloc[i].to_dict()
            
            # Показуємо вхідні дані
            print(f"📝 OSM ID: {record.get('osm_id')}")
            print(f"🏷️ Теги: {record.get('tags')[:100]}..." if record.get('tags') else "🏷️ Теги: немає")
            print(f"📍 Геометрія: {type(record.get('geometry'))}")
            
            # Тест підготовки
            try:
                prepared = importer._prepare_record(record, 'Kherson_Test')
                if prepared:
                    print(f"✅ Запис підготовлений:")
                    print(f"   OSM ID: {prepared['osm_id']}")
                    print(f"   Назва: {prepared['name'] or 'немає'}")
                    print(f"   H3-8: {prepared['h3_res_8'] or 'немає'}")
                    print(f"   Якість: {prepared['data_quality_score']:.2f}")
                    successful += 1
                else:
                    print(f"❌ Запис відкинутий")
            except Exception as e:
                print(f"💥 Помилка підготовки: {e}")
        
        print(f"\n📊 Результат: {successful}/5 записів підготовлено")
        
        if successful > 0:
            print("\n🎉 Підготовка записів працює! Можна запускати повний імпорт.")
            
            # Тест малого батчу
            print("\n🧪 Тест вставки малого батчу (10 записів):")
            
            small_batch = []
            for i in range(10):
                record = gdf.iloc[i].to_dict()
                prepared = importer._prepare_record(record, 'Kherson_Test')
                if prepared:
                    small_batch.append(prepared)
            
            if small_batch:
                print(f"📦 Підготовлено {len(small_batch)} записів для вставки")
                
                try:
                    inserted = importer._insert_batch(small_batch)
                    print(f"✅ Вставлено {inserted} записів в БД")
                    
                    if inserted > 0:
                        print("🎉 ВСЕ ПРАЦЮЄ! Модуль готовий до використання!")
                    else:
                        print("⚠️ Записи підготовлені, але не вставлені. Можлива проблема з БД.")
                        
                except Exception as e:
                    print(f"❌ Помилка вставки в БД: {e}")
            else:
                print("❌ Жоден запис не підготовлений для вставки")
        else:
            print("❌ Підготовка записів не працює. Потрібне додаткове налагодження.")
            
    except Exception as e:
        print(f"💥 Критична помилка тесту: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_record_preparation()