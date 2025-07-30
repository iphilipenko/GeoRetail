#!/usr/bin/env python3
"""
Прямий тест SQL вставки з детальною діагностикою
"""

from module1_raw_import import ImportConfig, RawDataImporter
from sqlalchemy import create_engine, text
import geopandas as gpd
import json

def test_sql_direct():
    """Прямий тест SQL з детальною діагностикою"""
    
    print("🔍 Пряма діагностика SQL вставки")
    
    try:
        config = ImportConfig()
        engine = create_engine(config.connection_string)
        
        # Читаємо тестові дані
        gdf = gpd.read_file(r"C:\OSMData\UA_MAP_Kherson.gpkg")
        test_record = gdf.iloc[0]
        
        print(f"📝 Тестовий запис: OSM_ID {test_record['osm_id']}")
        
        # Створюємо імпортер для підготовки запису
        importer = RawDataImporter(config)
        prepared = importer._prepare_record(test_record.to_dict(), 'Kherson')  # Змінено на 'Kherson'
        
        if not prepared:
            print("❌ Запис не підготовлений")
            return
        
        print("✅ Запис підготовлений")
        print("📊 Структура підготовленого запису:")
        for key, value in prepared.items():
            if key == 'geom':
                print(f"  {key}: {type(value)} - {value.geom_type if hasattr(value, 'geom_type') else 'Unknown'}")
            else:
                value_str = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                print(f"  {key}: {value_str}")
        
        # Конвертуємо геометрію в WKT (як в оригінальному коді)
        if prepared.get('geom'):
            try:
                if hasattr(prepared['geom'], 'wkt'):
                    prepared['geom_wkt'] = prepared['geom'].wkt
                    print(f"✅ WKT конверсія: {len(prepared['geom_wkt'])} символів")
                else:
                    prepared['geom_wkt'] = str(prepared['geom'])
                    print(f"✅ WKT fallback: {len(prepared['geom_wkt'])} символів")
            except Exception as e:
                print(f"❌ Помилка WKT конверсії: {e}")
                return
            
            # Видаляємо оригінальну геометрію
            del prepared['geom']
        
        # ТОЧНИЙ SQL З РОБОЧОГО КОДУ
        sql = """
        INSERT INTO osm_ukraine.osm_raw 
        (region_name, original_fid, osm_id, geom, tags, name, 
         h3_res_7, h3_res_8, h3_res_9, h3_res_10, data_quality_score)
        VALUES (:region_name, :original_fid, :osm_id, 
                ST_GeomFromText(:geom_wkt, 4326), :tags, :name,
                :h3_res_7, :h3_res_8, :h3_res_9, :h3_res_10, 
                :data_quality_score)
        """
        
        print("\n🔧 Тест SQL вставки:")
        print("SQL:", sql.replace('\n', '\\n'))
        
        # Показуємо параметри
        print("\n📋 Параметри для вставки:")
        for key, value in prepared.items():
            print(f"  :{key} = {value}")
        
        # Спробуємо вставку
        try:
            with engine.connect() as conn:
                with conn.begin():
                    result = conn.execute(text(sql), prepared)
                    print(f"✅ SQL виконано успішно! Rows affected: {result.rowcount}")
                    
                    # Перевіримо що вставилося
                    check_sql = """
                    SELECT osm_id, name, h3_res_8, ST_AsText(geom) as geom_text
                    FROM osm_ukraine.osm_raw 
                    WHERE region_name = :region_name AND osm_id = :osm_id
                    LIMIT 1
                    """
                    
                    check_result = conn.execute(text(check_sql), {
                        'region_name': prepared['region_name'],
                        'osm_id': prepared['osm_id']
                    })
                    
                    row = check_result.fetchone()
                    if row:
                        print(f"✅ Запис знайдено в БД:")
                        print(f"   OSM_ID: {row[0]}")
                        print(f"   Name: {row[1]}")
                        print(f"   H3: {row[2]}")
                        print(f"   Geom: {row[3][:100]}...")
                    else:
                        print("❌ Запис не знайдено в БД після вставки")
                        
        except Exception as e:
            print(f"❌ Помилка SQL виконання: {e}")
            print(f"📊 Тип помилки: {type(e)}")
            
            # Спробуємо діагностувати конкретну проблему
            import traceback
            print("\n📋 Детальна діагностика:")
            traceback.print_exc()
            
            # Можливі причини:
            print("\n🔍 Можливі причини:")
            print("1. Партиціонування таблиці - можливо потрібно створити партицію для 'Kherson_Test'")
            print("2. Constraint violations")
            print("3. Некоректні дані")
            print("4. Проблеми з типами даних")
        
    except Exception as e:
        print(f"💥 Критична помилка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_sql_direct()