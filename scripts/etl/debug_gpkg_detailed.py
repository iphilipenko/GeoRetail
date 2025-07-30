#!/usr/bin/env python3
"""
Детальна діагностика GPKG файлу
"""

import geopandas as gpd
from pathlib import Path
import sys

def analyze_gpkg_detailed(file_path):
    """Детальний аналіз GPKG файлу"""
    print(f"🔍 Аналіз файлу: {file_path}")
    
    try:
        # Читаємо файл
        print("📖 Читання файлу...")
        gdf = gpd.read_file(file_path)
        
        print(f"✅ Успішно прочитано: {len(gdf):,} записів")
        print(f"📊 Колонки ({len(gdf.columns)}): {list(gdf.columns)}")
        
        # Аналіз першого запису
        print("\n🔎 Перший запис:")
        first_record = gdf.iloc[0]
        
        for col in gdf.columns:
            value = first_record[col]
            if col == 'geometry':
                print(f"  📍 {col}: {type(value)} - {value.geom_type if hasattr(value, 'geom_type') else 'Unknown'}")
                if hasattr(value, 'wkt'):
                    wkt_preview = value.wkt[:100] + "..." if len(value.wkt) > 100 else value.wkt
                    print(f"      WKT: {wkt_preview}")
            else:
                value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                print(f"  📝 {col}: {value_str}")
        
        # Перевірка геометрії
        print(f"\n🌍 Аналіз геометрії:")
        geom_col = 'geometry' if 'geometry' in gdf.columns else 'geom' if 'geom' in gdf.columns else None
        
        if geom_col:
            print(f"  ✅ Колонка геометрії: {geom_col}")
            print(f"  📐 Типи геометрії: {gdf[geom_col].geom_type.value_counts().to_dict()}")
            
            # Тест конвертації WKT
            test_geom = gdf.iloc[0][geom_col]
            try:
                wkt = test_geom.wkt
                print(f"  ✅ WKT конвертація працює: {len(wkt)} символів")
            except Exception as e:
                print(f"  ❌ Помилка WKT конвертації: {e}")
        else:
            print("  ❌ Колонка геометрії не знайдена!")
        
        # Аналіз OSM ID
        print(f"\n🆔 OSM ID аналіз:")
        if 'osm_id' in gdf.columns:
            osm_ids = gdf['osm_id'].dropna()
            print(f"  ✅ OSM ID знайдено: {len(osm_ids)} записів")
            print(f"  📊 Приклади: {osm_ids.head().tolist()}")
        else:
            print("  ❌ Колонка osm_id не знайдена!")
        
        # Аналіз назв
        print(f"\n📛 Аналіз назв:")
        name_fields = ['name', 'name:en', 'name:uk']
        for field in name_fields:
            if field in gdf.columns:
                names = gdf[field].dropna()
                print(f"  ✅ {field}: {len(names)} записів")
                if len(names) > 0:
                    print(f"      Приклади: {names.head(3).tolist()}")
            else:
                print(f"  ❌ {field}: не знайдено")
        
        # Тест нашої логіки підготовки запису
        print(f"\n🧪 Тест підготовки запису:")
        test_record = first_record.to_dict()
        
        # Імітуємо нашу логіку
        has_geom = test_record.get('geom') or test_record.get('geometry')
        print(f"  🌍 Геометрія присутня: {bool(has_geom)}")
        
        if has_geom:
            geom = test_record.get('geom') or test_record.get('geometry')
            try:
                if hasattr(geom, 'wkt'):
                    geom_wkt = geom.wkt
                    print(f"  ✅ WKT отримано: {len(geom_wkt)} символів")
                    
                    # Тест H3
                    if hasattr(geom, 'centroid'):
                        centroid = geom.centroid
                        lat, lon = centroid.y, centroid.x
                        print(f"  📍 Координати центру: {lat:.6f}, {lon:.6f}")
                    else:
                        print(f"  📍 Координати: {geom.y:.6f}, {geom.x:.6f}")
                        
                    print(f"  ✅ Запис може бути підготовлений!")
                    
                else:
                    print(f"  ❌ Геометрія не має атрибут .wkt")
            except Exception as e:
                print(f"  ❌ Помилка обробки геометрії: {e}")
        else:
            print(f"  ❌ Запис буде відкинутий - немає геометрії")
            
        # Статистика по колонках
        print(f"\n📈 Статистика заповненості:")
        for col in gdf.columns:
            if col != 'geometry':
                non_null = gdf[col].count()
                percentage = (non_null / len(gdf)) * 100
                print(f"  📊 {col}: {non_null:,} ({percentage:.1f}%)")
                
    except Exception as e:
        print(f"❌ Помилка аналізу: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Аналіз файлу Херсон
    file_path = Path(r"C:\OSMData\UA_MAP_Kherson.gpkg")
    if not file_path.exists():
        print(f"❌ Файл не знайдено: {file_path}")
        sys.exit(1)
    
    analyze_gpkg_detailed(file_path)