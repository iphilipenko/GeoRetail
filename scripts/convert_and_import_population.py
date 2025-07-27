# scripts/convert_and_import_population.py
import geopandas as gpd
import h3
from sqlalchemy import create_engine
from tqdm import tqdm
import pandas as pd

def import_population_data():
    print("🇺🇦 Importing Ukraine population data...")
    
    # Підключення до бази
    engine = create_engine('postgresql://georetail_user:georetail_password@localhost:5432/georetail')
    
    # Читання GPKG
    gpkg_path = r"C:\projects\AA AI Assistance\GeoRetail_git\kontur_population_UA_20231101.gpkg"
    
    print("📖 Reading GPKG file...")
    gdf = gpd.read_file(gpkg_path, layer='population')
    
    print(f"📊 Processing {len(gdf)} population records...")
    
    # Тест конвертації H3
    print("🔧 Testing H3 conversion...")
    sample_h3 = gdf.iloc[0]['h3']
    print(f"Original: {sample_h3}")
    
    # Спробувати різні способи
    try:
        # Метод 1: Як hex integer
        h3_int = int(sample_h3, 16)
        print(f"As integer: {h3_int}")
        
        # Спробувати отримати resolution прямо з integer
        resolution = h3.get_resolution(h3_int)  # Нова версія h3
        print(f"Resolution: {resolution}")
        
        # Перевірити чи це валідний H3
        center_lat, center_lon = h3.cell_to_latlng(h3_int)  # Нова версія h3
        print(f"Center coordinates: {center_lat:.6f}, {center_lon:.6f}")
        
        # Розмір гексагона
        edge_length = h3.get_hexagon_edge_length_avg(resolution, 'm')
        print(f"Edge length: {edge_length:.0f} meters")
        
        print("✅ H3 conversion successful!")
        
    except Exception as e:
        print(f"❌ H3 conversion failed: {e}")
        print("Trying alternative method...")
        
        # Альтернативний метод - використати h3 як string
        try:
            # Можливо це вже правильний формат, просто треба додати 0x
            h3_formatted = sample_h3
            resolution = h3.get_resolution(h3_formatted)
            center_lat, center_lon = h3.cell_to_latlng(h3_formatted)
            print(f"✅ Alternative method worked! Resolution: {resolution}")
        except Exception as e2:
            print(f"❌ Alternative method also failed: {e2}")
            return
    
    # Підготовка даних для масового імпорту
    print("🔄 Converting all H3 indices...")
    
    processed_data = []
    errors = 0
    
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf), desc="Converting"):
        try:
            h3_string = row['h3']
            population = row['population']
            
            # Конвертувати H3 - спробувати обидва методи
            try:
                # Метод 1: як integer
                h3_int = int(h3_string, 16)
                h3_id = h3_int
                center_lat, center_lon = h3.cell_to_latlng(h3_int)
                resolution = h3.get_resolution(h3_int)
            except:
                # Метод 2: як string
                h3_id = h3_string
                center_lat, center_lon = h3.cell_to_latlng(h3_string)
                resolution = h3.get_resolution(h3_string)
            
            # Отримати характеристики
            area_km2 = h3.get_hexagon_area_avg(resolution, 'km^2')
            
            # Розрахувати щільність
            population_density = population / area_km2 if area_km2 > 0 else 0
            
            processed_data.append({
                'hex_id': str(h3_id),  # Зберігаємо як string
                'resolution': resolution,
                'population': float(population),
                'population_density': float(population_density),
                'center_lat': center_lat,
                'center_lon': center_lon,
                'area_km2': area_km2,
                'original_h3': h3_string,
                'geometry': row['geometry']
            })
            
        except Exception as e:
            errors += 1
            if errors < 10:  # Показати перші 10 помилок
                print(f"Error processing row {idx}: {e}")
    
    print(f"✅ Processed {len(processed_data)} records, {errors} errors")
    
    if len(processed_data) == 0:
        print("❌ No valid data to import")
        return
    
    # Створити GeoDataFrame
    result_gdf = gpd.GeoDataFrame(processed_data)
    
    print("💾 Writing to PostGIS...")
    
    # Спочатку створити таблицю
    create_table_sql = """
    CREATE SCHEMA IF NOT EXISTS demographics;
    
    DROP TABLE IF EXISTS demographics.h3_population;
    
    CREATE TABLE demographics.h3_population (
        hex_id VARCHAR(50) PRIMARY KEY,
        resolution INTEGER NOT NULL,
        population DECIMAL(10,2),
        population_density DECIMAL(10,4),
        center_lat DECIMAL(10,7),
        center_lon DECIMAL(10,7),
        area_km2 DECIMAL(10,6),
        original_h3 VARCHAR(50),
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE INDEX idx_h3_population_density ON demographics.h3_population (population_density DESC);
    CREATE INDEX idx_h3_population_resolution ON demographics.h3_population (resolution);
    """
    
    with engine.connect() as conn:
        conn.execute(create_table_sql)
        conn.commit()
    
    # Записати дані без geometry спочатку
    df_no_geom = result_gdf.drop('geometry', axis=1)
    df_no_geom.to_sql(
        'h3_population', 
        engine, 
        schema='demographics',
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )
    
    print(f"✅ Successfully imported {len(result_gdf)} population records!")
    
    # Статистика
    print("\n📈 Import statistics:")
    print(f"Total hexagons: {len(result_gdf)}")
    print(f"Total population: {result_gdf['population'].sum():,.0f}")
    print(f"Avg population per hex: {result_gdf['population'].mean():.1f}")
    print(f"Max density: {result_gdf['population_density'].max():.1f} people/km²")
    print(f"Resolution: {result_gdf['resolution'].iloc[0]}")
    
    # Перевірити топ-10 найщільніших районів
    top_dense = result_gdf.nlargest(10, 'population_density')[['hex_id', 'population', 'population_density', 'center_lat', 'center_lon']]
    print(f"\n🏙️ Top 10 most dense areas:")
    print(top_dense)

if __name__ == "__main__":
    import_population_data()