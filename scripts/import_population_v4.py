# scripts/import_population_v4.py
import geopandas as gpd
import h3
from sqlalchemy import create_engine
from tqdm import tqdm
import pandas as pd

def import_population_data():
    print("🇺🇦 Importing Ukraine population data...")
    print(f"H3 version: {h3.__version__}")
    
    # Підключення до бази
    engine = create_engine('postgresql://georetail_user:georetail_password@localhost:5432/georetail')
    
    # Читання GPKG
    gpkg_path = r"C:\projects\AA AI Assistance\GeoRetail_git\kontur_population_UA_20231101.gpkg"
    
    print("📖 Reading GPKG file...")
    gdf = gpd.read_file(gpkg_path, layer='population')
    
    print(f"📊 Processing {len(gdf)} population records...")
    
    # Тест з першим H3 (правильні функції для v4.3.0)
    sample_h3 = gdf.iloc[0]['h3']
    print(f"Testing with: {sample_h3}")
    
    try:
        # H3 v4.3.0 функції
        lat, lon = h3.cell_to_latlng(sample_h3)
        resolution = h3.get_resolution(sample_h3)
        area_km2 = h3.cell_area(sample_h3, unit='km^2')
        
        print(f"✅ H3 functions working!")
        print(f"Resolution: {resolution}")
        print(f"Center: {lat:.6f}, {lon:.6f}")
        print(f"Area: {area_km2:.6f} km²")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return
    
    # Якщо тест пройшов, продовжуємо
    print("🔄 Processing all records...")
    
    processed_data = []
    errors = 0
    
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf), desc="Converting"):
        try:
            h3_id = row['h3']
            population = row['population']
            
            # H3 v4.3.0 API
            lat, lon = h3.cell_to_latlng(h3_id)
            resolution = h3.get_resolution(h3_id)
            area_km2 = h3.cell_area(h3_id, unit='km^2')
            
            # Розрахувати щільність
            population_density = population / area_km2 if area_km2 > 0 else 0
            
            processed_data.append({
                'hex_id': h3_id,
                'resolution': resolution,
                'population': float(population),
                'population_density': float(population_density),
                'center_lat': lat,
                'center_lon': lon,
                'area_km2': area_km2
            })
            
        except Exception as e:
            errors += 1
            if errors < 5:  # Показати перші 5 помилок
                print(f"Error processing row {idx}: {e}")
    
    print(f"✅ Processed {len(processed_data)} records, {errors} errors")
    
    if len(processed_data) == 0:
        print("❌ No valid data to import")
        return
    
    # Створити DataFrame
    df = pd.DataFrame(processed_data)
    
    print("💾 Writing to PostGIS...")
    
    # Спочатку створити таблицю
    try:
        with engine.connect() as conn:
            conn.execute("""
                CREATE SCHEMA IF NOT EXISTS demographics;
            """)
            conn.commit()
            
            conn.execute("""
                DROP TABLE IF EXISTS demographics.h3_population;
            """)
            conn.commit()
            
            conn.execute("""
                CREATE TABLE demographics.h3_population (
                    hex_id VARCHAR(50) PRIMARY KEY,
                    resolution INTEGER NOT NULL,
                    population DECIMAL(10,2),
                    population_density DECIMAL(10,4),
                    center_lat DECIMAL(10,7),
                    center_lon DECIMAL(10,7),
                    area_km2 DECIMAL(10,6),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            
            conn.execute("""
                CREATE INDEX idx_h3_population_density ON demographics.h3_population (population_density DESC);
            """)
            conn.commit()
            
            conn.execute("""
                CREATE INDEX idx_h3_population_resolution ON demographics.h3_population (resolution);
            """)
            conn.commit()
            
        print("✅ Database table created successfully")
        
    except Exception as e:
        print(f"❌ Error creating table: {e}")
        return
    
    # Записати дані
    try:
        df.to_sql(
            'h3_population', 
            engine, 
            schema='demographics',
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        print(f"✅ Successfully imported {len(df)} population records!")
        
    except Exception as e:
        print(f"❌ Error importing data: {e}")
        return
    
    # Статистика
    print("\n📈 Import statistics:")
    print(f"Total hexagons: {len(df)}")
    print(f"Total population: {df['population'].sum():,.0f}")
    print(f"Avg population per hex: {df['population'].mean():.1f}")
    print(f"Max density: {df['population_density'].max():.1f} people/km²")
    print(f"Min density: {df['population_density'].min():.1f} people/km²")
    print(f"Resolution: {df['resolution'].iloc[0]}")
    
    # Топ-10 найщільніших районів
    print(f"\n🏙️ Top 10 most dense areas:")
    top_dense = df.nlargest(10, 'population_density')[['hex_id', 'population', 'population_density', 'center_lat', 'center_lon']]
    for _, row in top_dense.iterrows():
        print(f"  {row['hex_id']}: {row['population']:.0f} people, {row['population_density']:.0f}/km² at ({row['center_lat']:.4f}, {row['center_lon']:.4f})")

if __name__ == "__main__":
    import_population_data()