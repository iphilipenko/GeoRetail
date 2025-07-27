# scripts/import_population_final.py
import geopandas as gpd
import h3
from sqlalchemy import create_engine
from tqdm import tqdm
import pandas as pd

def import_population_final():
    print("🇺🇦 Importing Ukraine population data...")
    
    # Правильні credentials (без паролю)
    try:
        engine = create_engine('postgresql://georetail_user:@localhost:5432/georetail')
        with engine.connect() as conn:
            conn.execute("SELECT 1;")
        print("✅ Connected without password")
    except:
        try:
            # Або з паролем georetail_password
            engine = create_engine('postgresql://georetail_user:georetail_password@localhost:5432/georetail')
            with engine.connect() as conn:
                conn.execute("SELECT 1;")
            print("✅ Connected with georetail_password")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return
    
    # Читання даних
    gpkg_path = r"C:\projects\AA AI Assistance\GeoRetail_git\kontur_population_UA_20231101.gpkg"
    gdf = gpd.read_file(gpkg_path, layer='population')
    
    print(f"📊 Processing {len(gdf)} records...")
    
    # Обробка даних
    processed_data = []
    
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf)):
        h3_id = row['h3']
        population = row['population']
        
        lat, lon = h3.cell_to_latlng(h3_id)
        resolution = h3.get_resolution(h3_id)
        area_km2 = h3.cell_area(h3_id, unit='km^2')
        population_density = population / area_km2
        
        processed_data.append({
            'hex_id': h3_id,
            'resolution': resolution,
            'population': population,
            'population_density': population_density,
            'center_lat': lat,
            'center_lon': lon,
            'area_km2': area_km2
        })
    
    df = pd.DataFrame(processed_data)
    
    # Створити структуру бази
    print("💾 Setting up database...")
    
    with engine.connect() as conn:
        # Створити схему demographics
        conn.execute("CREATE SCHEMA IF NOT EXISTS demographics;")
        
        # Видалити таблицю якщо існує
        conn.execute("DROP TABLE IF EXISTS demographics.h3_population CASCADE;")
        
        # Створити нову таблицю
        conn.execute("""
            CREATE TABLE demographics.h3_population (
                hex_id VARCHAR(50) PRIMARY KEY,
                resolution INTEGER NOT NULL,
                population DECIMAL(10,2) NOT NULL,
                population_density DECIMAL(10,4) NOT NULL,
                center_lat DECIMAL(10,7) NOT NULL,
                center_lon DECIMAL(10,7) NOT NULL,
                area_km2 DECIMAL(10,6) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Створити індекси
        conn.execute("CREATE INDEX idx_h3_pop_density ON demographics.h3_population (population_density DESC);")
        conn.execute("CREATE INDEX idx_h3_pop_resolution ON demographics.h3_population (resolution);")
        conn.execute("CREATE INDEX idx_h3_pop_location ON demographics.h3_population (center_lat, center_lon);")
        
        conn.commit()
    
    # Імпорт даних
    print("📥 Importing data to PostgreSQL...")
    df.to_sql(
        'h3_population', 
        engine, 
        schema='demographics',
        if_exists='append',
        index=False,
        chunksize=2000,
        method='multi'
    )
    
    print(f"✅ Successfully imported {len(df)} population records!")
    
    # Перевірка
    with engine.connect() as conn:
        result = conn.execute("SELECT COUNT(*) FROM demographics.h3_population;")
        count = result.fetchone()[0]
        
        result = conn.execute("SELECT SUM(population) FROM demographics.h3_population;")
        total_pop = result.fetchone()[0]
        
        result = conn.execute("SELECT MAX(population_density) FROM demographics.h3_population;")
        max_density = result.fetchone()[0]
    
    print(f"\n📈 Import verification:")
    print(f"Records in database: {count:,}")
    print(f"Total population: {total_pop:,.0f}")
    print(f"Max density: {max_density:,.1f} people/km²")
    print(f"✅ Import completed successfully!")

if __name__ == "__main__":
    import_population_final()