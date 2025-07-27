# scripts/import_population_sqlalchemy_fixed.py
import geopandas as gpd
import h3
from sqlalchemy import create_engine, text
from tqdm import tqdm
import pandas as pd

def import_population_fixed():
    print("🇺🇦 Importing Ukraine population data...")
    
    # Правильний пароль з text() для raw SQL
    engine = create_engine('postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail')
    
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1;"))
        print("✅ Connected successfully!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return
    
    # Читання даних
    gpkg_path = r"C:\projects\AA AI Assistance\GeoRetail_git\kontur_population_UA_20231101.gpkg"
    gdf = gpd.read_file(gpkg_path, layer='population')
    
    print(f"📊 Processing {len(gdf)} records...")
    
    # Обробка даних
    processed_data = []
    
    for idx, row in tqdm(gdf.iterrows(), total=len(gdf), desc="Processing H3 data"):
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
    print("💾 Setting up database structure...")
    
    with engine.connect() as conn:
        # Видалити таблицю якщо існує
        conn.execute(text("DROP TABLE IF EXISTS demographics.h3_population CASCADE;"))
        
        # Створити нову таблицю
        conn.execute(text("""
            CREATE TABLE demographics.h3_population (
                id SERIAL PRIMARY KEY,
                hex_id VARCHAR(50) UNIQUE NOT NULL,
                resolution INTEGER NOT NULL,
                population DECIMAL(10,2) NOT NULL,
                population_density DECIMAL(10,4) NOT NULL,
                center_lat DECIMAL(10,7) NOT NULL,
                center_lon DECIMAL(10,7) NOT NULL,
                area_km2 DECIMAL(10,6) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))
        
        # Створити індекси
        conn.execute(text("CREATE INDEX idx_h3_pop_hex_id ON demographics.h3_population (hex_id);"))
        conn.execute(text("CREATE INDEX idx_h3_pop_density ON demographics.h3_population (population_density DESC);"))
        conn.execute(text("CREATE INDEX idx_h3_pop_resolution ON demographics.h3_population (resolution);"))
        conn.execute(text("CREATE INDEX idx_h3_pop_location ON demographics.h3_population (center_lat, center_lon);"))
        
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
    
    # Перевірка та статистика
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM demographics.h3_population;"))
        count = result.fetchone()[0]
        
        result = conn.execute(text("SELECT SUM(population) FROM demographics.h3_population;"))
        total_pop = result.fetchone()[0]
        
        result = conn.execute(text("SELECT MAX(population_density) FROM demographics.h3_population;"))
        max_density = result.fetchone()[0]
        
        # Топ-5 найщільніших районів
        result = conn.execute(text("""
            SELECT hex_id, population, population_density, center_lat, center_lon 
            FROM demographics.h3_population 
            ORDER BY population_density DESC 
            LIMIT 5;
        """))
        top_areas = result.fetchall()
    
    print(f"\n📈 Import verification:")
    print(f"Records in database: {count:,}")
    print(f"Total population: {total_pop:,.0f}")
    print(f"Max density: {max_density:,.1f} people/km²")
    
    print(f"\n🏙️ Top 5 most dense areas:")
    for area in top_areas:
        print(f"  {area[0]}: {area[1]:.0f} people, {area[2]:.0f}/km² at ({area[3]:.4f}, {area[4]:.4f})")
    
    print(f"\n✅ Population data import completed successfully!")

if __name__ == "__main__":
    import_population_fixed()