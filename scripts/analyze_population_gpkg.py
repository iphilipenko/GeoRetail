# scripts/analyze_population_gpkg.py
import geopandas as gpd
import h3
import fiona

def analyze_population_file():
    gpkg_path = r"C:\projects\AA AI Assistance\GeoRetail_git\kontur_population_UA_20231101.gpkg"
    
    try:
        # Перевірити шари (правильний спосіб)
        layers = fiona.listlayers(gpkg_path)
        print(f"📋 Layers in file: {layers}")
        
        # Читання даних (використовуємо перший шар)
        layer_name = layers[0] if layers else None
        print(f"📖 Reading layer: {layer_name}")
        
        gdf = gpd.read_file(gpkg_path, layer=layer_name)
        print(f"📊 Total records: {len(gdf)}")
        print(f"📊 Columns: {list(gdf.columns)}")
        print(f"📊 Data types: {gdf.dtypes}")
        
        print(f"\n📊 Sample data (first 3 rows):")
        print(gdf.head(3))
        
        # Знайти колонку з H3
        h3_column = None
        for col in gdf.columns:
            if 'h3' in col.lower() or 'hex' in col.lower():
                h3_column = col
                break
        
        if h3_column:
            print(f"\n🔍 Found H3 column: '{h3_column}'")
            sample_hex = gdf.iloc[0][h3_column]
            print(f"🔍 Sample H3 ID: {sample_hex}")
            
            try:
                resolution = h3.h3_get_resolution(sample_hex)
                edge_length = h3.edge_length(resolution, unit='m')
                print(f"🔍 H3 Resolution: {resolution}")
                print(f"📏 Edge length: {edge_length:.0f} meters")
            except:
                print("❌ Invalid H3 format")
        else:
            print("❌ No H3 column found")
        
        # Знайти колонку з population
        pop_column = None
        for col in gdf.columns:
            if any(word in col.lower() for word in ['pop', 'population', 'value', 'count']):
                pop_column = col
                break
        
        if pop_column:
            print(f"\n👥 Found population column: '{pop_column}'")
            print(f"👥 Population stats:")
            print(f"   Total: {gdf[pop_column].sum():,.0f}")
            print(f"   Average: {gdf[pop_column].mean():.1f}")
            print(f"   Max: {gdf[pop_column].max():,.0f}")
            print(f"   Min: {gdf[pop_column].min():.1f}")
        else:
            print("❌ No population column found")
            
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        print("\n🔍 Let's try a simpler approach...")
        
        # Спрощений підхід
        try:
            gdf = gpd.read_file(gpkg_path)
            print(f"📊 Successfully read {len(gdf)} records")
            print(f"📊 Columns: {list(gdf.columns)}")
        except Exception as e2:
            print(f"❌ Still failed: {e2}")

if __name__ == "__main__":
    analyze_population_file()