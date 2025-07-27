# scripts/import_shops_data.py
import pandas as pd
import h3
from sqlalchemy import create_engine, text
from tqdm import tqdm

def import_shops_data():
    print("🏪 Importing shops data...")
    
    # Підключення до бази
    engine = create_engine('postgresql://georetail_user:georetail_secure_2024@localhost:5432/georetail')
    
    # Читання Excel файлу
    excel_path = r"C:\projects\AA AI Assistance\GeoRetail_git\Shops.xlsx"
    
    try:
        df = pd.read_excel(excel_path)
        print(f"📊 Loaded {len(df)} shops from Excel")
        print(f"📊 Columns: {list(df.columns)}")
        
        # Показати перші кілька записів
        print(f"\n📋 Sample data:")
        print(df.head(3))
        
    except Exception as e:
        print(f"❌ Error reading Excel: {e}")
        return
    
    # Очистити назви колонок (видалити пробіли, спецсимволи)
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace(',', '_').str.replace('(', '').str.replace(')', '')
    
    print(f"\n📊 Cleaned columns: {list(df.columns)}")
    
    # Додати H3 гексагони для кожного магазину
    print("🔄 Adding H3 hexagon data...")
    
    h3_data = []
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing shops"):
        lat = row['lat']
        lon = row['lon']
        
        # Генерувати H3 для різних резолюцій
        h3_res_8 = h3.latlng_to_cell(lat, lon, 8)  # ~700м радіус
        h3_res_9 = h3.latlng_to_cell(lat, lon, 9)  # ~350м радіус
        h3_res_10 = h3.latlng_to_cell(lat, lon, 10)  # ~180м радіус
        
        h3_data.append({
            'shop_id': idx + 1,
            'h3_res_8': h3_res_8,
            'h3_res_9': h3_res_9, 
            'h3_res_10': h3_res_10
        })
    
    # Додати H3 дані до DataFrame
    h3_df = pd.DataFrame(h3_data)
    df = df.reset_index(drop=True)
    df = pd.concat([df, h3_df], axis=1)
    
    # Створити таблицю для магазинів
    print("💾 Creating shops table...")
    
    with engine.connect() as conn:
        # Створити схему для магазинів
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stores;"))
        
        # Видалити таблицю якщо існує
        conn.execute(text("DROP TABLE IF EXISTS stores.shops CASCADE;"))
        
        # Створити нову таблицю
        conn.execute(text("""
            CREATE TABLE stores.shops (
                shop_id SERIAL PRIMARY KEY,
                lat DECIMAL(10,7) NOT NULL,
                lon DECIMAL(10,7) NOT NULL,
                format VARCHAR(50),
                qntty_sku INTEGER,
                qntty_clusters INTEGER,
                location_features VARCHAR(100),
                square_trade DECIMAL(8,2),
                square_total DECIMAL(8,2),
                bakery_full_cycle INTEGER,
                bakery_short_cycle INTEGER,
                meat_kg INTEGER,
                meat_sht INTEGER,
                pizza_revenue INTEGER,
                bakery_revenue INTEGER,
                food_to_go_revenue INTEGER,
                avg_month_n_checks DECIMAL(12,2),
                avg_check_sum DECIMAL(10,4),
                revenue DECIMAL(15,2),
                h3_res_8 VARCHAR(50),
                h3_res_9 VARCHAR(50),
                h3_res_10 VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """))
        
        # Створити індекси
        conn.execute(text("CREATE INDEX idx_shops_location ON stores.shops (lat, lon);"))
        conn.execute(text("CREATE INDEX idx_shops_format ON stores.shops (format);"))
        conn.execute(text("CREATE INDEX idx_shops_revenue ON stores.shops (revenue DESC);"))
        conn.execute(text("CREATE INDEX idx_shops_h3_8 ON stores.shops (h3_res_8);"))
        conn.execute(text("CREATE INDEX idx_shops_h3_9 ON stores.shops (h3_res_9);"))
        
        conn.commit()
    
    # Імпорт даних
    print("📥 Importing shops to PostgreSQL...")
    
    # Підготувати DataFrame для імпорту
    df_import = df.copy()
    
    # Перейменувати колонки щоб співпадали з таблицею
    column_mapping = {
        'lat': 'lat',
        'lon': 'lon', 
        'format': 'format',
        'qntty_SKU': 'qntty_sku',
        'qntty_clusters': 'qntty_clusters',
        'location_features': 'location_features',
        'square_trade': 'square_trade',
        'square_total': 'square_total',
        'bakery_full_cycle': 'bakery_full_cycle',
        'bakery_short_cycle': 'bakery_short_cycle',
        'meat_кг': 'meat_kg',
        'meat_шт': 'meat_sht',
        'pizza_revenue': 'pizza_revenue',
        'bakery_revenue': 'bakery_revenue',
        'food_to_go_revenue': 'food_to_go_revenue',
        'avg_month_n_checks': 'avg_month_n_checks',
        'avg_check_sum': 'avg_check_sum',
        'revenue': 'revenue'
    }
    
    # Вибрати тільки потрібні колонки
    available_columns = [col for col in column_mapping.keys() if col in df_import.columns]
    df_final = df_import[available_columns + ['h3_res_8', 'h3_res_9', 'h3_res_10']].copy()
    
    # Перейменувати колонки
    df_final = df_final.rename(columns=column_mapping)
    
    # Імпортувати в базу
    df_final.to_sql(
        'shops', 
        engine, 
        schema='stores',
        if_exists='append',
        index=False,
        chunksize=50
    )
    
    print(f"✅ Successfully imported {len(df_final)} shops!")
    
    # Статистика
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stores.shops;"))
        count = result.fetchone()[0]
        
        result = conn.execute(text("SELECT COUNT(DISTINCT format) FROM stores.shops;"))
        formats_count = result.fetchone()[0]
        
        result = conn.execute(text("SELECT format, COUNT(*) FROM stores.shops GROUP BY format;"))
        formats = result.fetchall()
        
        result = conn.execute(text("SELECT AVG(revenue), MAX(revenue), MIN(revenue) FROM stores.shops;"))
        revenue_stats = result.fetchone()
    
    print(f"\n📈 Shops import statistics:")
    print(f"Total shops: {count}")
    print(f"Different formats: {formats_count}")
    print(f"Format distribution:")
    for fmt in formats:
        print(f"  {fmt[0]}: {fmt[1]} shops")
    print(f"Revenue stats: avg={revenue_stats[0]:,.0f}, max={revenue_stats[1]:,.0f}, min={revenue_stats[2]:,.0f}")
    
    print(f"\n✅ Shops data import completed!")
    return df_final

if __name__ == "__main__":
    shops_df = import_shops_data()