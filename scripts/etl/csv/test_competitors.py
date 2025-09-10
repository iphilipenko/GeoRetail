import pandas as pd
import re

# Читаємо файл
df = pd.read_csv('C:/projects/Avrora/Competitors.csv', sep=';', encoding='utf-8')

print(f"Кількість записів: {len(df)}")
print(f"Колонки (repr): {df.columns.tolist()}")
print(f"Колонки (назви): {[repr(col) for col in df.columns]}")

# Перевіряємо geometry колонку
geom_col = None
for col in df.columns:
    if 'geometry' in col.lower():
        geom_col = col
        print(f"\nЗнайдена колонка geometry: '{col}' (repr: {repr(col)})")
        break

if geom_col:
    # Перевіряємо формат
    sample = df[geom_col].iloc[0]
    print(f"\nПриклад geometry: {sample}")
    print(f"Тип: {type(sample)}")
    
    # Тестуємо regex
    pattern = r'POINT\s*\(\s*([\d.-]+)\s+([\d.-]+)\s*\)'
    match = re.search(pattern, str(sample))
    
    if match:
        print(f"\nRegex спрацював!")
        print(f"  lon: {match.group(1)}")
        print(f"  lat: {match.group(2)}")
        
        # Перевіряємо валідацію
        lon = float(match.group(1))
        lat = float(match.group(2))
        print(f"\nВалідація координат:")
        print(f"  44 <= {lat} <= 52: {44 <= lat <= 52}")
        print(f"  22 <= {lon} <= 40: {22 <= lon <= 40}")
    else:
        print(f"\nRegex НЕ спрацював!")
        print("Спробуємо інший патерн...")
        
        # Альтернативний патерн
        pattern2 = r'POINT\s*\(([\d.-]+)\s+([\d.-]+)\)'
        match2 = re.search(pattern2, str(sample))
        if match2:
            print("Альтернативний патерн спрацював!")

# Перевірка скільки валідних geometry
valid_count = 0
invalid_samples = []

for idx, row in df.iterrows():
    geom = row.get(geom_col) if geom_col else row.get('geometry')
    if pd.notna(geom):
        pattern = r'POINT\s*\(\s*([\d.-]+)\s+([\d.-]+)\s*\)'
        match = re.search(pattern, str(geom))
        if match:
            lon = float(match.group(1))
            lat = float(match.group(2))
            if 44 <= lat <= 52 and 22 <= lon <= 40:
                valid_count += 1
            else:
                if len(invalid_samples) < 5:
                    invalid_samples.append((idx, lat, lon, geom))
    
    if idx >= 100:  # Перевіряємо перші 100
        break

print(f"\nВалідних координат (з перших 100): {valid_count}")
if invalid_samples:
    print("\nПриклади невалідних:")
    for idx, lat, lon, geom in invalid_samples:
        print(f"  Row {idx}: lat={lat}, lon={lon}")