import pandas as pd

# Читаємо файл
df = pd.read_csv('C:/projects/Avrora/Competitors.csv', sep=';', encoding='utf-8')

# Перевіряємо дублікати по координатах
import re

coords_list = []
for idx, row in df.iterrows():
    geom = row['geometry']
    match = re.search(r'POINT\s*\(\s*([\d.-]+)\s+([\d.-]+)\s*\)', str(geom))
    if match:
        lon, lat = match.groups()
        coords_list.append((float(lat), float(lon), row['conc_name']))

# Шукаємо дублікати
from collections import Counter
coord_pairs = [(lat, lon) for lat, lon, _ in coords_list]
duplicates = {k: v for k, v in Counter(coord_pairs).items() if v > 1}

print(f"Всього конкурентів: {len(coords_list)}")
print(f"Унікальних координат: {len(set(coord_pairs))}")
print(f"Дублікатів: {len(duplicates)}")

if duplicates:
    print("\nПриклади дублікатів:")
    for (lat, lon), count in list(duplicates.items())[:5]:
        print(f"  ({lat:.6f}, {lon:.6f}): {count} конкурентів")
        # Показати які саме
        for clat, clon, name in coords_list:
            if clat == lat and clon == lon:
                print(f"    - {name}")