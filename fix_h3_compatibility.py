# Швидке виправлення H3 сумісності
import re

# Читаємо файл
with open(r'C:\projects\AA AI Assistance\GeoRetail_git\GeoRetail\src\api\endpoints\h3_modal_endpoints.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Замінюємо всі старі функції на нові
replacements = [
    ('h3.h3_is_valid', 'h3.is_valid_cell'),
    ('h3.h3_to_geo', 'h3.cell_to_latlng'),
    ('h3.k_ring', 'h3.grid_ring'),
    ('h3.hex_area(resolution, unit=\'km^2\')', 'h3.average_hexagon_area(resolution, unit=\'km^2\')'),
    ('h3.hex_area(resolution)', 'h3.average_hexagon_area(resolution, unit=\'km^2\')'),
]

for old, new in replacements:
    content = content.replace(old, new)

# Записуємо назад
with open(r'C:\projects\AA AI Assistance\GeoRetail_git\GeoRetail\src\api\endpoints\h3_modal_endpoints.py', 'w', encoding='utf-8') as f:
    f.write(content)

print("✅ Виправлено всі H3 функції для v4.x сумісності")
