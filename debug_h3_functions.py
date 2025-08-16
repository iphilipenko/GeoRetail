# debug_h3_functions.py
"""
Діагностика H3 функцій для розуміння проблеми
"""

import h3

def test_h3_functions():
    """Тестування різних H3 функцій"""
    print(f"H3 version: {getattr(h3, '__version__', 'unknown')}")
    
    # Тестовий H3 індекс для resolution 10
    test_h3 = "8a1fb6699bfffff"
    
    print(f"\nДоступні функції H3:")
    h3_functions = [f for f in dir(h3) if not f.startswith('_')]
    for func in sorted(h3_functions):
        print(f"  - {func}")
    
    print(f"\nТестування функцій:")
    
    # Тест 1: Валідація
    try:
        valid = h3.h3_is_valid(test_h3)
        print(f"✅ h3_is_valid('{test_h3}'): {valid}")
    except Exception as e:
        try:
            valid = h3.is_valid_cell(test_h3)
            print(f"✅ is_valid_cell('{test_h3}'): {valid}")
        except Exception as e2:
            print(f"❌ Validation failed: {e}, {e2}")
    
    # Тест 2: Координати центру
    try:
        coords = h3.h3_to_geo(test_h3)
        print(f"✅ h3_to_geo: {coords}")
    except Exception as e:
        try:
            coords = h3.cell_to_latlng(test_h3)
            print(f"✅ cell_to_latlng: {coords}")
        except Exception as e2:
            print(f"❌ Coordinates failed: {e}, {e2}")
    
    # Тест 3: Площа гексагона
    resolution = 10
    print(f"\nТестування площі для resolution {resolution}:")
    
    # Спробуємо всі можливі варіанти
    area_functions = [
        ('h3.hex_area(resolution)', lambda: h3.hex_area(resolution)),
        ('h3.hex_area(resolution, "km^2")', lambda: h3.hex_area(resolution, "km^2")),
        ('h3.hex_area(resolution, unit="km^2")', lambda: h3.hex_area(resolution, unit="km^2")),
        ('h3.cell_area(resolution)', lambda: h3.cell_area(resolution)),
        ('h3.cell_area(resolution, "km^2")', lambda: h3.cell_area(resolution, "km^2")),
        ('h3.cell_area(resolution, unit="km^2")', lambda: h3.cell_area(resolution, unit="km^2")),
        ('h3.hex_area_km2(resolution)', lambda: h3.hex_area_km2(resolution)),
        ('h3.hex_area_m2(resolution)', lambda: h3.hex_area_m2(resolution)),
    ]
    
    for name, func in area_functions:
        try:
            result = func()
            print(f"✅ {name}: {result}")
        except Exception as e:
            print(f"❌ {name}: {e}")
    
    # Тест 4: Альтернативні способи отримання площі
    print(f"\nАльтернативні способи:")
    try:
        # Через константи або таблиці
        if hasattr(h3, 'EARTH_RADIUS_KM'):
            print(f"✅ EARTH_RADIUS_KM: {h3.EARTH_RADIUS_KM}")
        
        # Спробуємо розрахувати вручну через кількість пікселів
        import math
        
        # Для resolution 10, приблизна площа
        approximate_areas = {
            7: 5.161293360,    # km²
            8: 0.737327598,    # km²  
            9: 0.105332513,    # km²
            10: 0.015047502    # km²
        }
        
        if resolution in approximate_areas:
            approx_area = approximate_areas[resolution]
            print(f"✅ Approximate area for res {resolution}: {approx_area} km²")
            
    except Exception as e:
        print(f"❌ Alternative methods failed: {e}")

if __name__ == "__main__":
    test_h3_functions()
