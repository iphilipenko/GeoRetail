#!/usr/bin/env python3
"""
Швидкий фікс для додання експорту router в h3_modal_endpoints.py
"""

import os

def fix_h3_modal_router():
    """Додає експорт router в кінець файлу"""
    
    file_path = r'C:\projects\AA AI Assistance\GeoRetail_git\GeoRetail\src\api\endpoints\h3_modal_endpoints.py'
    
    # Читаємо поточний вміст
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Перевіряємо чи вже є експорт
    if '__all__' in content and 'router' in content:
        print("✅ Експорт router вже існує")
        return
    
    # Додаємо експорт
    export_code = """

# ===============================================
# ROUTER EXPORT  
# ===============================================

# Експорт router для використання в main_safe.py
__all__ = ["router"]
"""
    
    # Записуємо назад
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content + export_code)
    
    print("✅ Додано експорт router в h3_modal_endpoints.py")

if __name__ == "__main__":
    fix_h3_modal_router()
