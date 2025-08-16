# API_TEST_REQUESTS.md

# 🧪 H3 Modal API - Тестові запити

## 🚀 **Запуск сервера**
```bash
cd "C:\projects\AA AI Assistance\GeoRetail_git\GeoRetail"
python start_test_server.py
```

## 🔧 **Базові тести підключення**

### 1. Health Check
```bash
curl http://localhost:8000/health
```

### 2. Тест підключення до БД
```bash
curl http://localhost:8000/api/v1/database/test-connection
```

### 3. Перевірка Swagger UI
```
http://localhost:8000/docs
```

## 📊 **H3 Modal API Tests**

### Тестові H3 індекси для Києва:
- **H3-7**: `871fb4662ffffff` (район)
- **H3-8**: `881fb46622fffff` (частина району)
- **H3-9**: `891fb466227ffff` (квартал)
- **H3-10**: `8a1fb46622d7fff` (вулиця)

### 4. Coverage Calculator
```bash
# Базовий тест
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=2"

# Різні resolutions
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=8&rings=3"
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=9&rings=1"
```

### 5. Analysis Preview
```bash
# Для H3-10
curl "http://localhost:8000/api/v1/hexagon-details/analysis-preview/8a1fb46622d7fff?resolution=10"

# Для H3-9
curl "http://localhost:8000/api/v1/hexagon-details/analysis-preview/891fb466227ffff?resolution=9"

# Для H3-8
curl "http://localhost:8000/api/v1/hexagon-details/analysis-preview/881fb46622fffff?resolution=8"
```

### 6. Hexagon Details - Різні типи аналізу
```bash
# Пішохідна конкуренція (за замовчуванням)
curl "http://localhost:8000/api/v1/hexagon-details/details/8a1fb46622d7fff?resolution=10"

# Транспортна доступність
curl "http://localhost:8000/api/v1/hexagon-details/details/8a1fb46622d7fff?resolution=10&analysis_type=transport_accessibility"

# Огляд ринку
curl "http://localhost:8000/api/v1/hexagon-details/details/881fb46622fffff?resolution=8&analysis_type=market_overview"

# Вибір локації
curl "http://localhost:8000/api/v1/hexagon-details/details/891fb466227ffff?resolution=9&analysis_type=site_selection"

# Користувацький аналіз
curl "http://localhost:8000/api/v1/hexagon-details/details/8a1fb46622d7fff?resolution=10&analysis_type=custom&custom_rings=4"
```

### 7. POI в гексагоні
```bash
# Тільки центральний гексагон
curl "http://localhost:8000/api/v1/hexagon-details/poi-in-hexagon/8a1fb46622d7fff?resolution=10&include_neighbors=0"

# З 1 кільцем сусідів
curl "http://localhost:8000/api/v1/hexagon-details/poi-in-hexagon/8a1fb46622d7fff?resolution=10&include_neighbors=1"

# З 2 кільцями сусідів
curl "http://localhost:8000/api/v1/hexagon-details/poi-in-hexagon/891fb466227ffff?resolution=9&include_neighbors=2"
```

### 8. Конкурентний аналіз
```bash
# Базовий аналіз (2 кільця)
curl "http://localhost:8000/api/v1/hexagon-details/competitive-analysis/8a1fb46622d7fff?resolution=10&radius_rings=2"

# Розширений аналіз (3 кільця)
curl "http://localhost:8000/api/v1/hexagon-details/competitive-analysis/891fb466227ffff?resolution=9&radius_rings=3"

# Широкий аналіз (4 кільця)
curl "http://localhost:8000/api/v1/hexagon-details/competitive-analysis/881fb46622fffff?resolution=8&radius_rings=4"
```

### 9. Тест специфічних H3 даних
```bash
# Тест існування H3 в БД
curl "http://localhost:8000/api/v1/database/test-h3/8a1fb46622d7fff"
curl "http://localhost:8000/api/v1/database/test-h3/891fb466227ffff"
curl "http://localhost:8000/api/v1/database/test-h3/881fb46622fffff"
```

## ⚠️ **Тести помилок (Error Handling)**

### 10. Неправильні H3 індекси
```bash
# Неправильний формат
curl "http://localhost:8000/api/v1/hexagon-details/details/invalid_h3?resolution=10"

# Неправильна довжина
curl "http://localhost:8000/api/v1/hexagon-details/details/123?resolution=10"

# Пустий індекс
curl "http://localhost:8000/api/v1/hexagon-details/details/?resolution=10"
```

### 11. Неправильні параметри
```bash
# Неправильний resolution
curl "http://localhost:8000/api/v1/hexagon-details/details/8a1fb46622d7fff?resolution=15"

# Негативні кільця
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=-1"

# Занадто великі кільця
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=20"
```

## 🌐 **Браузерні тести**

### 12. Відкрити в браузері
```
# Swagger UI
http://localhost:8000/docs

# ReDoc
http://localhost:8000/redoc

# Health endpoint
http://localhost:8000/health

# Database test
http://localhost:8000/api/v1/database/test-connection

# Coverage calculator
http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=2

# Analysis preview
http://localhost:8000/api/v1/hexagon-details/analysis-preview/8a1fb46622d7fff?resolution=10

# POI endpoint
http://localhost:8000/api/v1/hexagon-details/poi-in-hexagon/8a1fb46622d7fff?resolution=10&include_neighbors=1
```

## 📋 **Перевірочний чек-лист**

### ✅ Що повинно працювати:
1. **Health Check** → `{"status": "healthy", ...}`
2. **Database Connection** → `{"status": "success", "version": "PostgreSQL ...", ...}`
3. **Coverage Calculator** → JSON з `{"resolution": 10, "rings": 2, "total_area_km2": ..., ...}`
4. **Analysis Preview** → JSON з `{"available_analyses": [...], ...}`
5. **Hexagon Details** → JSON з `{"location_info": {...}, "metrics": {...}, "poi_details": [...], ...}`
6. **POI in Hexagon** → JSON з `{"poi_summary": {...}, "poi_details": [...], ...}`
7. **Competitive Analysis** → JSON з `{"competitive_analysis": {...}, "competitors": [...], ...}`

### ⚠️ Очікувані помилки (це нормально):
- **Database errors** - якщо PostgreSQL не підключений
- **H3 not found** - якщо конкретний H3 індекс не існує в БД
- **Validation errors** - для неправильних параметрів

### 🎯 Успішний тест означає:
- ✅ Сервер запускається без помилок
- ✅ Endpoints повертають JSON структури
- ✅ H3 математика працює коректно
- ✅ Валідація параметрів спрацьовує
- ✅ Swagger UI доступний та показує всі endpoints

## 🚀 **Швидкий тест**

### Мінімальний набір для перевірки:
```bash
# 1. Запустіть сервер
python start_test_server.py

# 2. В іншому терміналі:
curl http://localhost:8000/health
curl http://localhost:8000/api/v1/database/test-connection
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=2"

# 3. Відкрийте браузер:
# http://localhost:8000/docs
```

---

**🎉 Готово до тестування!**

**Якщо все працює** → API готове для інтеграції з frontend  
**Якщо є помилки** → перевірте логи сервера та налаштування БД
