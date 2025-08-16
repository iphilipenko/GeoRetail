# 🧪 H3 Modal API Testing Guide

## 🚀 Швидкий старт тестування

### Варіант 1: Автоматичний тест (Windows)
```bash
# Запустіть батник для автоматичного тестування
double-click test_h3_api.bat
```

### Варіант 2: Ручний запуск
```bash
# 1. Запустіть тестовий сервер
python start_test_server.py

# 2. В іншому терміналі запустіть тести
python test_api_manually.py
```

### Варіант 3: Використання існуючого сервера
```bash
# Якщо сервер вже запущений, просто запустіть тести
python test_api_manually.py
```

## 🔧 Що тестується

### ✅ Основні endpoints:
1. **Health Check** - `/health`
2. **Hexagon Details** - `/api/v1/hexagon-details/details/{h3_index}`
3. **Analysis Preview** - `/api/v1/hexagon-details/analysis-preview/{h3_index}`
4. **Coverage Calculator** - `/api/v1/hexagon-details/coverage-calculator`
5. **POI in Hexagon** - `/api/v1/hexagon-details/poi-in-hexagon/{h3_index}`
6. **Competitive Analysis** - `/api/v1/hexagon-details/competitive-analysis/{h3_index}`

### ✅ Різні типи аналізів:
- 🚶 **pedestrian_competition** - пішохідна конкуренція
- 🚇 **transport_accessibility** - транспортна доступність  
- 📊 **market_overview** - огляд ринку
- 🏪 **site_selection** - вибір локації
- ⚙️ **custom** - користувацький

### ✅ H3 resolutions:
- **H3-7** - огляд області
- **H3-8** - рівень району
- **H3-9** - рівень кварталу
- **H3-10** - рівень вулиці

### ✅ Error handling:
- Неправильний H3 індекс
- Неправильний resolution
- Неправильний тип аналізу
- Валідація custom_rings

## 📊 Очікувані результати

### 🎯 Успішний тест покаже:
```
✅ SUCCESS: Статус 200 (ожидался 200)
📦 JSON размер: 2547 символов
🔑 Ключи ответа: ['location_info', 'metrics', 'poi_details', ...]
📍 Локация: H3-10 8a1fb46622d7fff
📐 Координаты: 50.450123, 30.523456
📏 Площадь: 0.015 км²
🔗 Покрытие: 3 колец, 37 гексагонов
📊 Общая площадь: 0.555 км²
🏪 POI найдено: 15
```

### ⚠️ Очікувані помилки:
- Якщо база даних не підключена - endpoints повернуть 500
- Якщо таблиці не існують - endpoints повернуть 404
- Це нормально на етапі розробки!

## 🔍 Діагностика проблем

### ❌ Сервер не запускається:
```bash
# Перевірте Python та залежності
python --version  # Потрібен Python 3.8+
pip install fastapi uvicorn psycopg2-binary h3
```

### ❌ Endpoints повертають 500:
```bash
# Перевірте підключення до бази даних
# В поточній версії це очікувано, так як БД ще не налаштована
```

### ❌ H3 індекси не знайдені:
```bash
# Використовуються тестові H3 індекси для Києва
# Вони можуть не існувати в вашій БД - це нормально
```

## 🌐 Swagger UI

Коли сервер запущений, відкрийте:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

Там ви зможете:
- 📋 Подивитися всі доступні endpoints
- 🧪 Протестувати API через web interface
- 📚 Подивитися документацію кожного endpoint
- 🔧 Експериментувати з параметрами

## 🎯 Наступні кроки після тестування

### Якщо тести пройшли:
1. ✅ Backend API готовий
2. 🚀 Можна інтегрувати з frontend
3. 💾 Налаштувати реальну базу даних
4. 🏪 Додати реальні POI дані

### Якщо тести провалилися:
1. 🔧 Перевірити залежності
2. 📊 Налаштувати базу даних
3. 🗄️ Створити потрібні таблиці
4. 📥 Імпортувати тестові дані

## 💡 Корисні команди

```bash
# Запуск сервера з логами
uvicorn src.main:app --reload --log-level debug

# Перевірка простого endpoint
curl http://localhost:8000/health

# Тест конкретного H3 endpoint
curl "http://localhost:8000/api/v1/hexagon-details/details/8a1fb46622d7fff?resolution=10"

# Перевірка coverage calculator
curl "http://localhost:8000/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=2"
```

---

**🎉 Успішного тестування!** 

Якщо у вас виникли питання, перевірте логи сервера або зверніться за допомогою.
