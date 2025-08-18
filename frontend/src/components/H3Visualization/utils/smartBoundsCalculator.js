// frontend/src/components/H3Visualization/utils/smartBoundsCalculator.js

/**
 * 🎯 Smart Bounds Calculator - Viewport-Aware Bounds Utility
 * 
 * Розумне обчислення меж для завантаження H3 даних з урахуванням:
 * - Поточного viewport користувача
 * - Zoom рівня для оптимального розміру області
 * - Стратегії завантаження (conservative/balanced/comprehensive)
 * - Performance оптимізації для різних tier'ів
 */

// ===============================================
// CORE BOUNDS CALCULATION CLASS
// ===============================================

class SmartBoundsCalculator {
  
  /**
   * Розрахунок bounds для viewport з різними стратегіями
   * @param {Object} viewport - {longitude, latitude, zoom}
   * @param {string} strategy - 'conservative' | 'balanced' | 'comprehensive'
   * @returns {Object} bounds object з detailed breakdown
   */
  static calculate(viewport, strategy = 'balanced') {
    const { longitude, latitude, zoom } = viewport;
    
    // Валідація параметрів
    if (!this.validateViewport(viewport)) {
      throw new Error('Invalid viewport parameters');
    }
    
    // Базові параметри для кожної стратегії
    const strategies = {
      'conservative': {
        description: 'Minimal area for instant loading (Tier 1)',
        multiplier: 0.8,
        maxArea: 2, // км²
        bufferFactor: 0.5,
        targetLoadTime: '< 2 seconds'
      },
      
      'balanced': {
        description: 'Balanced area for full functionality (Tier 2)', 
        multiplier: 1.0,
        maxArea: 8, // км²
        bufferFactor: 0.75,
        targetLoadTime: '2-8 seconds'
      },
      
      'comprehensive': {
        description: 'Extended area for complete dataset (Tier 3)',
        multiplier: 1.5,
        maxArea: 25, // км²
        bufferFactor: 1.0,
        targetLoadTime: '8-20 seconds'
      }
    };
    
    const config = strategies[strategy];
    if (!config) {
      throw new Error(`Unknown strategy: ${strategy}`);
    }
    
    // Базовий розрахунок span з урахуванням zoom
    const baseSpan = this.calculateBaseSpan(zoom) * config.multiplier;
    
    // Обмеження максимальної площі
    const constrainedSpan = this.constrainByMaxArea(baseSpan, config.maxArea);
    
    // Коригування для широти (проекція Меркатора)
    const latSpan = constrainedSpan * config.bufferFactor;
    const lonSpan = (constrainedSpan * config.bufferFactor) * Math.cos(latitude * Math.PI / 180);
    
    // Розрахунок bounds
    const bounds = {
      north: latitude + latSpan,
      south: latitude - latSpan,
      east: longitude + lonSpan,
      west: longitude - lonSpan
    };
    
    // Детальна аналітика
    const analytics = this.calculateAnalytics(bounds, config, zoom);
    
    console.log(`📐 Smart Bounds [${strategy}]: ${analytics.estimatedArea}km² @ zoom ${zoom}`);
    
    return {
      ...bounds,
      strategy,
      config,
      analytics,
      meta: {
        viewport,
        calculatedAt: new Date().toISOString(),
        isOptimized: analytics.estimatedArea <= config.maxArea
      }
    };
  }
  
  /**
   * Швидкий розрахунок для простих випадків
   */
  static quick(latitude, longitude, zoom, radiusKm = 1) {
    const viewport = { latitude, longitude, zoom };
    
    // Автоматичний вибір стратегії на основі radiusKm
    let strategy;
    if (radiusKm <= 1) strategy = 'conservative';
    else if (radiusKm <= 3) strategy = 'balanced';
    else strategy = 'comprehensive';
    
    return this.calculate(viewport, strategy);
  }
  
  /**
   * Валідація viewport параметрів
   */
  static validateViewport(viewport) {
    if (!viewport || typeof viewport !== 'object') return false;
    
    const { longitude, latitude, zoom } = viewport;
    
    // Перевірка longitude
    if (typeof longitude !== 'number' || longitude < -180 || longitude > 180) {
      return false;
    }
    
    // Перевірка latitude
    if (typeof latitude !== 'number' || latitude < -90 || latitude > 90) {
      return false;
    }
    
    // Перевірка zoom
    if (typeof zoom !== 'number' || zoom < 0 || zoom > 20) {
      return false;
    }
    
    return true;
  }
  
  /**
   * Розрахунок базового span на основі zoom
   */
  static calculateBaseSpan(zoom) {
    // Формула на основі емпіричних даних для зручного viewport
    // Чим більший zoom, тим менша область потрібна
    const zoomFactor = Math.max(0, 12 - zoom);
    return 0.01 * Math.pow(1.8, zoomFactor);
  }
  
  /**
   * Обмеження span по максимальній площі
   */
  static constrainByMaxArea(span, maxAreaKm2) {
    // Приблизна площа в км² для span в градусах
    const estimatedAreaKm2 = this.spanToAreaKm2(span);
    
    if (estimatedAreaKm2 <= maxAreaKm2) {
      return span; // В межах ліміту
    }
    
    // Коригуємо span щоб не перевищити maxArea
    const scaleFactor = Math.sqrt(maxAreaKm2 / estimatedAreaKm2);
    return span * scaleFactor;
  }
  
  /**
   * Конвертація span в градусах в площу в км²
   */
  static spanToAreaKm2(span, latitude = 50.45) {
    // Приблизний розрахунок для широти Києва
    const kmPerDegree = 111; // км на градус
    const latitudeCorrection = Math.cos(latitude * Math.PI / 180);
    
    const widthKm = span * 2 * kmPerDegree * latitudeCorrection;
    const heightKm = span * 2 * kmPerDegree;
    
    return widthKm * heightKm;
  }
  
  /**
   * Детальна аналітика bounds
   */
  static calculateAnalytics(bounds, config, zoom) {
    const { north, south, east, west } = bounds;
    
    // Розміри в градусах
    const latSpan = north - south;
    const lonSpan = east - west;
    
    // Центр області
    const centerLat = (north + south) / 2;
    const centerLon = (east + west) / 2;
    
    // Площа в км²
    const estimatedArea = this.spanToAreaKm2(Math.max(latSpan, lonSpan) / 2, centerLat);
    
    // Приблизна кількість H3 гексагонів для різних resolutions
    const hexagonCounts = this.estimateHexagonCounts(estimatedArea, zoom);
    
    // Оцінка часу завантаження
    const estimatedLoadTime = this.estimateLoadTime(hexagonCounts.total, config);
    
    return {
      dimensions: {
        latSpan: parseFloat(latSpan.toFixed(6)),
        lonSpan: parseFloat(lonSpan.toFixed(6)),
        aspectRatio: parseFloat((lonSpan / latSpan).toFixed(2))
      },
      area: {
        estimatedArea: parseFloat(estimatedArea.toFixed(3)),
        maxAllowedArea: config.maxArea,
        utilizationPercent: Math.round((estimatedArea / config.maxArea) * 100)
      },
      performance: {
        hexagonCounts,
        estimatedLoadTime,
        networkRequests: this.estimateNetworkRequests(estimatedArea),
        memoryUsageKB: Math.round(hexagonCounts.total * 0.5)
      },
      center: {
        latitude: parseFloat(centerLat.toFixed(6)),
        longitude: parseFloat(centerLon.toFixed(6))
      }
    };
  }
  
  /**
   * Оцінка кількості гексагонів по resolutions
   */
  static estimateHexagonCounts(areaKm2, zoom) {
    // Базові площі гексагонів H3 (км²)
    const hexagonAreas = {
      7: 5.16,    // ~5.16 км² на гексагон
      8: 0.737,   // ~0.737 км² на гексагон
      9: 0.105,   // ~0.105 км² на гексагон
      10: 0.015   // ~0.015 км² на гексагон
    };
    
    const counts = {};
    let total = 0;
    
    for (const [resolution, hexArea] of Object.entries(hexagonAreas)) {
      const count = Math.ceil(areaKm2 / hexArea);
      counts[`h3_${resolution}`] = count;
      total += count;
    }
    
    // Рекомендований resolution на основі zoom
    const recommendedResolution = this.getOptimalResolution(zoom);
    counts.recommended = counts[`h3_${recommendedResolution}`];
    counts.total = total;
    counts.optimalResolution = recommendedResolution;
    
    return counts;
  }
  
  /**
   * Оптимальний H3 resolution для zoom рівня
   */
  static getOptimalResolution(zoom) {
    if (zoom < 8) return 7;
    if (zoom < 10) return 8;
    if (zoom < 12) return 9;
    return 10;
  }
  
  /**
   * Оцінка часу завантаження
   */
  static estimateLoadTime(hexagonCount, config) {
    // Базова модель: ~100 гексагонів/секунда для API calls
    const baseRate = 1000; // гексагонів/секунда
    const networkLatency = 200; // мс базової затримки
    
    const processingTime = (hexagonCount / baseRate) * 1000; // мс
    const totalTime = Math.max(processingTime + networkLatency, 500); // мінімум 500мс
    
    return {
      estimatedMs: Math.round(totalTime),
      category: this.categorizeLoadTime(totalTime),
      withinTarget: totalTime <= this.parseTargetTime(config.targetLoadTime)
    };
  }
  
  /**
   * Категоризація часу завантаження
   */
  static categorizeLoadTime(timeMs) {
    if (timeMs < 2000) return 'instant';
    if (timeMs < 5000) return 'fast';
    if (timeMs < 10000) return 'moderate';
    return 'slow';
  }
  
  /**
   * Парсинг target time з конфігурації
   */
  static parseTargetTime(targetTimeStr) {
    // Парсимо "< 2 seconds" -> 2000ms
    const match = targetTimeStr.match(/(\d+)/);
    return match ? parseInt(match[1]) * 1000 : 5000;
  }
  
  /**
   * Оцінка кількості мережевих запитів
   */
  static estimateNetworkRequests(areaKm2) {
    // Базується на розмірі області та pagination limits
    const avgRecordsPerKm2 = 500; // середня щільність POI
    const totalRecords = areaKm2 * avgRecordsPerKm2;
    const recordsPerRequest = 1000; // API limit
    
    return Math.ceil(totalRecords / recordsPerRequest);
  }
}

// ===============================================
// SPECIALIZED CALCULATORS
// ===============================================

/**
 * Calculator для Tier 1 - максимальна швидкість
 */
class Tier1BoundsCalculator extends SmartBoundsCalculator {
  static calculate(viewport) {
    const result = super.calculate(viewport, 'conservative');
    
    // Додаткові оптимізації для Tier 1
    const optimized = {
      ...result,
      optimizations: {
        maxHexagons: 1000,
        priorityMetric: 'opportunity',
        priorityResolution: 8,
        cacheStrategy: 'aggressive',
        networkTimeout: 2000
      }
    };
    
    console.log(`🚀 Tier 1 Bounds: ${optimized.analytics.performance.hexagonCounts.recommended} hexagons`);
    return optimized;
  }
}

/**
 * Calculator для Tier 2 - збалансована функціональність
 */
class Tier2BoundsCalculator extends SmartBoundsCalculator {
  static calculate(viewport) {
    const result = super.calculate(viewport, 'balanced');
    
    // Додаткові параметри для Tier 2
    const enhanced = {
      ...result,
      enhancements: {
        includeNeighbors: true,
        multipleResolutions: [7, 8, 9, 10],
        multipleMetrics: ['opportunity', 'competition'],
        parallelRequests: 3,
        networkTimeout: 8000
      }
    };
    
    console.log(`⚡ Tier 2 Bounds: ${enhanced.analytics.area.estimatedArea}km² coverage`);
    return enhanced;
  }
}

/**
 * Calculator для Tier 3 - повне покриття
 */
class Tier3BoundsCalculator extends SmartBoundsCalculator {
  static calculate(viewport) {
    const result = super.calculate(viewport, 'comprehensive');
    
    // Максимальне покриття для Tier 3
    const comprehensive = {
      ...result,
      comprehensive: {
        fullDataset: true,
        allResolutions: [7, 8, 9, 10],
        allMetrics: ['opportunity', 'competition'],
        extendedRadius: true,
        offlineCapable: true,
        backgroundLoading: true
      }
    };
    
    console.log(`📚 Tier 3 Bounds: Full dataset with ${comprehensive.analytics.performance.networkRequests} requests`);
    return comprehensive;
  }
}

// ===============================================
// VIEWPORT UTILITIES
// ===============================================

/**
 * Utility функції для роботи з viewport
 */
class ViewportUtils {
  
  /**
   * Перевірка чи viewport змінився значно
   */
  static hasSignificantChange(oldViewport, newViewport, threshold = 0.1) {
    if (!oldViewport || !newViewport) return true;
    
    const latDiff = Math.abs(oldViewport.latitude - newViewport.latitude);
    const lonDiff = Math.abs(oldViewport.longitude - newViewport.longitude);
    const zoomDiff = Math.abs(oldViewport.zoom - newViewport.zoom);
    
    return latDiff > threshold || lonDiff > threshold || zoomDiff > 1;
  }
  
  /**
   * Розрахунок відстані між двома viewport'ами
   */
  static calculateDistance(viewport1, viewport2) {
    const lat1 = viewport1.latitude * Math.PI / 180;
    const lat2 = viewport2.latitude * Math.PI / 180;
    const deltaLat = (viewport2.latitude - viewport1.latitude) * Math.PI / 180;
    const deltaLon = (viewport2.longitude - viewport1.longitude) * Math.PI / 180;
    
    const a = Math.sin(deltaLat/2) * Math.sin(deltaLat/2) +
             Math.cos(lat1) * Math.cos(lat2) *
             Math.sin(deltaLon/2) * Math.sin(deltaLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    
    return 6371 * c; // відстань в кілометрах
  }
  
  /**
   * Перевірка чи viewport у межах bounds
   */
  static isViewportInBounds(viewport, bounds, bufferPercent = 10) {
    const { latitude, longitude } = viewport;
    const { north, south, east, west } = bounds;
    
    // Додаємо buffer
    const latBuffer = (north - south) * (bufferPercent / 100);
    const lonBuffer = (east - west) * (bufferPercent / 100);
    
    return latitude >= (south - latBuffer) &&
           latitude <= (north + latBuffer) &&
           longitude >= (west - lonBuffer) &&
           longitude <= (east + lonBuffer);
  }
  
  /**
   * Оптимальний viewport для заданої області
   */
  static getOptimalViewport(bounds) {
    const centerLat = (bounds.north + bounds.south) / 2;
    const centerLon = (bounds.east + bounds.west) / 2;
    
    // Розрахунок оптимального zoom
    const latSpan = bounds.north - bounds.south;
    const lonSpan = bounds.east - bounds.west;
    const maxSpan = Math.max(latSpan, lonSpan);
    
    // Емпірична формула для zoom
    const zoom = Math.max(6, Math.min(15, 12 - Math.log2(maxSpan * 100)));
    
    return {
      latitude: centerLat,
      longitude: centerLon,
      zoom: Math.round(zoom),
      pitch: 0,
      bearing: 0
    };
  }
}

// ===============================================
// PERFORMANCE MONITORING
// ===============================================

/**
 * Моніторинг performance bounds calculations
 */
class BoundsPerformanceMonitor {
  constructor() {
    this.calculations = [];
    this.maxHistory = 100;
  }
  
  /**
   * Запис calculation з метриками
   */
  recordCalculation(strategy, viewport, result, executionTime) {
    const record = {
      timestamp: Date.now(),
      strategy,
      viewport: { ...viewport },
      result: {
        area: result.analytics.area.estimatedArea,
        hexagons: result.analytics.performance.hexagonCounts.recommended,
        requests: result.analytics.performance.networkRequests
      },
      performance: {
        executionTime,
        memoryUsage: result.analytics.performance.memoryUsageKB
      }
    };
    
    this.calculations.push(record);
    
    // Обмежуємо історію
    if (this.calculations.length > this.maxHistory) {
      this.calculations.shift();
    }
    
    console.log(`📊 Bounds calculation [${strategy}]: ${executionTime}ms`);
  }
  
  /**
   * Статистика performance
   */
  getPerformanceStats() {
    if (this.calculations.length === 0) return null;
    
    const executionTimes = this.calculations.map(c => c.performance.executionTime);
    const areas = this.calculations.map(c => c.result.area);
    
    return {
      totalCalculations: this.calculations.length,
      averageExecutionTime: executionTimes.reduce((a, b) => a + b, 0) / executionTimes.length,
      maxExecutionTime: Math.max(...executionTimes),
      minExecutionTime: Math.min(...executionTimes),
      averageArea: areas.reduce((a, b) => a + b, 0) / areas.length,
      strategyCounts: this.calculations.reduce((acc, calc) => {
        acc[calc.strategy] = (acc[calc.strategy] || 0) + 1;
        return acc;
      }, {})
    };
  }
  
  /**
   * Очищення історії
   */
  clearHistory() {
    this.calculations = [];
    console.log('📊 Bounds calculation history cleared');
  }
}

// ===============================================
// CACHING SYSTEM
// ===============================================

/**
 * Cache для bounds calculations
 */
class BoundsCache {
  constructor(maxSize = 50) {
    this.cache = new Map();
    this.maxSize = maxSize;
    this.accessTimes = new Map();
  }
  
  /**
   * Генерація cache key
   */
  generateKey(viewport, strategy) {
    const { latitude, longitude, zoom } = viewport;
    return `${strategy}_${latitude.toFixed(4)}_${longitude.toFixed(4)}_${zoom}`;
  }
  
  /**
   * Отримання з cache
   */
  get(viewport, strategy) {
    const key = this.generateKey(viewport, strategy);
    
    if (this.cache.has(key)) {
      this.accessTimes.set(key, Date.now());
      console.log(`📦 Bounds cache hit: ${key}`);
      return this.cache.get(key);
    }
    
    console.log(`📦 Bounds cache miss: ${key}`);
    return null;
  }
  
  /**
   * Збереження в cache
   */
  set(viewport, strategy, result) {
    const key = this.generateKey(viewport, strategy);
    
    // Очищуємо старі записи якщо досягли ліміту
    if (this.cache.size >= this.maxSize) {
      this.evictOldest();
    }
    
    this.cache.set(key, result);
    this.accessTimes.set(key, Date.now());
    
    console.log(`📦 Bounds cached: ${key} (${this.cache.size}/${this.maxSize})`);
  }
  
  /**
   * Видалення найстарішого запису
   */
  evictOldest() {
    let oldestKey = null;
    let oldestTime = Date.now();
    
    for (const [key, time] of this.accessTimes.entries()) {
      if (time < oldestTime) {
        oldestTime = time;
        oldestKey = key;
      }
    }
    
    if (oldestKey) {
      this.cache.delete(oldestKey);
      this.accessTimes.delete(oldestKey);
      console.log(`📦 Evicted oldest bounds cache entry: ${oldestKey}`);
    }
  }
  
  /**
   * Очищення cache
   */
  clear() {
    this.cache.clear();
    this.accessTimes.clear();
    console.log('📦 Bounds cache cleared');
  }
  
  /**
   * Статистика cache
   */
  getStats() {
    return {
      size: this.cache.size,
      maxSize: this.maxSize,
      keys: Array.from(this.cache.keys()),
      hitRate: this.calculateHitRate()
    };
  }
  
  /**
   * Розрахунок hit rate (наближено)
   */
  calculateHitRate() {
    // Simplified calculation - в production можна зробити точніше
    return this.cache.size > 0 ? Math.min(this.cache.size / this.maxSize, 1) : 0;
  }
}

// ===============================================
// SINGLETON INSTANCES
// ===============================================

// Глобальні інстанси для використання в додатку
const performanceMonitor = new BoundsPerformanceMonitor();
const boundsCache = new BoundsCache();

// ===============================================
// MAIN EXPORT INTERFACE
// ===============================================

export {
  SmartBoundsCalculator,
  Tier1BoundsCalculator,
  Tier2BoundsCalculator, 
  Tier3BoundsCalculator,
  ViewportUtils,
  BoundsPerformanceMonitor,
  BoundsCache,
  performanceMonitor,
  boundsCache
};

export default SmartBoundsCalculator;