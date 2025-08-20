// frontend/src/components/H3Visualization/utils/smartBoundsCalculator.js
// Розумне обчислення меж для завантаження H3 даних з різними стратегіями

/**
 * SmartBoundsCalculator - Utility для розрахунку оптимальних bounds
 * для різних H3 resolutions та loading strategies
 */
class SmartBoundsCalculator {
  
  /**
   * Головна функція для розрахунку bounds залежно від стратегії
   * @param {Object} viewport - Поточний viewport {longitude, latitude, zoom}
   * @param {string} strategy - Стратегія: 'conservative', 'balanced', 'comprehensive'
   * @returns {Object} Bounds з метаданими
   */
  static calculate(viewport, strategy = 'balanced') {
    const { longitude, latitude, zoom } = viewport;
    
    console.log(`🎯 Calculating bounds for strategy: ${strategy}, zoom: ${zoom}`);
    
    // Стратегії розрахунку залежно від потреб
    const strategies = {
      'conservative': {
        // Мінімальна область для Tier 1 (швидкий старт)
        description: 'Minimal area for instant loading',
        multiplier: 0.5,
        maxHexagons: 1000,
        targetLoadTime: 2000, // 2 секунди
        priority: 'speed'
      },
      
      'balanced': {
        // Збалансована область для Tier 2 (оптимальний баланс)
        description: 'Balanced area for full functionality', 
        multiplier: 1.0,
        maxHexagons: 5000,
        targetLoadTime: 8000, // 8 секунд
        priority: 'balance'
      },
      
      'comprehensive': {
        // Розширена область для Tier 3 (максимальне покриття)
        description: 'Extended area for offline capability',
        multiplier: 2.0,
        maxHexagons: 20000,
        targetLoadTime: 20000, // 20 секунд
        priority: 'coverage'
      }
    };
    
    const config = strategies[strategy];
    if (!config) {
      console.warn(`⚠️ Unknown strategy: ${strategy}, falling back to 'balanced'`);
      return this.calculate(viewport, 'balanced');
    }
    
    // Базовий розрахунок bounds
    const baseBounds = this.calculateBaseBounds(latitude, longitude, zoom, config.multiplier);
    
    // Додаткові bounds для different use cases
    const result = {
      // Основна область
      current: baseBounds,
      
      // Розширена область (для preloading)
      extended: zoom > 8 ? 
        this.calculateBaseBounds(latitude, longitude, zoom, config.multiplier * 1.5) : 
        null,
      
      // Buffer область (для smooth panning)
      buffer: zoom > 6 ? 
        this.calculateBaseBounds(latitude, longitude, zoom, config.multiplier * 2.0) : 
        null,
      
      // Metadata
      metadata: {
        strategy,
        description: config.description,
        priority: config.priority,
        zoom,
        estimatedHexagons: this.estimateHexagonCount(baseBounds, zoom),
        maxHexagons: config.maxHexagons,
        targetLoadTime: config.targetLoadTime,
        optimalResolution: this.getOptimalResolution(zoom)
      }
    };
    
    console.log(`📦 Bounds calculated:`, {
      strategy,
      zoom,
      estimatedHexagons: result.metadata.estimatedHexagons,
      area: this.calculateAreaKm2(baseBounds)
    });
    
    return result;
  }
  
  /**
   * Базовий розрахунок bounds для заданих координат та multiplier
   */
  static calculateBaseBounds(lat, lon, zoom, multiplier) {
    // Zoom-aware span calculation
    const baseSpan = 360 / Math.pow(2, zoom);
    const adjustedSpan = baseSpan * multiplier;
    
    // Latitude correction (не змінюється з longitude)
    const latSpan = adjustedSpan;
    
    // Longitude correction (враховує кривизну землі)
    const lonSpan = adjustedSpan * Math.cos(lat * Math.PI / 180);
    
    return {
      north: lat + latSpan,
      south: lat - latSpan,
      east: lon + lonSpan,
      west: lon - lonSpan,
      
      // Utility methods
      center: { lat, lon },
      span: { lat: latSpan * 2, lon: lonSpan * 2 }
    };
  }
  
  /**
   * Адаптивні bounds на основі цільової кількості гексагонів
   */
  static adaptiveBounds(viewport, targetHexagonCount = 1000) {
    const { zoom, latitude, longitude } = viewport;
    const resolution = this.getOptimalResolution(zoom);
    
    // Оцінка щільності гексагонів на км² для різних resolutions
    const densityEstimates = {
      7: 0.12,   // ~0.12 гексагонів на км²
      8: 0.85,   // ~0.85 гексагонів на км²
      9: 5.95,   // ~5.95 гексагонів на км²
      10: 41.65  // ~41.65 гексагонів на км²
    };
    
    const density = densityEstimates[resolution] || 1.0;
    
    // Розрахунок необхідної площі
    const requiredAreaKm2 = targetHexagonCount / density;
    const spanKm = Math.sqrt(requiredAreaKm2);
    
    console.log(`🎯 Adaptive bounds calculation:`, {
      targetHexagons: targetHexagonCount,
      resolution,
      density: density.toFixed(2),
      requiredAreaKm2: requiredAreaKm2.toFixed(2),
      spanKm: spanKm.toFixed(2)
    });
    
    // Конвертація в lat/lon degrees
    const latSpan = spanKm / 111; // ~111 км на градус широти
    const lonSpan = spanKm / (111 * Math.cos(latitude * Math.PI / 180));
    
    return {
      north: latitude + latSpan,
      south: latitude - latSpan,
      east: longitude + lonSpan,
      west: longitude - lonSpan,
      
      metadata: {
        type: 'adaptive',
        targetHexagonCount,
        resolution,
        density,
        areaKm2: requiredAreaKm2,
        estimatedHexagons: targetHexagonCount
      }
    };
  }
  
  /**
   * Оптимальний H3 resolution на основі zoom level
   */
  static getOptimalResolution(zoom) {
    // Mapping zoom levels to H3 resolutions
    if (zoom < 7) return 7;   // Oblast level
    if (zoom < 9) return 8;   // District level  
    if (zoom < 11) return 9;  // City level
    if (zoom < 13) return 10; // Neighborhood level
    return 10; // Maximum detail
  }
  
  /**
   * Оцінка кількості гексагонів у bounds
   */
  static estimateHexagonCount(bounds, zoom) {
    const resolution = this.getOptimalResolution(zoom);
    const areaKm2 = this.calculateAreaKm2(bounds);
    
    // Density estimates для різних resolutions
    const densityEstimates = {
      7: 0.12, 8: 0.85, 9: 5.95, 10: 41.65
    };
    
    const density = densityEstimates[resolution] || 1.0;
    return Math.round(areaKm2 * density);
  }
  
  /**
   * Розрахунок площі bounds у км²
   */
  static calculateAreaKm2(bounds) {
    // Приблизний розрахунок площі прямокутника на сфері
    const latDiff = bounds.north - bounds.south;
    const lonDiff = bounds.east - bounds.west;
    
    // Середня широта для корекції
    const avgLat = (bounds.north + bounds.south) / 2;
    
    // Конвертація у км (приблизно)
    const latKm = latDiff * 111; // 111 км на градус широти
    const lonKm = lonDiff * 111 * Math.cos(avgLat * Math.PI / 180);
    
    return Math.abs(latKm * lonKm);
  }
  
  /**
   * Форматування bounds для API запитів
   */
  static formatForAPI(bounds) {
    // Стандартний формат: south,west,north,east
    return `${bounds.south.toFixed(6)},${bounds.west.toFixed(6)},${bounds.north.toFixed(6)},${bounds.east.toFixed(6)}`;
  }
  
  /**
   * Перевірка чи знаходиться точка в bounds
   */
  static containsPoint(bounds, lat, lon) {
    return lat >= bounds.south && 
           lat <= bounds.north && 
           lon >= bounds.west && 
           lon <= bounds.east;
  }
  
  /**
   * Об'єднання декількох bounds у один
   */
  static mergeBounds(boundsArray) {
    if (!boundsArray || boundsArray.length === 0) return null;
    if (boundsArray.length === 1) return boundsArray[0];
    
    const merged = {
      north: Math.max(...boundsArray.map(b => b.north)),
      south: Math.min(...boundsArray.map(b => b.south)),
      east: Math.max(...boundsArray.map(b => b.east)),
      west: Math.min(...boundsArray.map(b => b.west))
    };
    
    return merged;
  }
  
  /**
   * Розрахунок bounds для конкретного H3 resolution
   */
  static getBoundsForResolution(viewport, resolution, targetCount = 1000) {
    const strategy = this.getStrategyForResolution(resolution);
    const bounds = this.calculate(viewport, strategy);
    
    // Додаткова оптимізація для конкретного resolution
    const estimated = this.estimateHexagonCount(bounds.current, viewport.zoom);
    
    if (estimated > targetCount * 2) {
      // Занадто багато гексагонів - зменшуємо область
      return this.calculate(viewport, 'conservative');
    } else if (estimated < targetCount * 0.5) {
      // Замало гексагонів - збільшуємо область
      return this.calculate(viewport, 'comprehensive');
    }
    
    return bounds;
  }
  
  /**
   * Вибір стратегії на основі H3 resolution
   */
  static getStrategyForResolution(resolution) {
    switch(resolution) {
      case 7: return 'comprehensive'; // Великі гексагони - можна більше область
      case 8: return 'balanced';      // Середні гексагони - збалансовано
      case 9: return 'balanced';      // Малі гексагони - збалансовано
      case 10: return 'conservative'; // Дуже малі - обмежена область
      default: return 'balanced';
    }
  }
  
  /**
   * Debug інформація про bounds
   */
  static getDebugInfo(bounds, viewport) {
    return {
      bounds,
      viewport,
      areaKm2: this.calculateAreaKm2(bounds),
      estimatedHexagons: this.estimateHexagonCount(bounds, viewport.zoom),
      optimalResolution: this.getOptimalResolution(viewport.zoom),
      apiFormat: this.formatForAPI(bounds)
    };
  }
}

// Експорт для ES6 modules
export { SmartBoundsCalculator };
export default SmartBoundsCalculator;