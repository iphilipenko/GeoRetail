// frontend/src/components/H3Visualization/hooks/useSmartH3Loading.js
// FIXED VERSION - Всі критичні проблеми вирішено

import { useState, useEffect, useCallback, useRef } from 'react';

/**
 * Smart H3 Loading Hook - ВИПРАВЛЕНА ВЕРСІЯ
 * 
 * Основні виправлення:
 * 1. Map() замінено на звичайний об'єкт для React state
 * 2. Виправлено всі useCallback dependencies
 * 3. Виправлено race conditions
 * 4. Додано валідацію геометрії
 * 5. Покращено обробку помилок
 */

const API_BASE = 'http://localhost:8000/api/v1/visualization/kyiv-h3';
const TIMEOUT_MS = 30000;

const TIER_1_DATASETS = [
  { metric: 'opportunity', resolution: 7 },
  { metric: 'opportunity', resolution: 8 }
];

const BACKGROUND_DATASETS = [
  { metric: 'opportunity', resolution: 9 },
  { metric: 'competition', resolution: 7 },
  { metric: 'competition', resolution: 8 },
  { metric: 'competition', resolution: 9 },
  { metric: 'opportunity', resolution: 10 },
  { metric: 'competition', resolution: 10 }
];

// Валідація геометрії H3 гексагону
const validateH3Geometry = (geometry) => {
  if (!geometry || geometry.type !== 'Polygon') {
    return false;
  }
  
  if (!geometry.coordinates || !Array.isArray(geometry.coordinates)) {
    return false;
  }
  
  const ring = geometry.coordinates[0];
  if (!Array.isArray(ring) || ring.length < 4) {
    return false;
  }
  
  // Перевіряємо що всі координати валідні
  return ring.every(coord => 
    Array.isArray(coord) && 
    coord.length === 2 && 
    typeof coord[0] === 'number' && 
    typeof coord[1] === 'number' &&
    !isNaN(coord[0]) && 
    !isNaN(coord[1])
  );
};

// Нормалізація даних гексагону
const normalizeHexagonData = (hex, metric, resolution) => {
  // Валідуємо геометрію
  if (!validateH3Geometry(hex.geometry)) {
    console.warn(`⚠️ Invalid geometry for hexagon:`, hex);
    return null;
  }
  
  // Витягуємо значення метрики
  let metricValue = 0;
  if (metric === 'opportunity') {
    metricValue = hex.market_opportunity_score || hex.display_value || 0;
  } else if (metric === 'competition') {
    metricValue = hex.competition_intensity || hex.display_value || 0;
  } else {
    metricValue = hex.display_value || hex.market_opportunity_score || hex.competition_intensity || 0;
  }
  
  // Переконуємося що значення числове
  if (typeof metricValue === 'string') {
    metricValue = parseFloat(metricValue) || 0;
  }
  
  return {
    ...hex,
    // Стандартизовані поля для deck.gl
    market_opportunity_score: hex.market_opportunity_score || metricValue,
    competition_intensity: hex.competition_intensity || metricValue,
    display_value: metricValue,
    
    // Валідна геометрія
    geometry: hex.geometry,
    
    // Метадані
    properties: {
      ...hex.properties,
      metricValue,
      metric,
      resolution,
      h3_index: hex.h3_index || hex.id
    }
  };
};

const useSmartH3Loading = (viewport = {}, initialMetric = 'opportunity') => {
  // ВИПРАВЛЕНО: Використовуємо звичайний об'єкт замість Map
  const [dataCache, setDataCache] = useState({});
  
  // Основний стан
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [progress, setProgress] = useState(0);
  const [status, setStatus] = useState('Ініціалізація...');
  const [tier1Complete, setTier1Complete] = useState(false);
  
  // ВИПРАВЛЕНО: Використовуємо useRef для відстеження активних завантажень
  const activeLoadsRef = useRef(new Set());
  const loadingInProgressRef = useRef(false);
  
  // Функція завантаження даних з API
  const fetchData = useCallback(async (metric, resolution) => {
    const url = `${API_BASE}?metric_type=${metric}&resolution=${resolution}&limit=1000000`;
    
    console.log(`📡 Fetching: ${url}`);
    const startTime = Date.now();
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
      
      const response = await fetch(url, {
        signal: controller.signal
      });
      
      clearTimeout(timeoutId);
      const fetchTime = Date.now() - startTime;
      
      console.log(`⏱️ Fetch completed in ${fetchTime}ms`);
      
      if (!response.ok) {
        const errorText = await response.text().catch(() => 'Unknown error');
        throw new Error(`HTTP ${response.status}: ${errorText}`);
      }
      
      const data = await response.json();
      const totalTime = Date.now() - startTime;
      
      console.log(`✅ Data parsed in ${totalTime}ms total`);
      
      // Валідуємо структуру даних
      if (!data?.hexagons || !Array.isArray(data.hexagons)) {
        throw new Error('Invalid response: missing hexagons array');
      }
      
      // Валідуємо що є хоча б декілька валідних гексагонів
      const validHexagons = data.hexagons.filter(hex => validateH3Geometry(hex.geometry));
      
      if (validHexagons.length === 0) {
        throw new Error('No valid hexagons found in response');
      }
      
      if (validHexagons.length < data.hexagons.length) {
        console.warn(`⚠️ ${data.hexagons.length - validHexagons.length} invalid hexagons filtered out`);
      }
      
      console.log(`🎯 Success: ${validHexagons.length} valid hexagons received`);
      
      return {
        ...data,
        hexagons: validHexagons
      };
      
    } catch (err) {
      const totalTime = Date.now() - startTime;
      console.error(`❌ Fetch failed after ${totalTime}ms:`, err.message);
      throw err;
    }
  }, []);
  
  // ВИПРАВЛЕНО: Завантаження окремого датасету з правильною логікою
  const loadDataset = useCallback(async (metric, resolution) => {
    const key = `${metric}-${resolution}`;
    
    // Перевіряємо чи вже завантажено
    if (dataCache[key]) {
      console.log(`📦 Cache HIT: ${key} already loaded`);
      return { success: true, data: dataCache[key] };
    }
    
    // Перевіряємо чи зараз завантажується
    if (activeLoadsRef.current.has(key)) {
      console.log(`⏳ Already loading ${key}, skipping duplicate`);
      return { success: false, error: 'Already loading' };
    }
    
    // Додаємо до активних завантажень
    activeLoadsRef.current.add(key);
    console.log(`🚀 Starting load: ${key}`);
    
    try {
      const data = await fetchData(metric, resolution);
      
      // ВИПРАВЛЕНО: Оновлюємо dataCache правильно для React
      setDataCache(prevCache => ({
        ...prevCache,
        [key]: data
      }));
      
      console.log(`✅ SUCCESS: ${key} loaded with ${data.hexagons.length} hexagons`);
      
      return { success: true, data };
      
    } catch (err) {
      console.error(`❌ FAILED: ${key} - ${err.message}`);
      
      return {
        success: false,
        error: err.message,
        debugInfo: {
          dataset: key,
          url: `${API_BASE}?metric_type=${metric}&resolution=${resolution}`,
          error: err.message,
          timestamp: new Date().toISOString()
        }
      };
    } finally {
      // Видаляємо з активних завантажень
      activeLoadsRef.current.delete(key);
    }
  }, [fetchData, dataCache]); // ВИПРАВЛЕНО: Правильні dependencies
  
  // ВИПРАВЛЕНО: Основна логіка завантаження без race conditions
  const startLoading = useCallback(async () => {
    // Запобігаємо множинним одночасним завантаженням
    if (loadingInProgressRef.current) {
      console.log('🛑 Loading already in progress, skipping');
      return;
    }
    
    loadingInProgressRef.current = true;
    
    console.log('🎯 ===== SMART H3 LOADING STARTED =====');
    
    setLoading(true);
    setError(null);
    setProgress(0);
    setTier1Complete(false);
    setStatus('Початок завантаження...');
    
    try {
      console.log('📋 Phase 1: Loading critical datasets');
      
      // Phase 1: Критичні датасети (FAIL-STOP)
      for (let i = 0; i < TIER_1_DATASETS.length; i++) {
        const dataset = TIER_1_DATASETS[i];
        
        console.log(`🔄 [${i+1}/${TIER_1_DATASETS.length}] Loading ${dataset.metric} H3-${dataset.resolution}`);
        
        setStatus(`Завантаження ${dataset.metric} H3-${dataset.resolution}...`);
        setProgress((i / (TIER_1_DATASETS.length + BACKGROUND_DATASETS.length)) * 100);
        
        const result = await loadDataset(dataset.metric, dataset.resolution);
        
        if (!result.success && result.error !== 'Already loading') {
          console.error(`🛑 CRITICAL FAILURE: ${dataset.metric}-${dataset.resolution}`);
          
          setError(`Критична помилка: ${result.error}`);
          setLoading(false);
          loadingInProgressRef.current = false;
          return;
        }
      }
      
      // Phase 1 завершено - користувач може взаємодіяти
      console.log('🎉 PHASE 1 COMPLETE - Ready for interaction!');
      setTier1Complete(true);
      setStatus('Базові дані готові');
      setProgress(50);
      
      // Невелика затримка для UI
      await new Promise(resolve => setTimeout(resolve, 100));
      
      console.log('📋 Phase 2: Background loading');
      
      // Phase 2: Фонові датасети (non-blocking)
      for (let i = 0; i < BACKGROUND_DATASETS.length; i++) {
        const dataset = BACKGROUND_DATASETS[i];
        
        setStatus(`Фонове завантаження ${dataset.metric} H3-${dataset.resolution}...`);
        setProgress(50 + ((i + 1) / BACKGROUND_DATASETS.length) * 50);
        
        const result = await loadDataset(dataset.metric, dataset.resolution);
        
        if (!result.success && result.error !== 'Already loading') {
          console.warn(`⚠️ Background dataset failed: ${dataset.metric}-${dataset.resolution}`);
        }
      }
      
      // Завершено
      console.log('🎉 ===== ALL LOADING COMPLETED =====');
      setStatus('Завантаження завершено');
      setProgress(100);
      setLoading(false);
      
    } catch (err) {
      console.error('🚨 Unexpected error:', err);
      setError(`Неочікувана помилка: ${err.message}`);
      setLoading(false);
    } finally {
      loadingInProgressRef.current = false;
    }
  }, [loadDataset]); // ВИПРАВЛЕНО: Мінімальні dependencies
  
  // ВИПРАВЛЕНО: Функція retry без проблем
  const retry = useCallback(() => {
    console.log('🔄 Manual retry initiated');
    
    // Очищуємо помилки але зберігаємо успішно завантажені дані
    setError(null);
    setLoading(false);
    setTier1Complete(false);
    loadingInProgressRef.current = false;
    activeLoadsRef.current.clear();
    
    // Запускаємо завантаження
    setTimeout(startLoading, 100);
  }, [startLoading]);
  
  // ВИПРАВЛЕНО: getAvailableData з правильними dependencies та валідацією
  const getAvailableData = useCallback((metric, resolution) => {
    const key = `${metric}-${resolution}`;
    const data = dataCache[key];
    
    console.log(`🔍 getAvailableData(${key}):`, {
      hasData: !!data,
      hexagonsCount: data?.hexagons?.length || 0,
      availableKeys: Object.keys(dataCache)
    });
    
    if (!data?.hexagons || !Array.isArray(data.hexagons)) {
      console.warn(`⚠️ No valid data for ${key}`);
      return [];
    }
    
    // Нормалізуємо та валідуємо всі гексагони
    const normalizedHexagons = data.hexagons
      .map(hex => normalizeHexagonData(hex, metric, resolution))
      .filter(Boolean); // Видаляємо null значення (невалідні гексагони)
    
    console.log(`✅ Returning ${normalizedHexagons.length} normalized hexagons for ${key}`);
    return normalizedHexagons;
    
  }, [dataCache]); // ВИПРАВЛЕНО: Правильні dependencies
  
  // Автостарт при монтуванні
  useEffect(() => {
    if (!loadingInProgressRef.current && Object.keys(dataCache).length === 0) {
      console.log('🎬 Auto-starting loading on mount');
      startLoading();
    }
  }, []); // ВИПРАВЛЕНО: Пустий dependency array для one-time effect
  
  // ВИПРАВЛЕНО: Повертаємо стабільний та передбачуваний інтерфейс
  return {
    // Стани завантаження
    isLoading: loading,
    canInteract: tier1Complete,
    tier1Complete: tier1Complete,
    
    // Прогрес
    progress: Math.round(progress),
    currentStep: status,
    
    // Доступ до даних
    getAvailableData,
    
    // ВИПРАВЛЕНО: getCachedData з правильними dependencies
    getCachedData: useCallback((metric, resolution) => {
      const key = `${metric}-${resolution}`;
      const result = dataCache[key] || null;
      console.log(`🔍 getCachedData(${key}):`, !!result);
      return result;
    }, [dataCache]),
    
    // Legacy compatibility
    getVisibleData: getAvailableData,
    
    // Обробка помилок
    hasError: !!error,
    error,
    retryLoading: retry,
    
    // Compatibility поля
    isBasicReady: tier1Complete,
    isFullyFunctional: tier1Complete,
    isCompletelyLoaded: !loading && tier1Complete,
    
    // Метрики та дебаг інформація
    debugInfo: {
      cacheSize: Object.keys(dataCache).length,
      cachedKeys: Object.keys(dataCache),
      totalHexagons: Object.values(dataCache).reduce((sum, data) => {
        return sum + (data?.hexagons?.length || 0);
      }, 0),
      loadingState: loading ? 'loading' : (error ? 'error' : 'complete'),
      tier1Complete: tier1Complete
    },
    
    // Допоміжні методи
    updateViewport: useCallback(() => {
      console.log('Viewport updated');
    }, []),
    
    prioritizeMetric: useCallback((metric) => {
      console.log(`Prioritizing metric: ${metric}`);
    }, [])
  };
};

export default useSmartH3Loading;