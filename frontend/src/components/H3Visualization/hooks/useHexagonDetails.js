// useHexagonDetails.js
// 🎯 Hook для завантаження детальної інформації про гексагон
// Включає caching в sessionStorage та error handling

import { useState, useEffect, useCallback } from 'react';

const useHexagonDetails = (h3Index, resolution) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [analysisType, setAnalysisType] = useState('site_selection');
  const [customRings, setCustomRings] = useState(null);

  // Cache key для sessionStorage
  const getCacheKey = useCallback((index, res, type, rings) => {
    return `hexagon-details-${index}-${res}-${type}-${rings || 'auto'}`;
  }, []);

  // Завантаження з cache
  const loadFromCache = useCallback((cacheKey) => {
    try {
      const cached = sessionStorage.getItem(cacheKey);
      if (cached) {
        const parsedData = JSON.parse(cached);
        // Перевіряємо чи не застарілі дані (1 година)
        const age = Date.now() - new Date(parsedData.cached_at).getTime();
        if (age < 60 * 60 * 1000) { // 1 година
          return parsedData.data;
        }
      }
    } catch (e) {
      console.warn('Failed to load from cache:', e);
    }
    return null;
  }, []);

  // Збереження в cache
  const saveToCache = useCallback((cacheKey, data) => {
    try {
      const cacheData = {
        data,
        cached_at: new Date().toISOString()
      };
      sessionStorage.setItem(cacheKey, JSON.stringify(cacheData));
    } catch (e) {
      console.warn('Failed to save to cache:', e);
    }
  }, []);

  // API виклик
  const fetchHexagonDetails = useCallback(async (index, res, type, rings) => {
    if (!index || !res) return;

    const cacheKey = getCacheKey(index, res, type, rings);
    
    // Спробуємо завантажити з cache
    const cachedData = loadFromCache(cacheKey);
    if (cachedData) {
      console.log('📦 Loaded hexagon details from cache');
      setData(cachedData);
      return cachedData;
    }

    setLoading(true);
    setError(null);

    try {
      console.log(`🔍 Fetching hexagon details: ${index}, resolution=${res}, analysis=${type}`);
      
      // Формуємо URL з параметрами
      const params = new URLSearchParams({
        resolution: res,
        analysis_type: type
      });
      
      if (type === 'custom' && rings !== null) {
        params.append('custom_rings', rings);
      }

      const response = await fetch(`/api/v1/hexagon-details/details/${index}?${params}`);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();
      
      console.log(`✅ Fetched hexagon details: ${result.poi_details?.length || 0} POI, ${result.neighbor_coverage?.area_km2 || 0} km²`);
      
      // Зберігаємо в cache
      saveToCache(cacheKey, result);
      
      setData(result);
      return result;
      
    } catch (err) {
      console.error('❌ Error fetching hexagon details:', err);
      setError(err.message);
      return null;
    } finally {
      setLoading(false);
    }
  }, [getCacheKey, loadFromCache, saveToCache]);

  // Ефект для автоматичного завантаження
  useEffect(() => {
    if (h3Index && resolution) {
      fetchHexagonDetails(h3Index, resolution, analysisType, customRings);
    }
  }, [h3Index, resolution, analysisType, customRings, fetchHexagonDetails]);

  // Функції для зміни параметрів аналізу
  const changeAnalysisType = useCallback((newType) => {
    setAnalysisType(newType);
    setCustomRings(null); // Скидаємо custom rings при зміні типу
  }, []);

  const changeCustomRings = useCallback((rings) => {
    setCustomRings(rings);
    if (analysisType !== 'custom') {
      setAnalysisType('custom');
    }
  }, [analysisType]);

  // Ручне оновлення даних
  const refresh = useCallback(() => {
    if (h3Index && resolution) {
      // Очищуємо cache перед оновленням
      const cacheKey = getCacheKey(h3Index, resolution, analysisType, customRings);
      sessionStorage.removeItem(cacheKey);
      
      return fetchHexagonDetails(h3Index, resolution, analysisType, customRings);
    }
  }, [h3Index, resolution, analysisType, customRings, getCacheKey, fetchHexagonDetails]);

  // Очищення cache
  const clearCache = useCallback(() => {
    const keys = Object.keys(sessionStorage);
    keys.forEach(key => {
      if (key.startsWith('hexagon-details-')) {
        sessionStorage.removeItem(key);
      }
    });
    console.log('🗑️ Cleared hexagon details cache');
  }, []);

  return {
    data,
    loading,
    error,
    analysisType,
    customRings,
    
    // Functions
    changeAnalysisType,
    changeCustomRings,
    refresh,
    clearCache,
    
    // Computed properties
    hasData: !!data,
    locationInfo: data?.location_info || null,
    metrics: data?.metrics || null,
    poiDetails: data?.poi_details || [],
    influenceAnalysis: data?.influence_analysis || [],
    neighborCoverage: data?.neighbor_coverage || null,
    availableAnalyses: data?.available_analyses || []
  };
};

export default useHexagonDetails;
