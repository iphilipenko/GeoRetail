// frontend/src/components/H3Visualization/hooks/useSmartH3Loading.js

import { useState, useEffect, useRef, useCallback } from 'react';

/**
 * 🚀 Smart H3 Loading Hook - Resolution-Based Progressive Strategy
 * 
 * NEW STRATEGY based on real data analysis:
 * - H3-7: 8,714 hexagons (full coverage, instant context)
 * - H3-8: 30,840 hexagons (detailed view, progressive replacement)
 * - H3-9: 78,220 hexagons (high detail, on-demand)
 * - H3-10: 141,502 hexagons (maximum detail, on-demand)
 * 
 * Tier 1: H3-7 Full Coverage (0-2 сек) - користувач бачить всю область миттєво
 * Tier 2: H3-8 Detailed Replacement (2-8 сек) - заміна H3-7 на деталізовані дані
 * Tier 3: H3-9/10 Maximum Detail (8-20 сек) - максимальна деталізація по потребі
 * 
 * @param {Object} initialViewport - початковий viewport (optimized for H3-7)
 * @param {string} initialMetric - початкова метрика ('opportunity' за замовчуванням)
 */

// ===============================================
// DATA TRANSFORMATION UTILITIES
// ===============================================

/**
 * Трансформація API response в формат що очікує компонент
 */
const transformApiResponse = (apiData, resolution) => {
  console.log(`🔄 Transforming API response for H3-${resolution}:`, {
    type: typeof apiData,
    isArray: Array.isArray(apiData),
    keys: apiData && typeof apiData === 'object' ? Object.keys(apiData) : 'N/A'
  });
  
  if (!apiData) {
    console.warn('⚠️ API response is null/undefined');
    return { hexagons: [], resolution };
  }
  
  // Перевіряємо різні можливі формати API response
  let hexagons = [];
  
  if (apiData.hexagons && Array.isArray(apiData.hexagons)) {
    hexagons = apiData.hexagons;
    console.log(`✅ Found hexagons in apiData.hexagons for H3-${resolution}`);
  } else if (Array.isArray(apiData)) {
    hexagons = apiData;
    console.log(`✅ API response is direct array for H3-${resolution}`);
  } else if (apiData.data && Array.isArray(apiData.data)) {
    hexagons = apiData.data;
    console.log(`✅ Found hexagons in apiData.data for H3-${resolution}`);
  } else if (apiData.features && Array.isArray(apiData.features)) {
    hexagons = apiData.features;
    console.log(`✅ Found hexagons in apiData.features (GeoJSON) for H3-${resolution}`);
  } else {
    console.warn(`⚠️ Unexpected API response format for H3-${resolution}:`, {
      availableKeys: Object.keys(apiData),
      sampleData: JSON.stringify(apiData).substring(0, 200)
    });
    return { hexagons: [], resolution };
  }
  
  console.log(`🔢 Raw hexagons count for H3-${resolution}: ${hexagons.length}`);
  
  // Валідація структури гексагонів
  const validHexagons = hexagons.filter((hex, index) => {
    if (!hex || typeof hex !== 'object') {
      if (index < 3) console.warn(`⚠️ Invalid hexagon at index ${index}: not an object`);
      return false;
    }
    
    if (!hex.geometry) {
      if (index < 3) console.warn(`⚠️ Invalid hexagon at index ${index}: missing geometry`);
      return false;
    }
    
    // Додаємо resolution мітку для layer management
    hex._resolution = resolution;
    hex._loadTier = resolution === 7 ? 'tier1' : resolution === 8 ? 'tier2' : 'tier3';
    
    if (!hex.properties) {
      hex.properties = {};
    }
    
    return true;
  });
  
  if (validHexagons.length !== hexagons.length) {
    console.warn(`⚠️ Filtered out ${hexagons.length - validHexagons.length} invalid hexagons for H3-${resolution}`);
  }
  
  console.log(`✅ Valid hexagons count for H3-${resolution}: ${validHexagons.length}`);
  
  return { 
    hexagons: validHexagons,
    resolution,
    originalCount: hexagons.length,
    validCount: validHexagons.length,
    meta: apiData.meta || {}
  };
};

const useSmartH3Loading = (initialViewport, initialMetric = 'opportunity') => {
  // ===============================================
  // STATE MANAGEMENT - Resolution-Based Tiers
  // ===============================================
  
  const [loadingTiers, setLoadingTiers] = useState({
    tier1: { status: 'loading', progress: 0, data: null, error: null, resolution: 7 },
    tier2: { status: 'pending', progress: 0, data: null, error: null, resolution: 8 },
    tier3: { status: 'pending', progress: 0, data: null, error: null, resolution: 9 }
  });
  
  // Progressive readiness states
  const [isBasicReady, setIsBasicReady] = useState(false);        // H3-7 loaded
  const [isFullyFunctional, setIsFullyFunctional] = useState(false); // H3-8 loaded
  const [isCompletelyLoaded, setIsCompletelyLoaded] = useState(false); // H3-9+ loaded
  
  // Current context
  const [currentMetric, setCurrentMetric] = useState(initialMetric);
  const [currentViewport, setCurrentViewport] = useState(initialViewport);
  const [activeResolution, setActiveResolution] = useState(7); // Currently displayed resolution
  
  // Data cache по resolution
  const dataCache = useRef(new Map());
  const startTime = useRef(Date.now());
  const tierStartTimes = useRef({});
  
  // Performance metrics
  const [performanceMetrics, setPerformanceMetrics] = useState({
    timeToInteractive: null,
    timeToFullyFunctional: null,
    timeToComplete: null,
    dataUtilizationRate: 0,
    cacheHitRate: 0,
    resolutionProgression: []
  });

  // ===============================================
  // UTILITY FUNCTIONS
  // ===============================================

  const updateTierState = useCallback((tier, updates) => {
    setLoadingTiers(prev => ({
      ...prev,
      [tier]: { ...prev[tier], ...updates }
    }));
  }, []);

  // Resolution-aware bounds calculation
  const calculateResolutionBounds = useCallback((viewport, resolution) => {
    const { longitude, latitude, zoom } = viewport;
    
    // Different strategies for different resolutions
    const resolutionStrategies = {
      7: {
        // H3-7: Wide area for full coverage
        multiplier: 2.0,
        description: 'Full area coverage for H3-7'
      },
      8: {
        // H3-8: Balanced area
        multiplier: 1.5,
        description: 'Detailed area for H3-8'
      },
      9: {
        // H3-9: Focused area
        multiplier: 1.0,
        description: 'Focused area for H3-9'
      },
      10: {
        // H3-10: Precise area
        multiplier: 0.8,
        description: 'Precise area for H3-10'
      }
    };
    
    const strategy = resolutionStrategies[resolution] || resolutionStrategies[8];
    const baseSpan = 0.05 * strategy.multiplier * Math.pow(2, Math.max(0, 8 - zoom));
    
    const latSpan = baseSpan;
    const lonSpan = baseSpan * Math.cos(latitude * Math.PI / 180);
    
    return {
      north: latitude + latSpan,
      south: latitude - latSpan,
      east: longitude + lonSpan,
      west: longitude - lonSpan,
      strategy: strategy.description
    };
  }, []);

  // API call with resolution-specific limits
  const fetchResolutionData = useCallback(async (metric, resolution, retries = 2) => {
    // Resolution-specific limits based on real data analysis
    const resolutionLimits = {
      7: 10000,  // Will cover all 8,714 hexagons
      8: 35000,  // Will cover all 30,840 hexagons  
      9: 80000,  // Will cover most of 78,220 hexagons
      10: 150000 // Will cover most of 141,502 hexagons
    };
    
    const limit = resolutionLimits[resolution] || 10000;
    const url = `http://localhost:8000/api/v1/visualization/kyiv-h3?` +
      `metric_type=${metric}&` +
      `resolution=${resolution}&` +
      `limit=${limit}`;

    console.log(`📡 Fetching H3-${resolution} data:`, { url: url.split('?')[0], metric, resolution, limit });
    
    let lastError;
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 20000); // 20 second timeout
        
        const response = await fetch(url, { signal: controller.signal });
        clearTimeout(timeoutId);
        
        if (!response.ok) {
          throw new Error(`API error: ${response.status} ${response.statusText}`);
        }
        
        const rawData = await response.json();
        const transformedData = transformApiResponse(rawData, resolution);
        
        console.log(`✅ H3-${resolution} data loaded successfully: ${transformedData.hexagons.length} hexagons`);
        return transformedData;
        
      } catch (error) {
        lastError = error;
        console.warn(`⚠️ H3-${resolution} attempt ${attempt + 1} failed:`, error.message);
        
        if (attempt < retries) {
          const delay = Math.pow(2, attempt) * 1000;
          console.log(`🔄 Retrying H3-${resolution} in ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    
    throw lastError;
  }, []);

  // ===============================================
  // TIER LOADING FUNCTIONS - NEW RESOLUTION STRATEGY
  // ===============================================

  // Tier 1: H3-7 Full Coverage (< 2 seconds)
  const loadTier1Data = useCallback(async () => {
    console.log('🚀 Starting Tier 1 loading: H3-7 Full Coverage...');
    const tierStartTime = Date.now();
    tierStartTimes.current.tier1 = tierStartTime;
    
    try {
      updateTierState('tier1', { status: 'loading', progress: 10 });
      
      // Check cache first
      const cacheKey = `h3-7-${currentMetric}`;
      const cachedData = dataCache.current.get(cacheKey);
      
      if (cachedData) {
        console.log('⚡ H3-7 data found in cache!');
        updateTierState('tier1', { 
          status: 'completed', 
          progress: 100, 
          data: cachedData 
        });
        setIsBasicReady(true);
        setActiveResolution(7);
        
        const timeToInteractive = Date.now() - tierStartTime;
        setPerformanceMetrics(prev => ({ 
          ...prev, 
          timeToInteractive,
          cacheHitRate: prev.cacheHitRate + 1,
          resolutionProgression: [...prev.resolutionProgression, { resolution: 7, time: timeToInteractive }]
        }));
        return;
      }
      
      updateTierState('tier1', { progress: 30 });
      
      // Load H3-7 data (should be ~8,714 hexagons - full coverage)
      const h3_7_data = await fetchResolutionData(currentMetric, 7);
      
      updateTierState('tier1', { progress: 80 });
      
      // Cache the data
      dataCache.current.set(cacheKey, h3_7_data);
      
      updateTierState('tier1', { 
        status: 'completed', 
        progress: 100, 
        data: h3_7_data 
      });
      
      // Enable basic interaction with H3-7
      setIsBasicReady(true);
      setActiveResolution(7);
      
      const timeToInteractive = Date.now() - tierStartTime;
      setPerformanceMetrics(prev => ({ 
        ...prev, 
        timeToInteractive,
        resolutionProgression: [...prev.resolutionProgression, { resolution: 7, time: timeToInteractive }]
      }));
      
      console.log(`✅ Tier 1 (H3-7) completed in ${timeToInteractive}ms with ${h3_7_data.hexagons.length} hexagons`);
      
      // Auto-start Tier 2 after short delay
      setTimeout(() => {
        loadTier2Data();
      }, 1000);
      
    } catch (error) {
      console.error('❌ Tier 1 (H3-7) loading failed:', error);
      updateTierState('tier1', { 
        status: 'error', 
        error: error.message 
      });
    }
  }, [currentMetric, fetchResolutionData, updateTierState]);

  // Tier 2: H3-8 Progressive Replacement (2-8 seconds)
  const loadTier2Data = useCallback(async () => {
    if (!isBasicReady) {
      console.log('⏳ Tier 2 waiting for Tier 1 to complete...');
      return;
    }
    
    console.log('⚡ Starting Tier 2 loading: H3-8 Detailed Replacement...');
    const tierStartTime = Date.now();
    tierStartTimes.current.tier2 = tierStartTime;
    
    try {
      updateTierState('tier2', { status: 'loading', progress: 0 });
      
      // Check cache first
      const cacheKey = `h3-8-${currentMetric}`;
      const cachedData = dataCache.current.get(cacheKey);
      
      if (cachedData) {
        console.log('⚡ H3-8 data found in cache!');
        updateTierState('tier2', { 
          status: 'completed', 
          progress: 100, 
          data: cachedData 
        });
        setIsFullyFunctional(true);
        setActiveResolution(8); // Switch to H3-8 display
        
        const timeToFullyFunctional = Date.now() - tierStartTime;
        setPerformanceMetrics(prev => ({ 
          ...prev, 
          timeToFullyFunctional,
          cacheHitRate: prev.cacheHitRate + 1,
          resolutionProgression: [...prev.resolutionProgression, { resolution: 8, time: timeToFullyFunctional }]
        }));
        return;
      }
      
      updateTierState('tier2', { progress: 20 });
      
      // Load H3-8 data (should be ~30,840 hexagons - detailed coverage)
      const h3_8_data = await fetchResolutionData(currentMetric, 8);
      
      updateTierState('tier2', { progress: 80 });
      
      // Cache the data
      dataCache.current.set(cacheKey, h3_8_data);
      
      updateTierState('tier2', { 
        status: 'completed', 
        progress: 100, 
        data: h3_8_data 
      });
      
      // Switch to H3-8 display (progressive replacement)
      setIsFullyFunctional(true);
      setActiveResolution(8);
      
      const timeToFullyFunctional = Date.now() - tierStartTime;
      setPerformanceMetrics(prev => ({ 
        ...prev, 
        timeToFullyFunctional,
        resolutionProgression: [...prev.resolutionProgression, { resolution: 8, time: timeToFullyFunctional }]
      }));
      
      console.log(`✅ Tier 2 (H3-8) completed in ${timeToFullyFunctional}ms with ${h3_8_data.hexagons.length} hexagons`);
      console.log(`🔄 Switched display from H3-7 to H3-8 (progressive replacement)`);
      
      // Auto-start Tier 3 for high zoom levels
      if (currentViewport.zoom >= 9) {
        setTimeout(() => {
          loadTier3Data();
        }, 2000);
      }
      
    } catch (error) {
      console.error('❌ Tier 2 (H3-8) loading failed:', error);
      updateTierState('tier2', { 
        status: 'error', 
        error: error.message 
      });
    }
  }, [isBasicReady, currentMetric, currentViewport.zoom, fetchResolutionData, updateTierState]);

  // Tier 3: H3-9/10 Maximum Detail (8-20 seconds, on-demand)
  const loadTier3Data = useCallback(async () => {
    if (!isFullyFunctional) {
      console.log('⏳ Tier 3 waiting for Tier 2 to complete...');
      return;
    }
    
    console.log('🔄 Starting Tier 3 loading: H3-9 Maximum Detail...');
    const tierStartTime = Date.now();
    tierStartTimes.current.tier3 = tierStartTime;
    
    try {
      updateTierState('tier3', { status: 'loading', progress: 0 });
      
      // Load H3-9 for maximum detail (zoom-dependent)
      const targetResolution = currentViewport.zoom >= 11 ? 10 : 9;
      const cacheKey = `h3-${targetResolution}-${currentMetric}`;
      
      const cachedData = dataCache.current.get(cacheKey);
      
      if (cachedData) {
        console.log(`⚡ H3-${targetResolution} data found in cache!`);
        updateTierState('tier3', { 
          status: 'completed', 
          progress: 100, 
          data: cachedData,
          resolution: targetResolution
        });
        setIsCompletelyLoaded(true);
        setActiveResolution(targetResolution);
        return;
      }
      
      updateTierState('tier3', { progress: 30 });
      
      // Load high-resolution data
      const highResData = await fetchResolutionData(currentMetric, targetResolution);
      
      updateTierState('tier3', { progress: 80 });
      
      dataCache.current.set(cacheKey, highResData);
      
      updateTierState('tier3', { 
        status: 'completed', 
        progress: 100, 
        data: highResData,
        resolution: targetResolution
      });
      
      setIsCompletelyLoaded(true);
      setActiveResolution(targetResolution);
      
      const timeToComplete = Date.now() - tierStartTime;
      const totalTime = Date.now() - startTime.current;
      
      setPerformanceMetrics(prev => ({ 
        ...prev, 
        timeToComplete: totalTime,
        resolutionProgression: [...prev.resolutionProgression, { resolution: targetResolution, time: timeToComplete }],
        dataUtilizationRate: calculateDataUtilization()
      }));
      
      console.log(`✅ Tier 3 (H3-${targetResolution}) completed in ${timeToComplete}ms with ${highResData.hexagons.length} hexagons`);
      console.log(`🎉 Total loading time: ${totalTime}ms`);
      
    } catch (error) {
      console.error('❌ Tier 3 loading failed:', error);
      updateTierState('tier3', { 
        status: 'error', 
        error: error.message 
      });
    }
  }, [isFullyFunctional, currentMetric, currentViewport.zoom, fetchResolutionData, updateTierState]);

  // ===============================================
  // DATA ACCESS FUNCTIONS - RESOLUTION AWARE
  // ===============================================

  // Get currently active data based on resolution progression
  const getActiveData = useCallback((requestedMetric, requestedResolution, viewport) => {
    const metric = requestedMetric || currentMetric;
    
    // Determine best available resolution based on loading state and request
    let bestResolution = activeResolution;
    
    // If specific resolution requested, try to provide it
    if (requestedResolution) {
      const requestedKey = `h3-${requestedResolution}-${metric}`;
      if (dataCache.current.has(requestedKey)) {
        bestResolution = requestedResolution;
      }
    }
    
    const cacheKey = `h3-${bestResolution}-${metric}`;
    const data = dataCache.current.get(cacheKey);
    
    if (data && data.hexagons && data.hexagons.length > 0) {
      console.log(`📦 Returning H3-${bestResolution} data: ${data.hexagons.length} hexagons for ${metric}`);
      
      // Apply viewport culling if needed
      if (viewport && bestResolution >= 8) {
        return applyViewportCulling(data.hexagons, viewport);
      }
      
      return data.hexagons;
    }
    
    console.log(`⚠️ No data available for ${metric} at resolution ${bestResolution}`);
    console.log(`📋 Available cache keys:`, Array.from(dataCache.current.keys()));
    
    return [];
  }, [currentMetric, activeResolution]);

  // Viewport culling for high-resolution data
  const applyViewportCulling = useCallback((hexagons, viewport, bufferFactor = 1.5) => {
    if (!viewport || viewport.zoom <= 7) {
      return hexagons; // Show all for wide zoom
    }

    try {
      const { longitude, latitude, zoom } = viewport;
      
      const latSpan = (360 / Math.pow(2, zoom)) * bufferFactor;
      const lonSpan = latSpan * Math.cos(latitude * Math.PI / 180);
      
      const bounds = {
        north: latitude + latSpan,
        south: latitude - latSpan,
        east: longitude + lonSpan,
        west: longitude - lonSpan
      };

      const visibleHexagons = hexagons.filter(hex => {
        if (!hex.geometry?.coordinates?.[0]) return false;
        
        try {
          const coords = hex.geometry.coordinates[0];
          const centerLon = coords.reduce((sum, coord) => sum + coord[0], 0) / coords.length;
          const centerLat = coords.reduce((sum, coord) => sum + coord[1], 0) / coords.length;
          
          return centerLat >= bounds.south && 
                 centerLat <= bounds.north &&
                 centerLon >= bounds.west && 
                 centerLon <= bounds.east;
        } catch (error) {
          return false;
        }
      });

      console.log(`🔍 Viewport culling: ${visibleHexagons.length}/${hexagons.length} hexagons visible (zoom: ${zoom.toFixed(1)})`);
      return visibleHexagons;
      
    } catch (error) {
      console.error('❌ Error in viewport culling:', error);
      return hexagons;
    }
  }, []);

  // Calculate data utilization rate
  const calculateDataUtilization = useCallback(() => {
    const totalCached = dataCache.current.size;
    if (totalCached === 0) return 0;
    
    // Better utilization calculation based on resolution progression
    const progressionScore = performanceMetrics.resolutionProgression.length * 25;
    return Math.min(progressionScore, 100);
  }, [performanceMetrics.resolutionProgression]);

  // Priority metric switch with smart data reuse
  const prioritizeMetric = useCallback((newMetric) => {
    if (newMetric === currentMetric) return;
    
    console.log(`🔄 Switching metric: ${currentMetric} → ${newMetric}`);
    setCurrentMetric(newMetric);
    
    // Check if we have data for new metric at current resolution
    const currentResKey = `h3-${activeResolution}-${newMetric}`;
    if (dataCache.current.has(currentResKey)) {
      console.log(`⚡ Instant metric switch - H3-${activeResolution} data available for ${newMetric}`);
      return;
    }
    
    // Start progressive loading for new metric
    console.log(`📡 Starting progressive loading for ${newMetric}`);
    loadTier1Data();
  }, [currentMetric, activeResolution, loadTier1Data]);

  // Get current activity for UI
  const getCurrentActivity = useCallback(() => {
    if (loadingTiers.tier1.status === 'loading') return 'Завантаження повного покриття (H3-7)...';
    if (loadingTiers.tier2.status === 'loading') return 'Деталізація даних (H3-8)...';
    if (loadingTiers.tier3.status === 'loading') return 'Максимальна деталізація (H3-9/10)...';
    if (isCompletelyLoaded) return `Готово! Відображення H3-${activeResolution}`;
    if (isFullyFunctional) return `Детальні дані готові (H3-${activeResolution})`;
    if (isBasicReady) return `Повне покриття готове (H3-${activeResolution})`;
    return 'Ініціалізація...';
  }, [loadingTiers, isBasicReady, isFullyFunctional, isCompletelyLoaded, activeResolution]);

  // ===============================================
  // MAIN LOADING ORCHESTRATION
  // ===============================================

  useEffect(() => {
    let cancelled = false;
    
    const startProgressiveLoading = async () => {
      if (cancelled) return;
      
      console.log('🚀 Starting Progressive H3 Loading Strategy...');
      console.log('📊 Target coverage: H3-7(8K) → H3-8(30K) → H3-9/10(78K/141K)');
      startTime.current = Date.now();
      
      // Start with H3-7 for instant full coverage
      await loadTier1Data();
    };
    
    startProgressiveLoading();
    
    return () => {
      cancelled = true;
    };
  }, [loadTier1Data]);

  // Zoom-based tier triggering
  useEffect(() => {
    if (currentViewport.zoom >= 9 && isFullyFunctional && !isCompletelyLoaded) {
      // High zoom detected, trigger Tier 3 if not already loaded
      const tier3Status = loadingTiers.tier3.status;
      if (tier3Status === 'pending') {
        console.log(`🔍 High zoom detected (${currentViewport.zoom.toFixed(1)}), triggering Tier 3...`);
        setTimeout(() => loadTier3Data(), 500);
      }
    }
  }, [currentViewport.zoom, isFullyFunctional, isCompletelyLoaded, loadingTiers.tier3.status, loadTier3Data]);

  // ===============================================
  // RETURN API - COMPATIBLE WITH LEGACY
  // ===============================================

  return {
    // Progressive readiness states
    isBasicReady,        // H3-7 loaded (full coverage)
    isFullyFunctional,   // H3-8 loaded (detailed view)
    isCompletelyLoaded,  // H3-9/10 loaded (maximum detail)
    
    // New resolution-aware states
    activeResolution,    // Currently displayed resolution
    
    // Loading states for UI
    loadingTiers,
    currentActivity: getCurrentActivity(),
    
    // Data access (compatible with legacy API)
    getAvailableData: getActiveData,
    getVisibleHexagons: (metric, resolution, viewState) => 
      getActiveData(metric, resolution, viewState),
    getCachedData: (metric, resolution) => 
      getActiveData(metric, resolution, null),
    
    // User interactions
    prioritizeMetric,
    updateViewport: setCurrentViewport,
    
    // Performance insights
    performanceMetrics,
    dataUtilizationRate: calculateDataUtilization(),
    
    // Statistics (enhanced)
    getStats: () => ({
      loadedDatasets: dataCache.current.size,
      totalDatasets: 3, // H3-7, H3-8, H3-9/10
      totalHexagons: Array.from(dataCache.current.values())
        .reduce((sum, data) => sum + (data.hexagons?.length || 0), 0),
      estimatedMemoryKB: Math.round(dataCache.current.size * 500),
      activeResolution,
      resolutionProgression: performanceMetrics.resolutionProgression
    }),
    
    // Debug info
    debugInfo: {
      cacheSize: dataCache.current.size,
      cacheKeys: Array.from(dataCache.current.keys()),
      tierStates: loadingTiers,
      startTime: startTime.current,
      currentMetric,
      currentViewport,
      activeResolution,
      resolutionProgression: performanceMetrics.resolutionProgression
    }
  };
};

export default useSmartH3Loading;