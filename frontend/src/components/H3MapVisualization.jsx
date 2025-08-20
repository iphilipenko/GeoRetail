// frontend/src/components/H3MapVisualization.jsx
// ПОВНІСТЮ ВИПРАВЛЕНА ВЕРСІЯ - Всі критичні проблеми вирішено

import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

// Existing imports
import MetricSwitcher from './H3Visualization/components/controls/MetricSwitcher';
import PreloadProgressBar from './H3Visualization/components/ui/PreloadProgressBar';
import HoverTooltip from './H3Visualization/components/ui/HoverTooltip';
import { H3_COLOR_SCHEMES } from './H3Visualization/utils/colorSchemes';

// Legacy import
import usePreloadedH3Data from './H3Visualization/hooks/usePreloadedH3Data';

// NEW: Smart Loading imports
import useSmartH3Loading from './H3Visualization/hooks/useSmartH3Loading';

// ===============================================
// HELPER FUNCTIONS
// ===============================================

const useDebounce = (value, delay) => {
  const [debouncedValue, setDebouncedValue] = useState(value);
  
  useEffect(() => {
    const handler = setTimeout(() => {
      setDebouncedValue(value);
    }, delay);
    
    return () => {
      clearTimeout(handler);
    };
  }, [value, delay]);
  
  return debouncedValue;
};

const getOptimalResolution = (zoom) => {
  if (zoom < 8) return 7;
  if (zoom < 10) return 8;
  if (zoom < 12) return 9;
  return 10;
};

const getResolutionDescription = (resolution) => {
  const descriptions = {
    7: "Огляд області - великі гексагони",
    8: "Рівень району - середні гексагони", 
    9: "Рівень кварталу - детальні гексагони",
    10: "Рівень вулиці - найдетальніші"
  };
  return descriptions[resolution] || "";
};

// ===============================================
// ВИПРАВЛЕНА COLOR MAPPING ФУНКЦІЯ
// ===============================================

const getFixedFillColor = (d, metric) => {
  try {
    const scheme = H3_COLOR_SCHEMES[metric] || H3_COLOR_SCHEMES.opportunity;
    
    // Витягуємо значення метрики
    let value = 0;
    if (metric === 'opportunity') {
      value = d.market_opportunity_score || d.display_value || 0;
    } else if (metric === 'competition') {
      value = d.competition_intensity || d.display_value || 0;
    } else {
      value = d.display_value || d.market_opportunity_score || d.competition_intensity || 0;
    }
    
    // Конвертуємо в число
    if (typeof value === 'string') {
      value = parseFloat(value) || 0;
    }
    
    // Нормалізуємо до 0-1 діапазону
    value = Math.max(0, Math.min(1, value));
    
    // ВИПРАВЛЕНО: Використовуємо правильні назви полів відповідно до colorSchemes.js
    if (metric === 'opportunity') {
      // Для opportunity: low, medium, high
      if (value <= 0.33) return [...scheme.low, 140];
      if (value <= 0.66) return [...scheme.medium, 160];
      return [...scheme.high, 180];
    } else {
      // Для competition: low, medium, high, maximum
      if (value <= 0.25) return [...scheme.low, 140];
      if (value <= 0.5) return [...scheme.medium, 160];
      if (value <= 0.75) return [...scheme.high, 180];
      return [...scheme.maximum, 200];
    }
    
  } catch (error) {
    console.error('❌ Color mapping error:', error, d);
    return [255, 165, 0, 120]; // Помаранчевий fallback
  }
};

// ===============================================
// MAIN COMPONENT
// ===============================================

const H3MapVisualization = () => {
  // Feature flag для Smart Loading
  const [useSmartLoading, setUseSmartLoading] = useState(() => {
    const stored = localStorage.getItem('smart-loading-enabled');
    return stored !== 'false'; // Default to true
  });
  
  // State management
  const [metric, setMetric] = useState('opportunity');
  const [autoResolution, setAutoResolution] = useState(true);
  const [manualResolution, setManualResolution] = useState(8);
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [viewState, setViewState] = useState({
    longitude: 30.5234,
    latitude: 50.4501,
    zoom: 8,
    pitch: 0,
    bearing: 0
  });

  // Hooks
  const smartHook = useSmartH3Loading(viewState, metric);
  const legacyHook = usePreloadedH3Data(1000000);
  
  // Debounced view state for performance
  const debouncedViewState = useDebounce(viewState, 300);
  
  // Current resolution calculation
  const currentResolution = autoResolution ? getOptimalResolution(viewState.zoom) : manualResolution;
  
  // ===============================================
  // ВИПРАВЛЕНО: DATA MANAGEMENT
  // ===============================================
  
  // ВИПРАВЛЕНО: isDataReady дозволяє взаємодію після Phase 1
  const isDataReady = useMemo(() => {
    let ready = false;
    
    if (useSmartLoading) {
      // КЛЮЧОВЕ ВИПРАВЛЕННЯ: Дозволяємо взаємодію якщо tier1Complete, 
      // навіть якщо фонове завантаження все ще триває
      ready = (
        smartHook && 
        (smartHook.canInteract === true || smartHook.tier1Complete === true) &&
        !smartHook.hasError
        // ВИДАЛЕНО: !smartHook.isLoading - не блокуємо під час фонового завантаження
      );
      
      console.log(`🔍 Smart Loading ready check:`, {
        hookExists: !!smartHook,
        canInteract: smartHook?.canInteract,
        tier1Complete: smartHook?.tier1Complete,
        isLoading: smartHook?.isLoading,
        hasError: smartHook?.hasError,
        finalResult: ready
      });
    } else {
      ready = legacyHook && legacyHook.isPreloaded === true;
      console.log(`🔍 Legacy ready check:`, {
        hookExists: !!legacyHook,
        isPreloaded: legacyHook?.isPreloaded,
        finalResult: ready
      });
    }
    
    return ready;
  }, [
    useSmartLoading, 
    smartHook?.canInteract, 
    smartHook?.tier1Complete, 
    smartHook?.hasError,
    legacyHook?.isPreloaded
  ]); // ВИПРАВЛЕНО: Видалено isLoading з dependencies
  
  // ВИПРАВЛЕНО: getCurrentHexagons з надійною логікою
  const getCurrentHexagons = useCallback(() => {
    console.log(`🔍 getCurrentHexagons called:`, {
      isDataReady,
      useSmartLoading,
      metric,
      currentResolution
    });
    
    try {
      if (!isDataReady) {
        console.log('🔍 Data not ready yet, returning empty array');
        return [];
      }
      
      let hexagons = [];
      
      if (useSmartLoading) {
        if (smartHook && smartHook.getAvailableData) {
          console.log(`🔍 Calling smartHook.getAvailableData(${metric}, ${currentResolution})`);
          hexagons = smartHook.getAvailableData(metric, currentResolution);
          console.log(`🔍 Smart hook returned ${hexagons?.length || 0} hexagons`);
        } else {
          console.error('❌ smartHook.getAvailableData is not available');
        }
      } else {
        if (legacyHook && legacyHook.getVisibleHexagons) {
          hexagons = legacyHook.getVisibleHexagons(metric, currentResolution, debouncedViewState);
          console.log(`🔍 Legacy hook returned ${hexagons?.length || 0} hexagons`);
        } else {
          console.error('❌ legacyHook.getVisibleHexagons is not available');
        }
      }
      
      const finalHexagons = Array.isArray(hexagons) ? hexagons : [];
      console.log(`🔍 getCurrentHexagons final result: ${finalHexagons.length} hexagons`);
      
      return finalHexagons;
      
    } catch (error) {
      console.error('❌ Error in getCurrentHexagons:', error);
      return [];
    }
  }, [
    isDataReady, 
    useSmartLoading, 
    smartHook, 
    legacyHook, 
    metric, 
    currentResolution, 
    debouncedViewState
  ]);
  
  // ===============================================
  // EVENT HANDLERS
  // ===============================================
  
  const handleMetricChange = (newMetric) => {
    console.log(`🔄 Metric changed: ${metric} → ${newMetric}`);
    setMetric(newMetric);
    
    if (useSmartLoading && smartHook && smartHook.prioritizeMetric) {
      smartHook.prioritizeMetric(newMetric);
    }
  };
  
  const toggleSmartLoading = () => {
    const newValue = !useSmartLoading;
    setUseSmartLoading(newValue);
    localStorage.setItem('smart-loading-enabled', newValue.toString());
    console.log(`🔄 Loading strategy: ${newValue ? 'Smart' : 'Legacy'}`);
  };
  
  const handleViewStateChange = useCallback((event) => {
    setViewState(event.viewState);
    
    if (useSmartLoading && smartHook && smartHook.updateViewport) {
      smartHook.updateViewport(event.viewState);
    }
  }, [useSmartLoading, smartHook]);
  
  const handleHover = useCallback((info, event) => {
    if (info?.object) {
      setHoveredObject(info.object);
      setMousePosition({ x: info.x, y: info.y });
    } else {
      setHoveredObject(null);
    }
  }, []);
  
  // ===============================================
  // ВИПРАВЛЕНО: DECK.GL LAYERS
  // ===============================================
  
  const hexagonLayer = useMemo(() => {
    const hexagons = getCurrentHexagons(); // ВИПРАВЛЕНО: Правильний виклик функції
    
    if (hexagons.length === 0) {
      console.log('⚠️ No hexagons available for rendering');
      return null;
    }

    console.log(`🗺️ Rendering ${hexagons.length} hexagons with metric '${metric}'`);

    return new GeoJsonLayer({
      id: 'h3-hexagons-main',
      data: hexagons,
      filled: true,
      stroked: true,
      
      // Stroke configuration
      getLineColor: [255, 255, 255, 80],
      getLineWidth: 1,
      lineWidthMinPixels: 0.5,
      
      // ВИПРАВЛЕНО: Color mapping function
      getFillColor: (d) => getFixedFillColor(d, metric),
      
      // Hover effects
      autoHighlight: true,
      highlightColor: [255, 255, 255, 100],
      
      // Picking
      pickable: true,
      onHover: handleHover,
      
      // ВИПРАВЛЕНО: Performance optimization з правильними triggers
      updateTriggers: {
        getFillColor: [metric, hexagons.length, currentResolution], // ВИПРАВЛЕНО: Додано всі релевантні тригери
        getLineColor: [metric],
        getLineWidth: [viewState.zoom]
      },
      
      // Transitions
      transitions: {
        getFillColor: 300
      }
    });
  }, [getCurrentHexagons, metric, handleHover, currentResolution, viewState.zoom]); // ВИПРАВЛЕНО: Правильні dependencies
  
  // ===============================================
  // RENDER COMPONENTS
  // ===============================================
  
  // ВИПРАВЛЕНО: Progress component зникає після Phase 1
  const ProgressComponent = () => {
    // КЛЮЧОВЕ ВИПРАВЛЕННЯ: Ховаємо прогрес після Phase 1, а не після повного завантаження
    const shouldShowProgress = useSmartLoading ? 
      !smartHook?.tier1Complete :  // Ховаємо коли Phase 1 завершено
      !legacyHook?.isPreloaded;   // Для legacy - ховаємо коли все завантажено
    
    if (!shouldShowProgress) return null;
    
    if (useSmartLoading) {
      const progress = smartHook?.progress || 0;
      const currentStep = smartHook?.currentStep || 'Завантаження...';
      
      return (
        <div style={{
          position: 'absolute',
          top: '50%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          background: 'rgba(0,0,0,0.8)',
          color: 'white',
          padding: '20px',
          borderRadius: '10px',
          textAlign: 'center',
          zIndex: 1000
        }}>
          <div style={{ fontSize: '18px', marginBottom: '10px' }}>
            {currentStep}
          </div>
          <div style={{ 
            width: '300px', 
            height: '6px', 
            background: '#333',
            borderRadius: '3px',
            overflow: 'hidden'
          }}>
            <div style={{
              width: `${progress}%`,
              height: '100%',
              background: 'linear-gradient(90deg, #4CAF50, #45a049)',
              transition: 'width 0.3s ease'
            }} />
          </div>
          <div style={{ fontSize: '14px', marginTop: '10px', opacity: 0.8 }}>
            {Math.round(progress)}%
          </div>
        </div>
      );
    } else {
      return (
        <PreloadProgressBar
          overallProgress={legacyHook?.overallProgress || 0}
          currentStep={legacyHook?.currentStep || 'Завантаження...'}
          completedRequests={legacyHook?.completedRequests || 0}
          isPreloaded={legacyHook?.isPreloaded || false}
          preloadError={legacyHook?.preloadError || null}
        />
      );
    }
  };
  
  // Strategy toggle
  const StrategyToggle = () => (
    <div style={{
      position: 'absolute',
      top: '20px',
      left: '20px',
      background: 'rgba(255, 255, 255, 0.95)',
      padding: '10px',
      borderRadius: '8px',
      boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
      zIndex: 1000
    }}>
      <div style={{ marginBottom: '8px', fontSize: '12px', fontWeight: 'bold' }}>
        Loading Strategy: <strong>{useSmartLoading ? 'Smart' : 'Legacy'}</strong>
      </div>
      <button
        onClick={toggleSmartLoading}
        style={{
          background: useSmartLoading ? '#32FF7E' : '#FF6B6B',
          color: 'white',
          border: 'none',
          padding: '6px 12px',
          borderRadius: '4px',
          fontSize: '12px',
          cursor: 'pointer'
        }}
      >
        Switch to {useSmartLoading ? 'Legacy' : 'Smart'} Loading
      </button>
    </div>
  );
  
  // Resolution control
  const ResolutionControl = () => (
    <div style={{
      position: 'absolute',
      top: '20px',
      right: '20px',
      background: 'rgba(255, 255, 255, 0.95)',
      padding: '15px',
      borderRadius: '8px',
      boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
      minWidth: '200px',
      zIndex: 1000
    }}>
      <div style={{ marginBottom: '10px', fontSize: '14px', fontWeight: 'bold' }}>
        Resolution Control
      </div>
      
      <div style={{ marginBottom: '10px' }}>
        <label style={{ display: 'flex', alignItems: 'center', fontSize: '12px' }}>
          <input
            type="checkbox"
            checked={autoResolution}
            onChange={(e) => setAutoResolution(e.target.checked)}
            style={{ marginRight: '8px' }}
          />
          Auto Resolution (Zoom: {viewState.zoom.toFixed(1)})
        </label>
      </div>
      
      {!autoResolution && (
        <div style={{ marginBottom: '10px' }}>
          <label style={{ fontSize: '12px', display: 'block', marginBottom: '5px' }}>
            Manual Resolution:
          </label>
          <select
            value={manualResolution}
            onChange={(e) => setManualResolution(parseInt(e.target.value))}
            style={{ width: '100%', padding: '4px' }}
          >
            <option value={7}>H3-7 (Overview)</option>
            <option value={8}>H3-8 (District)</option>
            <option value={9}>H3-9 (Neighborhood)</option>
            <option value={10}>H3-10 (Street)</option>
          </select>
        </div>
      )}
      
      <div style={{ fontSize: '11px', color: '#666' }}>
        Current: H3-{currentResolution}<br />
        {getResolutionDescription(currentResolution)}
      </div>
    </div>
  );
  
  // Data info panel
  const DataInfoPanel = () => {
    if (!isDataReady) return null;
    
    const hexagons = getCurrentHexagons();
    const debugInfo = useSmartLoading ? (smartHook?.debugInfo || {}) : {};
    
    return (
      <div style={{
        position: 'absolute',
        bottom: '20px',
        left: '20px',
        background: 'rgba(0,0,0,0.8)',
        color: 'white',
        padding: '15px',
        borderRadius: '8px',
        fontSize: '12px',
        zIndex: 1000,
        minWidth: '200px'
      }}>
        <div style={{ 
          fontSize: '14px', 
          fontWeight: 'bold',
          marginBottom: '8px',
          textTransform: 'capitalize'
        }}>
          📊 Поточна метрика: {metric === 'opportunity' ? 'Можливості' : 'Конкуренція'}
        </div>
        
        <div>🔢 {hexagons.length} гексагонів</div>
        <div>🗂️ H3-{currentResolution} роздільність</div>
        <div>🔄 {useSmartLoading ? 'Smart' : 'Legacy'} режим</div>
        {debugInfo.cacheSize && (
          <div>💾 Cache: {debugInfo.cacheSize} datasets</div>
        )}
      </div>
    );
  };
  
  // ВИПРАВЛЕНИЙ Color Legend Component
  const ColorLegend = () => {
    if (!isDataReady) return null;
    
    const scheme = H3_COLOR_SCHEMES[metric] || H3_COLOR_SCHEMES.opportunity;
    const metricName = metric === 'opportunity' ? 'Можливості' : 'Конкуренція';
    
    // ВИПРАВЛЕНО: Використовуємо правильні назви полів
    let legendItems = [];
    
    if (metric === 'opportunity') {
      legendItems = [
        { label: 'Низький', color: scheme.low, range: '0-33%' },
        { label: 'Середній', color: scheme.medium, range: '33-66%' },
        { label: 'Високий', color: scheme.high, range: '66-100%' }
      ];
    } else {
      legendItems = [
        { label: 'Низький', color: scheme.low, range: '0-25%' },
        { label: 'Середній', color: scheme.medium, range: '25-50%' },
        { label: 'Високий', color: scheme.high, range: '50-75%' },
        { label: 'Максимальний', color: scheme.maximum, range: '75-100%' }
      ];
    }
    
    return (
      <div style={{
        position: 'absolute',
        bottom: '20px',
        right: '20px',
        background: 'rgba(255, 255, 255, 0.95)',
        padding: '15px',
        borderRadius: '8px',
        boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
        fontSize: '12px',
        zIndex: 1000,
        minWidth: '180px'
      }}>
        <div style={{ 
          fontWeight: 'bold', 
          marginBottom: '10px',
          fontSize: '14px'
        }}>
          🎨 {metricName}
        </div>
        
        {legendItems.map((item, index) => (
          <div key={index} style={{
            display: 'flex',
            alignItems: 'center',
            marginBottom: '6px'
          }}>
            <div style={{
              width: '20px',
              height: '15px',
              // ВИПРАВЛЕНО: Перевіряємо що color існує перед використанням join()
              backgroundColor: item.color ? `rgb(${item.color.join(',')})` : 'rgb(200,200,200)',
              border: '1px solid #ccc',
              marginRight: '8px',
              borderRadius: '3px'
            }} />
            <div style={{ flex: 1 }}>
              <div style={{ fontWeight: '500' }}>{item.label}</div>
              <div style={{ fontSize: '10px', color: '#666' }}>{item.range}</div>
            </div>
          </div>
        ))}
      </div>
    );
  };
  
  // Error display
  const ErrorDisplay = () => {
    const hasError = useSmartLoading ? (smartHook?.hasError || false) : (legacyHook?.preloadError || false);
    const errorMessage = useSmartLoading ? (smartHook?.error || '') : (legacyHook?.preloadError || '');
    
    if (!hasError) return null;
    
    return (
      <div style={{
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        background: 'rgba(255,0,0,0.9)',
        color: 'white',
        padding: '20px',
        borderRadius: '10px',
        textAlign: 'center',
        zIndex: 1000,
        maxWidth: '400px'
      }}>
        <div style={{ fontSize: '18px', marginBottom: '10px' }}>
          ⚠️ Помилка завантаження
        </div>
        <div style={{ fontSize: '14px', marginBottom: '15px' }}>
          {errorMessage}
        </div>
        {useSmartLoading && smartHook?.retryLoading && (
          <button
            onClick={smartHook.retryLoading}
            style={{
              background: 'white',
              color: 'red',
              border: 'none',
              padding: '8px 16px',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            Спробувати знову
          </button>
        )}
      </div>
    );
  };
  
  // ===============================================
  // MAIN RENDER
  // ===============================================
  
  return (
    <div style={{ position: 'relative', width: '100vw', height: '100vh' }}>
      {/* ВИПРАВЛЕНО: DeckGL з proper base map */}
      <DeckGL
        initialViewState={viewState}
        controller={true}
        layers={[hexagonLayer].filter(Boolean)}
        onViewStateChange={handleViewStateChange}
        onHover={handleHover}
      >
        {/* ВИПРАВЛЕНО: Added base map */}
        <Map
          mapLib={maplibregl}
          mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
          preventStyleDiffing={true}
        />
      </DeckGL>
      
      {/* UI Components */}
      <StrategyToggle />
      <ResolutionControl />
      <DataInfoPanel />
      <ColorLegend />
      
      {/* Metric Switcher */}
      <div style={{
        position: 'absolute',
        bottom: '180px',
        right: '20px',
        zIndex: 1000
      }}>
        <MetricSwitcher
          currentMetric={metric}
          onMetricChange={handleMetricChange}
          disabled={!isDataReady}
        />
      </div>
      
      {/* Progress/Loading */}
      <ProgressComponent />
      
      {/* Error Display */}
      <ErrorDisplay />
      
      {/* Hover Tooltip */}
      {hoveredObject && (
        <HoverTooltip
          object={hoveredObject}
          x={mousePosition.x}
          y={mousePosition.y}
          metric={metric}
        />
      )}
    </div>
  );
};

export default H3MapVisualization;