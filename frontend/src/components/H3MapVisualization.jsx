// frontend/src/components/H3MapVisualization.jsx
// Complete Fixed version з правильною color mapping та прозорістю

import React, { useState, useEffect, useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { MapView } from '@deck.gl/core';
import 'maplibre-gl/dist/maplibre-gl.css';

// Існуючі компоненти (залишаємо без змін)
import MetricSwitcher from './H3Visualization/components/controls/MetricSwitcher';
import PreloadProgressBar from './H3Visualization/components/ui/PreloadProgressBar';
import HoverTooltip from './H3Visualization/components/ui/HoverTooltip';
import { H3_COLOR_SCHEMES } from './H3Visualization/utils/colorSchemes';

// Legacy import (fallback)
import usePreloadedH3Data from './H3Visualization/hooks/usePreloadedH3Data';

// NEW: Smart Loading imports
import useSmartH3Loading from './H3Visualization/hooks/useSmartH3Loading';
import SmartLoadingIndicator, { 
  MiniProgressIndicator, 
  LoadingStatesDebugger, 
  ProgressBarCompat 
} from './H3Visualization/components/ui/SmartLoadingIndicator';

// ===============================================
// HELPER FUNCTIONS (existing)
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
// FIXED COLOR MAPPING FUNCTION
// ===============================================

const getFixedFillColor = (d, metric) => {
  try {
    const scheme = H3_COLOR_SCHEMES[metric] || H3_COLOR_SCHEMES.opportunity;
    
    // ВИПРАВЛЕННЯ: Використовуємо правильні назви полів з API
    let value = null;
    
    // Для opportunity метрики
    if (metric === 'opportunity') {
      value = d.market_opportunity_score || d.display_value;
    }
    // Для competition метрики  
    else if (metric === 'competition') {
      value = d.competition_intensity || d.display_value;
    }
    // Fallback для інших метрик
    else {
      value = d.display_value || d.market_opportunity_score || d.competition_intensity;
    }
    
    // Debug логування (рідко)
    if (Math.random() < 0.001) {
      console.log('🎨 Color mapping debug:', {
        metric,
        availableFields: Object.keys(d),
        market_opportunity_score: d.market_opportunity_score,
        competition_intensity: d.competition_intensity,
        display_value: d.display_value,
        selectedValue: value
      });
    }
    
    // Якщо значення відсутнє, використовуємо 0
    if (value === null || value === undefined) {
      value = 0;
      console.log(`⚠️ No value found for metric '${metric}', using fallback 0`);
    }
    
    // Конвертуємо в число
    if (typeof value === 'string') {
      value = parseFloat(value) || 0;
    }
    
    // Нормалізуємо до 0-1 діапазону
    value = Math.max(0, Math.min(1, value));
    
    // Color mapping з ПІДВИЩЕНОЮ ПРОЗОРІСТЮ (знижені alpha values)
    if (value <= 0.1) return [...scheme.veryLow, 120];    // Дуже прозорі для низьких значень
    if (value <= 0.3) return [...scheme.low, 140];       // Низька прозорість
    if (value <= 0.5) return [...scheme.medium, 160];    // Середня прозорість
    if (value <= 0.7) return [...scheme.high, 180];      // Вища прозорість
    return [...scheme.veryHigh, 200];                    // Максимальна прозорість (але не повна)
    
  } catch (error) {
    console.error('❌ Error in color mapping:', error, d);
    return [255, 165, 0, 100]; // Помаранчевий fallback з прозорістю
  }
};

// ===============================================
// ENHANCED RESOLUTION CONTROL
// ===============================================

const EnhancedResolutionControl = ({ 
  currentResolution, 
  autoMode, 
  onAutoModeChange,
  onManualResolutionChange,
  currentZoom,
  loading,
  error,
  loadingStrategy = 'smart',
  onStrategyChange = null
}) => {
  return (
    <div style={{
      position: 'absolute',
      top: '20px',
      right: '20px',
      backgroundColor: 'rgba(255, 255, 255, 0.98)',
      padding: '15px',
      borderRadius: '12px',
      boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
      minWidth: '220px',
      backdropFilter: 'blur(10px)',
      zIndex: 1000
    }}>
      <h4 style={{ 
        margin: '0 0 12px 0', 
        fontSize: '16px', 
        fontWeight: '600',
        color: '#1a1a1a'
      }}>
        🎚️ Рівень деталізації H3
      </h4>
      
      {/* Loading Strategy Selector */}
      {onStrategyChange && (
        <div style={{ marginBottom: '12px' }}>
          <label style={{ 
            display: 'block', 
            fontSize: '12px', 
            fontWeight: '500',
            marginBottom: '6px',
            color: '#666'
          }}>
            Стратегія завантаження:
          </label>
          <select 
            value={loadingStrategy}
            onChange={(e) => onStrategyChange(e.target.value)}
            style={{
              width: '100%',
              padding: '4px 8px',
              borderRadius: '6px',
              border: '1px solid #ddd',
              fontSize: '12px'
            }}
          >
            <option value="smart">🚀 Smart Loading (новий)</option>
            <option value="legacy">📊 Legacy Loading (старий)</option>
          </select>
        </div>
      )}
      
      <div style={{ marginBottom: '12px' }}>
        <label style={{ 
          display: 'flex', 
          alignItems: 'center', 
          cursor: 'pointer',
          fontSize: '14px'
        }}>
          <input 
            type="checkbox" 
            checked={autoMode}
            onChange={(e) => onAutoModeChange(e.target.checked)}
            style={{ marginRight: '8px' }}
          />
          <span>Автоматичний вибір при зумі</span>
        </label>
      </div>
      
      <div style={{
        padding: '10px',
        backgroundColor: loading ? '#fff3e0' : error ? '#ffebee' : '#f5f5f5',
        borderRadius: '8px',
        fontSize: '13px'
      }}>
        <div style={{ 
          display: 'flex', 
          justifyContent: 'space-between',
          marginBottom: '5px'
        }}>
          <span><strong>Поточний:</strong> H3-{currentResolution}</span>
          <span><strong>Zoom:</strong> {currentZoom.toFixed(1)}</span>
        </div>
        <div style={{ color: '#666', fontSize: '12px' }}>
          {getResolutionDescription(currentResolution)}
        </div>
        
        {/* Loading strategy indicator */}
        <div style={{ 
          marginTop: '8px', 
          padding: '4px 8px',
          background: loadingStrategy === 'smart' ? '#e3f2fd' : '#fff3e0',
          borderRadius: '4px',
          fontSize: '11px',
          color: loadingStrategy === 'smart' ? '#1976d2' : '#f57c00'
        }}>
          {loadingStrategy === 'smart' ? '🚀 Smart Loading активний' : '📊 Legacy Loading активний'}
        </div>
        
        {loading && (
          <div style={{ marginTop: '5px', color: '#ff9800' }}>
            ⏳ Завантаження...
          </div>
        )}
        {error && (
          <div style={{ marginTop: '5px', color: '#f44336' }}>
            ❌ {error}
          </div>
        )}
      </div>
      
      {!autoMode && (
        <div style={{ marginTop: '12px' }}>
          <label style={{ 
            display: 'block', 
            fontSize: '12px', 
            marginBottom: '6px' 
          }}>
            Ручний вибір resolution:
          </label>
          <select 
            value={currentResolution}
            onChange={(e) => onManualResolutionChange(parseInt(e.target.value))}
            style={{
              width: '100%',
              padding: '6px',
              borderRadius: '6px',
              border: '1px solid #ddd'
            }}
          >
            <option value={7}>H3-7 (область)</option>
            <option value={8}>H3-8 (район)</option>
            <option value={9}>H3-9 (квартал)</option>
            <option value={10}>H3-10 (вулиця)</option>
          </select>
        </div>
      )}
    </div>
  );
};

// ===============================================
// MAIN COMPONENT
// ===============================================

const H3MapVisualization = () => {
  // ===============================================
  // LOADING STRATEGY STATE
  // ===============================================
  
  // Feature flag для Smart Loading з localStorage persistence
  const [loadingStrategy, setLoadingStrategy] = useState(() => {
    const stored = localStorage.getItem('h3-loading-strategy');
    return stored || 'smart';
  });
  
  // Debug mode для development
  const [debugMode, setDebugMode] = useState(() => {
    return localStorage.getItem('h3-debug-mode') === 'true' || 
           process.env.NODE_ENV === 'development';
  });
  
  // Збереження стратегії в localStorage
  useEffect(() => {
    localStorage.setItem('h3-loading-strategy', loadingStrategy);
    console.log(`🔄 Loading strategy changed to: ${loadingStrategy}`);
  }, [loadingStrategy]);
  
  // ===============================================
  // EXISTING STATE MANAGEMENT
  // ===============================================
  
  const [metric, setMetric] = useState('opportunity');
  const [autoResolution, setAutoResolution] = useState(true);
  const [manualResolution, setManualResolution] = useState(7);
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [viewState, setViewState] = useState({
    longitude: 30.5234,
    latitude: 50.4501,
    zoom: 7,
    pitch: 0,
    bearing: 0
  });

  // ===============================================
  // DUAL LOADING HOOKS
  // ===============================================
  
  // Smart Loading hook (NEW)
  const smartHook = useSmartH3Loading(viewState, metric);
  
  // Legacy hook (FALLBACK)
  const legacyHook = usePreloadedH3Data(1000000);
  
  // Strategy selection
  const useSmartLoading = loadingStrategy === 'smart';
  const activeHook = useSmartLoading ? smartHook : legacyHook;
  
  // ===============================================
  // DERIVED STATE
  // ===============================================
  
  const debouncedViewState = useDebounce(viewState, 300);
  
  const currentResolution = useMemo(() => {
    return autoResolution ? getOptimalResolution(viewState.zoom) : manualResolution;
  }, [autoResolution, viewState.zoom, manualResolution]);

  // ===============================================
  // DATA LOADING STATUS
  // ===============================================
  
  const isDataReady = useMemo(() => {
    if (useSmartLoading) {
      return smartHook.isBasicReady;
    } else {
      return legacyHook.isPreloaded;
    }
  }, [useSmartLoading, smartHook.isBasicReady, legacyHook.isPreloaded]);
  
  const loadingProgress = useMemo(() => {
    if (useSmartLoading) {
      const { tier1, tier2, tier3 } = smartHook.loadingTiers;
      if (tier3.status === 'completed') return 100;
      if (tier2.status === 'completed') return 80;
      if (tier1.status === 'completed') return 40;
      return tier1.progress * 0.4;
    } else {
      return legacyHook.overallProgress;
    }
  }, [useSmartLoading, smartHook.loadingTiers, legacyHook.overallProgress]);
  
  const currentStep = useMemo(() => {
    if (useSmartLoading) {
      return smartHook.currentActivity;
    } else {
      return legacyHook.currentStep;
    }
  }, [useSmartLoading, smartHook.currentActivity, legacyHook.currentStep]);

  // ===============================================
  // DATA ACCESS
  // ===============================================
  
  const getCurrentHexagons = useMemo(() => {
    if (!isDataReady) return [];
    
    try {
      if (useSmartLoading) {
        return smartHook.getVisibleHexagons(metric, currentResolution, debouncedViewState);
      } else {
        return legacyHook.getVisibleHexagons(metric, currentResolution, debouncedViewState);
      }
    } catch (error) {
      console.error('Error getting hexagons:', error);
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
    
    if (useSmartLoading && smartHook.prioritizeMetric) {
      smartHook.prioritizeMetric(newMetric);
    }
  };
  
  const handleStrategyChange = (newStrategy) => {
    setLoadingStrategy(newStrategy);
    
    if (newStrategy === 'smart' && smartHook.updateViewport) {
      smartHook.updateViewport(viewState);
    }
  };
  
  const handleViewStateChange = (newViewState) => {
    setViewState(newViewState);
    
    if (useSmartLoading && smartHook.updateViewport) {
      smartHook.updateViewport(newViewState);
    }
  };

  // ===============================================
  // DECKGL LAYERS WITH FIXED COLOR MAPPING
  // ===============================================
  
  const hexagonLayer = useMemo(() => {
    if (getCurrentHexagons.length === 0) {
      console.log('⚠️ No hexagons available for rendering');
      return null;
    }

    console.log(`🗺️ Rendering ${getCurrentHexagons.length} hexagons with metric '${metric}'`);

    return new GeoJsonLayer({
      id: 'h3-hexagons-fixed',
      data: getCurrentHexagons,
      filled: true,
      stroked: true,
      
      // Stroke (outline) configuration
      getLineColor: [255, 255, 255, 80], // Дуже прозорий білий outline
      getLineWidth: d => {
        const zoom = viewState.zoom;
        if (zoom < 8) return 0.5;
        if (zoom < 10) return 1;
        if (zoom < 12) return 1.5;
        return 2;
      },
      lineWidthMinPixels: 0.5,
      lineWidthMaxPixels: 3,
      
      // ВИПРАВЛЕНА fill configuration
      getFillColor: (d) => getFixedFillColor(d, metric),
      
      // Interaction
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 255, 255, 60], // Дуже прозорий highlight
      
      // Performance optimizations
      updateTriggers: {
        getFillColor: [metric, getCurrentHexagons.length],
        getLineWidth: [viewState.zoom]
      },
      
      // Hover handling
      onHover: (info, event) => {
        if (info.object) {
          setHoveredObject(info.object);
          setMousePosition({ x: info.x, y: info.y });
          
          // Debug логування hover info
          if (Math.random() < 0.1) {
            console.log('🖱️ Hover info:', {
              h3_index: info.object.h3_index,
              market_opportunity_score: info.object.market_opportunity_score,
              competition_intensity: info.object.competition_intensity,
              display_value: info.object.display_value
            });
          }
        } else {
          setHoveredObject(null);
        }
      }
    });
  }, [getCurrentHexagons, metric, viewState.zoom]);

  // ===============================================
  // LOADING UI LOGIC
  // ===============================================
  
  const showLoadingUI = !isDataReady;
  const showProgressIndicator = useSmartLoading ? 
    !smartHook.isCompletelyLoaded : 
    !legacyHook.isPreloaded;

  // ===============================================
  // RENDER
  // ===============================================
  
  return (
    <div style={{ width: '100vw', height: '100vh', position: 'relative' }}>
      
      {/* LOADING UI */}
      {showLoadingUI && (
        <>
          {useSmartLoading ? (
            <SmartLoadingIndicator 
              loadingTiers={smartHook.loadingTiers}
              isBasicReady={smartHook.isBasicReady}
              isFullyFunctional={smartHook.isFullyFunctional}
              isCompletelyLoaded={smartHook.isCompletelyLoaded}
              performanceMetrics={smartHook.performanceMetrics}
              currentActivity={smartHook.currentActivity}
              debugMode={debugMode}
            />
          ) : (
            <ProgressBarCompat 
              overallProgress={legacyHook.overallProgress}
              currentStep={legacyHook.currentStep}
              isLegacyMode={true}
            />
          )}
        </>
      )}
      
      {/* MAIN MAP */}
      {isDataReady && (
        <>
          <DeckGL
            viewState={viewState}
            onViewStateChange={({ viewState: newViewState }) => {
              console.log(`🗺️ Viewport changed: zoom ${newViewState.zoom.toFixed(2)}`);
              handleViewStateChange(newViewState);
            }}
            controller={{
              dragPan: true,
              dragRotate: false,
              doubleClickZoom: true,
              touchZoom: true,
              touchRotate: false,
              keyboard: true,
              scrollZoom: true
            }}
            layers={[hexagonLayer].filter(Boolean)}
            width="100%"
            height="100%"
            style={{ position: 'relative' }}
            views={new MapView({ 
              id: 'map',
              controller: true
            })}
            useDevicePixels={window.devicePixelRatio || 1}
            onLoad={() => console.log('🗺️ DeckGL loaded successfully')}
            onError={(error) => console.error('❌ DeckGL error:', error)}
          >
            <Map
              mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
              styleDiffing={false}
              reuseMaps={true}
            />
          </DeckGL>

          {/* Controls */}
          <MetricSwitcher
            currentMetric={metric}
            onMetricChange={handleMetricChange}
            loading={showProgressIndicator}
          />

          <EnhancedResolutionControl
            currentResolution={currentResolution}
            autoMode={autoResolution}
            onAutoModeChange={setAutoResolution}
            onManualResolutionChange={setManualResolution}
            currentZoom={viewState.zoom}
            loading={showProgressIndicator}
            error={useSmartLoading ? 
              smartHook.loadingTiers.tier1.error : 
              legacyHook.preloadError
            }
            loadingStrategy={loadingStrategy}
            onStrategyChange={handleStrategyChange}
          />

          {/* Hover Tooltip */}
          <HoverTooltip
            hoveredObject={hoveredObject}
            mousePosition={mousePosition}
            metric={metric}
            resolution={currentResolution}
          />
        </>
      )}
      
      {/* PROGRESS INDICATORS */}
      {showProgressIndicator && isDataReady && (
        <div style={{
          position: 'absolute',
          bottom: '20px',
          left: '20px',
          zIndex: 1000
        }}>
          {useSmartLoading ? (
            <MiniProgressIndicator 
              loadingTiers={smartHook.loadingTiers}
              isBasicReady={smartHook.isBasicReady}
              isFullyFunctional={smartHook.isFullyFunctional}
              isCompletelyLoaded={smartHook.isCompletelyLoaded}
            />
          ) : (
            <div style={{
              background: 'rgba(255, 255, 255, 0.9)',
              padding: '8px 12px',
              borderRadius: '8px',
              fontSize: '12px',
              fontWeight: '500',
              boxShadow: '0 2px 10px rgba(0,0,0,0.1)'
            }}>
              📊 Legacy Loading: {Math.round(legacyHook.overallProgress)}%
            </div>
          )}
        </div>
      )}
      
      {/* DEBUG PANEL */}
      {debugMode && useSmartLoading && (
        <LoadingStatesDebugger 
          loadingTiers={smartHook.loadingTiers}
          performanceMetrics={smartHook.performanceMetrics}
          debugInfo={smartHook.debugInfo}
        />
      )}
      
      {/* STATISTICS PANEL */}
      {isDataReady && (
        <div style={{
          position: 'absolute',
          bottom: '20px',
          right: '20px',
          background: 'rgba(255, 255, 255, 0.95)',
          padding: '12px',
          borderRadius: '8px',
          fontSize: '11px',
          boxShadow: '0 2px 10px rgba(0,0,0,0.1)',
          minWidth: '200px',
          zIndex: 1000
        }}>
          <div style={{ fontWeight: '600', marginBottom: '6px', color: '#1a1a1a' }}>
            📊 Статистика даних
          </div>
          
          {(() => {
            const stats = activeHook.getStats ? activeHook.getStats() : {};
            return (
              <>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                  <span>Видимих гексагонів:</span>
                  <span style={{ fontWeight: '500' }}>{getCurrentHexagons.length}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                  <span>Завантажених datasets:</span>
                  <span style={{ fontWeight: '500' }}>{stats.loadedDatasets || 0}/{stats.totalDatasets || 8}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                  <span>Загалом гексагонів:</span>
                  <span style={{ fontWeight: '500' }}>{stats.totalHexagons?.toLocaleString() || 0}</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '3px' }}>
                  <span>Стратегія:</span>
                  <span style={{ 
                    fontWeight: '500',
                    color: useSmartLoading ? '#1976d2' : '#f57c00'
                  }}>
                    {useSmartLoading ? 'Smart' : 'Legacy'}
                  </span>
                </div>
                
                {/* Smart Loading specific metrics */}
                {useSmartLoading && smartHook.performanceMetrics && (
                  <>
                    <hr style={{ margin: '6px 0', border: 'none', borderTop: '1px solid #eee' }} />
                    <div style={{ fontSize: '10px', color: '#666' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <span>Time to Interactive:</span>
                        <span>{smartHook.performanceMetrics.timeToInteractive || 'N/A'}ms</span>
                      </div>
                      {smartHook.performanceMetrics.timeToFullyFunctional && (
                        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                          <span>Time to Functional:</span>
                          <span>{smartHook.performanceMetrics.timeToFullyFunctional}ms</span>
                        </div>
                      )}
                      <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                        <span>Data Utilization:</span>
                        <span>{Math.round(smartHook.dataUtilizationRate || 0)}%</span>
                      </div>
                    </div>
                  </>
                )}
              </>
            );
          })()}
        </div>
      )}
      
      {/* KEYBOARD SHORTCUTS HELP */}
      {debugMode && (
        <div style={{
          position: 'absolute',
          top: '50%',
          right: '20px',
          transform: 'translateY(-50%)',
          background: 'rgba(0, 0, 0, 0.8)',
          color: 'white',
          padding: '12px',
          borderRadius: '8px',
          fontSize: '11px',
          fontFamily: 'monospace',
          zIndex: 1000,
          maxWidth: '250px'
        }}>
          <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>
            ⌨️ Keyboard Shortcuts
          </div>
          <div>S - Toggle Smart/Legacy Loading</div>
          <div>D - Toggle Debug Mode</div>
          <div>R - Reload Data</div>
          <div>C - Clear Cache</div>
          <div>O - Switch to Opportunity</div>
          <div>P - Switch to Competition</div>
          <div>H - Toggle this Help</div>
        </div>
      )}
    </div>
  );
};

// ===============================================
// KEYBOARD SHORTCUTS HANDLER
// ===============================================

if (process.env.NODE_ENV === 'development') {
  let shortcutsEnabled = true;
  
  const handleKeyPress = (event) => {
    if (!shortcutsEnabled) return;
    
    const key = event.key.toLowerCase();
    
    switch (key) {
      case 's':
        const currentStrategy = localStorage.getItem('h3-loading-strategy') || 'smart';
        const newStrategy = currentStrategy === 'smart' ? 'legacy' : 'smart';
        localStorage.setItem('h3-loading-strategy', newStrategy);
        window.location.reload();
        break;
        
      case 'd':
        const currentDebug = localStorage.getItem('h3-debug-mode') === 'true';
        localStorage.setItem('h3-debug-mode', (!currentDebug).toString());
        window.location.reload();
        break;
        
      case 'r':
        window.location.reload();
        break;
        
      case 'c':
        localStorage.removeItem('h3-loading-strategy');
        localStorage.removeItem('h3-debug-mode');
        console.log('🧹 Cache cleared');
        window.location.reload();
        break;
        
      case 'h':
        shortcutsEnabled = !shortcutsEnabled;
        console.log(`⌨️ Keyboard shortcuts ${shortcutsEnabled ? 'enabled' : 'disabled'}`);
        break;
        
      default:
        break;
    }
  };
  
  document.addEventListener('keydown', handleKeyPress);
  
  console.log(`
🚀 H3MapVisualization Development Mode
🎨 FIXED Color Mapping:
- opportunity → market_opportunity_score
- competition → competition_intensity  
- Enhanced transparency for better visibility
- Real API field mapping

Current Strategy: ${localStorage.getItem('h3-loading-strategy') || 'smart'}
Debug Mode: ${localStorage.getItem('h3-debug-mode') === 'true'}
  `);
}

export default H3MapVisualization;