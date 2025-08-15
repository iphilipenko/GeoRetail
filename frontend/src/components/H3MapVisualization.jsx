import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { MapView } from '@deck.gl/core';
import 'maplibre-gl/dist/maplibre-gl.css';

// Імпорт модульних компонентів
import MetricSwitcher from './H3Visualization/components/controls/MetricSwitcher';
import PreloadProgressBar from './H3Visualization/components/ui/PreloadProgressBar';
import usePreloadedH3Data from './H3Visualization/hooks/usePreloadedH3Data';
import { H3_COLOR_SCHEMES } from './H3Visualization/utils/colorSchemes';

// Custom hook for debouncing values
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

// Helper function to determine optimal H3 resolution based on zoom
const getOptimalResolution = (zoom) => {
  if (zoom < 8) return 7;   // Найбільші гексагони (~5.16 км²) - Oblast overview
  if (zoom < 10) return 8;  // Великі гексагони (~0.74 км²) - District level
  if (zoom < 12) return 9;  // Середні гексагони (~0.105 км²) - City level
  return 10;                // Найменші гексагони (~0.015 км²) - Neighborhood detail
};

// Helper function to get resolution description
const getResolutionDescription = (resolution) => {
  const descriptions = {
    7: "Огляд області - великі гексагони",
    8: "Рівень району - середні гексагони", 
    9: "Рівень кварталу - детальні гексагони",
    10: "Рівень вулиці - найдетальніші"
  };
  return descriptions[resolution] || "";
};

// СТАРИЙ hook useH3Data - ЗАМІЩЕНИЙ на preloaded систему
// const useH3Data = (metric, resolution, limit) => { ... }

// ОНОВЛЕНІ кольорові схеми (винесені в константи, тут тільки alpha логіка)
const getColorWithDynamicAlpha = (scheme, category, alpha) => {
  const baseColor = H3_COLOR_SCHEMES[scheme]?.[category] || [200, 200, 200];
  return [baseColor[0], baseColor[1], baseColor[2], alpha];
};

// Resolution Control Component
const ResolutionControl = ({ 
  currentResolution, 
  autoMode, 
  onAutoModeChange,
  onManualResolutionChange,
  currentZoom,
  loading,
  error 
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
      
      {/* Перемикач авто/ручний режим */}
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
      
      {/* Індикатор поточного resolution */}
      <div style={{
        padding: '10px',
        backgroundColor: loading ? '#fff3e0' : error ? '#ffebee' : '#f0f8ff',
        borderRadius: '6px',
        marginBottom: '12px',
        border: `1px solid ${loading ? '#ff9800' : error ? '#f44336' : '#2196f3'}`
      }}>
        <div style={{ 
          fontSize: '12px', 
          color: '#666', 
          marginBottom: '4px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}>
          <span>Поточний рівень:</span>
          {loading && (
            <div style={{
              width: '12px',
              height: '12px',
              border: '2px solid #ff9800',
              borderTopColor: 'transparent',
              borderRadius: '50%',
              animation: 'spin 1s linear infinite'
            }}></div>
          )}
        </div>
        <div style={{ 
          fontSize: '18px', 
          fontWeight: 'bold', 
          color: loading ? '#ff9800' : error ? '#f44336' : '#2196f3'
        }}>
          H3-{currentResolution}
        </div>
        <div style={{ 
          fontSize: '11px', 
          color: '#666', 
          marginTop: '4px',
          lineHeight: '1.3'
        }}>
          {getResolutionDescription(currentResolution)}
        </div>
        
        {/* Повідомлення про помилку */}
        {error && (
          <div style={{
            fontSize: '11px',
            color: '#f44336',
            marginTop: '6px',
            padding: '4px',
            backgroundColor: 'rgba(244, 67, 54, 0.1)',
            borderRadius: '4px'
          }}>
            {error}
          </div>
        )}
      </div>
      
      {/* Ручний вибір (якщо не авто) */}
      {!autoMode && (
        <div style={{ marginBottom: '12px' }}>
          <label style={{ 
            fontSize: '12px', 
            color: '#666',
            display: 'block',
            marginBottom: '4px'
          }}>
            Виберіть рівень вручну:
          </label>
          <select 
            value={currentResolution}
            onChange={(e) => onManualResolutionChange(Number(e.target.value))}
            style={{
              width: '100%',
              padding: '8px',
              borderRadius: '6px',
              border: '1px solid #ddd',
              fontSize: '13px',
              backgroundColor: 'white'
            }}
          >
            <option value={7}>H3-7 (Область ~5 км²)</option>
            <option value={8}>H3-8 (Район ~0.7 км²)</option>
            <option value={9}>H3-9 (Квартал ~0.1 км²)</option>
            <option value={10}>H3-10 (Вулиця ~0.015 км²)</option>
          </select>
        </div>
      )}
      
      {/* Інфо про zoom */}
      <div style={{
        padding: '8px',
        backgroundColor: '#f8f8f8',
        borderRadius: '4px',
        fontSize: '12px',
        color: '#666',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center'
      }}>
        <span>🔍 Поточний zoom:</span>
        <strong>{currentZoom.toFixed(1)}</strong>
      </div>
      
      {autoMode && (
        <div style={{
          marginTop: '8px',
          fontSize: '11px',
          color: '#999',
          fontStyle: 'italic'
        }}>
          Рівень автоматично змінюється при зумі карти
        </div>
      )}
    </div>
  );
};

// Enhanced Tooltip Component
const HoverTooltip = ({ hoveredObject, x, y }) => {
  if (!hoveredObject) return null;

  const { h3_index, competition_intensity, transport_accessibility_score, 
          residential_indicator_score, commercial_activity_score, 
          market_opportunity_score, poi_total_count, retail_count, 
          competitor_count } = hoveredObject;

  // Визначаємо колір оцінки можливостей
  const getOpportunityColor = (score) => {
    if (score >= 0.7) return '#4caf50';
    if (score >= 0.4) return '#2196f3';
    return '#f44336';
  };

  return (
    <div 
      style={{
        position: 'absolute',
        left: x + 15,
        top: y + 15,
        zIndex: 1000,
        pointerEvents: 'none',
        backgroundColor: 'rgba(33, 33, 33, 0.95)',
        color: 'white',
        padding: '16px',
        borderRadius: '8px',
        fontSize: '13px',
        lineHeight: '1.5',
        boxShadow: '0 4px 20px rgba(0,0,0,0.3)',
        minWidth: '280px',
        backdropFilter: 'blur(10px)',
        border: '1px solid rgba(255,255,255,0.1)'
      }}
    >
      <div style={{
        borderBottom: '1px solid rgba(255,255,255,0.2)',
        paddingBottom: '8px',
        marginBottom: '10px'
      }}>
        <strong style={{fontSize: '14px'}}>
          🔍 Гексагон: {h3_index ? h3_index.slice(-7) : 'unknown'}
        </strong>
      </div>
      
      <div style={{display: 'grid', gap: '6px'}}>
        <div style={{display: 'flex', justifyContent: 'space-between'}}>
          <span>🏪 Загальні POI:</span>
          <strong>{poi_total_count || 0}</strong>
        </div>
        
        <div style={{display: 'flex', justifyContent: 'space-between'}}>
          <span>🛍️ Роздрібна торгівля:</span>
          <strong>{retail_count || 0}</strong>
        </div>
        
        <div style={{display: 'flex', justifyContent: 'space-between'}}>
          <span>⚔️ Конкуренти:</span>
          <strong style={{color: competitor_count > 5 ? '#ff9800' : '#4caf50'}}>
            {competitor_count || 0}
          </strong>
        </div>
        
        <div style={{
          margin: '8px 0',
          padding: '8px 0',
          borderTop: '1px solid rgba(255,255,255,0.2)',
          borderBottom: '1px solid rgba(255,255,255,0.2)'
        }}>
          <div style={{marginBottom: '8px'}}>
            <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '2px'}}>
              <span>Конкуренція:</span>
              <span>{((competition_intensity || 0) * 100).toFixed(0)}%</span>
            </div>
            <div style={{
              height: '4px',
              backgroundColor: 'rgba(255,255,255,0.1)',
              borderRadius: '2px',
              overflow: 'hidden'
            }}>
              <div style={{
                width: `${(competition_intensity || 0) * 100}%`,
                height: '100%',
                background: competition_intensity > 0.6 
                  ? 'linear-gradient(90deg, #f44336, #ff6b6b)'
                  : competition_intensity > 0.4
                  ? 'linear-gradient(90deg, #ff9800, #ffb74d)'
                  : 'linear-gradient(90deg, #4caf50, #81c784)',
                transition: 'width 0.3s ease'
              }}></div>
            </div>
          </div>
          
          <div style={{marginBottom: '8px'}}>
            <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '2px'}}>
              <span>Транспорт:</span>
              <span>{((transport_accessibility_score || 0) * 100).toFixed(0)}%</span>
            </div>
            <div style={{
              height: '4px',
              backgroundColor: 'rgba(255,255,255,0.1)',
              borderRadius: '2px',
              overflow: 'hidden'
            }}>
              <div style={{
                width: `${(transport_accessibility_score || 0) * 100}%`,
                height: '100%',
                background: 'linear-gradient(90deg, #2196f3, #64b5f6)',
                transition: 'width 0.3s ease'
              }}></div>
            </div>
          </div>
          
          <div style={{marginBottom: '8px'}}>
            <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '2px'}}>
              <span>Житлова забудова:</span>
              <span>{((residential_indicator_score || 0) * 100).toFixed(0)}%</span>
            </div>
            <div style={{
              height: '4px',
              backgroundColor: 'rgba(255,255,255,0.1)',
              borderRadius: '2px',
              overflow: 'hidden'
            }}>
              <div style={{
                width: `${(residential_indicator_score || 0) * 100}%`,
                height: '100%',
                background: 'linear-gradient(90deg, #9c27b0, #ba68c8)',
                transition: 'width 0.3s ease'
              }}></div>
            </div>
          </div>
          
          <div>
            <div style={{display: 'flex', justifyContent: 'space-between', marginBottom: '2px'}}>
              <span>Комерційна активність:</span>
              <span>{((commercial_activity_score || 0) * 100).toFixed(0)}%</span>
            </div>
            <div style={{
              height: '4px',
              backgroundColor: 'rgba(255,255,255,0.1)',
              borderRadius: '2px',
              overflow: 'hidden'
            }}>
              <div style={{
                width: `${(commercial_activity_score || 0) * 100}%`,
                height: '100%',
                background: 'linear-gradient(90deg, #ff5722, #ff8a65)',
                transition: 'width 0.3s ease'
              }}></div>
            </div>
          </div>
        </div>
        
        <div style={{
          backgroundColor: 'rgba(255,255,255,0.1)',
          padding: '8px',
          borderRadius: '4px',
          textAlign: 'center'
        }}>
          <div style={{fontSize: '11px', opacity: 0.8, marginBottom: '4px'}}>
            Оцінка можливостей
          </div>
          <div style={{
            fontSize: '20px',
            fontWeight: 'bold',
            color: getOpportunityColor(market_opportunity_score || 0)
          }}>
            {(market_opportunity_score || 0).toFixed(2)}
          </div>
        </div>
      </div>
    </div>
  );
};

// Main H3 Map Visualization Component з PRELOADING СИСТЕМОЮ
const H3MapVisualization = () => {
  const [metric, setMetric] = useState('opportunity');
  const [autoResolution, setAutoResolution] = useState(true);
  const [manualResolution, setManualResolution] = useState(8);
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [viewState, setViewState] = useState({
    longitude: 30.5234,
    latitude: 50.4501,
    zoom: 9,
    pitch: 0,
    bearing: 0
  });

  // 🚀 НОВИЙ PRELOADED H3 DATA SYSTEM
  const {
    isPreloaded,
    overallProgress,
    currentProgress,
    completedRequests,
    totalTasks,
    preloadError,
    currentStep,
    getVisibleHexagons,
    getCachedData,
    getStats,
    reloadData
  } = usePreloadedH3Data(1000000);

  // Визначаємо поточний resolution
  const currentResolution = autoResolution 
    ? getOptimalResolution(viewState.zoom)
    : manualResolution;
  
  // Debounce для smooth зуму
  const debouncedResolution = useDebounce(currentResolution, 300);
  const debouncedViewState = useDebounce(viewState, 150);

  // Статичний opacity для стабільності
  const staticOpacity = 150;

  // 🎯 VIEWPORT CULLING - отримуємо тільки видимі гексагони
  const visibleHexagons = useMemo(() => {
    if (!isPreloaded) return [];
    
    return getVisibleHexagons(metric, debouncedResolution, debouncedViewState);
  }, [isPreloaded, metric, debouncedResolution, debouncedViewState, getVisibleHexagons]);

  // Process data з viewport culling
  const geoJsonData = useMemo(() => {
    if (!visibleHexagons.length) {
      return { type: 'FeatureCollection', features: [] };
    }
    
    const features = visibleHexagons.map(hex => {
      const baseColor = H3_COLOR_SCHEMES[metric][hex.display_category] || [200, 200, 200];
      const colorWithAlpha = [baseColor[0], baseColor[1], baseColor[2], staticOpacity];
      
      return {
        type: 'Feature',
        properties: {
          ...hex,
          color: colorWithAlpha
        },
        geometry: hex.geometry
      };
    });
    
    return {
      type: 'FeatureCollection',
      features
    };
  }, [visibleHexagons, metric, staticOpacity]);

  // Статистика для debug і інформаційної панелі
  const stats = useMemo(() => {
    if (!isPreloaded) return { loadedDatasets: 0, totalHexagons: 0 };
    
    return getStats();
  }, [isPreloaded, getStats]);

  // Update viewport when first load (тільки один раз)
  useEffect(() => {
    if (!isPreloaded || visibleHexagons.length === 0) return;

    // Встановлюємо viewport тільки при першому завантаженні
    const hasSetInitialView = sessionStorage.getItem('h3-initial-view-set');
    if (hasSetInitialView) return;

    try {
      const allCoords = visibleHexagons.flatMap(hex => 
        hex.geometry?.coordinates?.[0] || []
      ).filter(coord => coord && coord.length === 2);

      if (allCoords.length === 0) return;

      const lons = allCoords.map(c => c[0]);
      const lats = allCoords.map(c => c[1]);
      
      const minLon = Math.min(...lons);
      const maxLon = Math.max(...lons);
      const minLat = Math.min(...lats);
      const maxLat = Math.max(...lats);
      
      const centerLon = (minLon + maxLon) / 2;
      const centerLat = (minLat + maxLat) / 2;
      
      setViewState(prev => ({
        ...prev,
        longitude: centerLon,
        latitude: centerLat,
        zoom: 8
      }));
      
      sessionStorage.setItem('h3-initial-view-set', 'true');
    } catch (error) {
      console.error('Error calculating viewport:', error);
    }
  }, [isPreloaded, visibleHexagons.length]);

  // Create deck.gl layers з viewport culling
  const layers = useMemo(() => [
    new GeoJsonLayer({
      id: 'h3-hexagons',
      data: geoJsonData,
      
      // Polygon rendering
      filled: true,
      getFillColor: d => d.properties.color,
      
      // Enhanced outline
      stroked: true,
      getLineColor: [255, 255, 255, 60],
      getLineWidth: 1,
      lineWidthMinPixels: 0.5,
      lineWidthMaxPixels: 1,
      
      // Оптимізований interaction
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 255, 255, 100],
      
      // Простий hover
      onHover: (info) => {
        if (info.object) {
          setHoveredObject(info.object.properties);
          setMousePosition({ x: info.x, y: info.y });
        } else {
          setHoveredObject(null);
        }
      },
      
      // Performance-oriented update triggers
      updateTriggers: {
        getFillColor: [metric]
      }
    })
  ], [geoJsonData, metric]);

  // 🎨 ПОКАЗУЄМО PROGRESS BAR поки не завантажено
  if (!isPreloaded) {
    return (
      <PreloadProgressBar
        overallProgress={overallProgress}
        currentProgress={currentProgress}
        completedRequests={completedRequests}
        totalTasks={totalTasks}
        currentStep={currentStep}
        error={preloadError}
        onRetry={reloadData}
      />
    );
  }

  return (
    <div style={{position: 'relative', width: '100%', height: '100vh'}}>
      {/* DeckGL with Map */}
      <DeckGL
        viewState={viewState}
        onViewStateChange={({viewState}) => setViewState(viewState)}
        controller={true}
        layers={layers}
        parameters={{
          blendFunc: [770, 771, 1, 771],
          blendEquation: 32774,
          depthTest: false
        }}
      >
        <Map
          reuseMaps
          mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
          preventStyleDiffing={true}
          attributionControl={false}
        />
      </DeckGL>
      
      {/* UI Elements */}
      <MetricSwitcher 
        currentMetric={metric} 
        onMetricChange={setMetric} 
      />
      
      {/* Resolution Control */}
      <ResolutionControl 
        currentResolution={debouncedResolution}
        autoMode={autoResolution}
        onAutoModeChange={setAutoResolution}
        onManualResolutionChange={setManualResolution}
        currentZoom={viewState.zoom}
        loading={loading}
        error={error}
      />
      
      {/* Tooltip */}
      <HoverTooltip 
        hoveredObject={hoveredObject}
        x={mousePosition.x}
        y={mousePosition.y}
      />

      {/* Loading Overlay при зміні resolution */}
      {loading && data && (
        <div style={{
          position: 'absolute',
          top: '50%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          backgroundColor: 'rgba(0, 0, 0, 0.8)',
          color: 'white',
          padding: '20px 30px',
          borderRadius: '12px',
          zIndex: 1001,
          textAlign: 'center',
          backdropFilter: 'blur(5px)'
        }}>
          <div style={{
            width: '40px',
            height: '40px',
            border: '3px solid rgba(255,255,255,0.3)',
            borderTopColor: 'white',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite',
            margin: '0 auto 15px'
          }}></div>
          <div style={{ fontSize: '16px', fontWeight: '500' }}>
            Завантаження H3-{debouncedResolution}...
          </div>
          <div style={{ fontSize: '13px', opacity: 0.8, marginTop: '5px' }}>
            {getResolutionDescription(debouncedResolution)}
          </div>
        </div>
      )}

      {/* Info Panel */}
      <div style={{
        position: 'absolute',
        bottom: '20px',
        right: '20px',
        backgroundColor: 'rgba(255, 255, 255, 0.98)',
        padding: '20px',
        borderRadius: '12px',
        boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
        minWidth: '320px',
        backdropFilter: 'blur(10px)'
      }}>
        <h3 style={{
          margin: '0 0 15px 0',
          fontSize: '18px',
          fontWeight: '600',
          color: '#1a1a1a'
        }}>
          🇺🇦 Київська область - Retail Intelligence
        </h3>
        
        <div style={{
          display: 'grid',
          gap: '10px',
          fontSize: '14px',
          color: '#555'
        }}>
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            padding: '8px 12px',
            backgroundColor: '#f8f8f8',
            borderRadius: '6px'
          }}>
            <span>📊 Завантажено гексагонів:</span>
            <strong style={{color: '#1a1a1a'}}>
              {geoJsonData.features?.length || 0} / {data?.total_hexagons || 0}
            </strong>
          </div>
          
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            padding: '8px 12px',
            backgroundColor: '#f8f8f8',
            borderRadius: '6px'
          }}>
            <span>🔍 Роздільність H3:</span>
            <strong style={{color: '#1a1a1a'}}>
              Рівень {debouncedResolution}
              {autoResolution && <span style={{fontSize: '11px', color: '#666'}}> (авто)</span>}
            </strong>
          </div>
          
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            padding: '8px 12px',
            backgroundColor: metric === 'competition' ? '#fff3e0' : '#f3e5f5',
            borderRadius: '6px'
          }}>
            <span>📈 Поточна метрика:</span>
            <strong style={{color: metric === 'competition' ? '#ff6f00' : '#7b1fa2'}}>
              {metric === 'competition' ? 'Інтенсивність конкуренції' : 'Ринкові можливості'}
            </strong>
          </div>
          
          {/* Debug інформація - ТИМЧАСОВО ВІДКЛЮЧЕНО */}
          {/* <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            padding: '8px 12px',
            backgroundColor: '#f0f8ff',
            borderRadius: '6px',
            fontSize: '12px'
          }}>
            <span>👁️ Прозорість:</span>
            <strong style={{color: '#2196f3'}}>
              {debugInfo.baseOpacity} (zoom: {debugInfo.zoom.toFixed(1)})
            </strong>
          </div> */}
        </div>
      </div>
      
      {/* CSS для анімації */}
      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
};

export default H3MapVisualization;