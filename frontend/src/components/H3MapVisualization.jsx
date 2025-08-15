// frontend/src/components/H3MapVisualization.jsx
import React, { useState, useEffect, useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { MapView } from '@deck.gl/core';
import 'maplibre-gl/dist/maplibre-gl.css';

// Імпорт модульних компонентів
import MetricSwitcher from './H3Visualization/components/controls/MetricSwitcher';
import PreloadProgressBar from './H3Visualization/components/ui/PreloadProgressBar';
import HoverTooltip from './H3Visualization/components/ui/HoverTooltip';
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
  if (zoom < 8) return 7;
  if (zoom < 10) return 8;
  if (zoom < 12) return 9;
  return 10;
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

// Main H3 Map Visualization Component
const H3MapVisualization = () => {
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

  const {
    isPreloaded,
    overallProgress,
    currentProgress,
    completedRequests,
    totalTasks,
    preloadError,
    currentStep,
    getVisibleHexagons,
    getStats,
    reloadData
  } = usePreloadedH3Data(1000000);

  const loading = !isPreloaded || (currentProgress > 0 && currentProgress < 100);
  const error = preloadError;

  const stats = useMemo(() => {
    if (!isPreloaded) return { loadedDatasets: 0, totalHexagons: 0 };
    return getStats();
  }, [isPreloaded, getStats]);

  const currentResolution = autoResolution 
    ? getOptimalResolution(viewState.zoom)
    : manualResolution;
  
  const debouncedResolution = useDebounce(currentResolution, 300);
  const debouncedViewState = useDebounce(viewState, 150);

  // 🎯 VIEWPORT CULLING - отримуємо тільки видимі гексагони без обмеження
  const visibleHexagons = useMemo(() => {
    if (!isPreloaded) return [];
    
    // ВИПРАВЛЕННЯ: використовуємо широкий viewport або взагалі без viewport culling для початку
    const allHexagons = getVisibleHexagons(metric, debouncedResolution, {
      ...debouncedViewState,
      // Розширюємо viewport для отримання більше даних
      zoom: Math.max(6, debouncedViewState.zoom - 2)
    });
    
    console.log(`🎯 Loaded hexagons for ${metric} H3-${debouncedResolution}:`, allHexagons.length);
    return allHexagons;
  }, [isPreloaded, metric, debouncedResolution, debouncedViewState, getVisibleHexagons]);

  const data = useMemo(() => ({
    total_hexagons: stats.totalHexagons || 0,
    loaded_datasets: stats.loadedDatasets || 0,
    hexagons: visibleHexagons || []
  }), [stats.totalHexagons, stats.loadedDatasets, visibleHexagons]);

  // 🎨 ВИПРАВЛЕННЯ КОЛЬОРІВ: правильна обробка кольорової схеми
  const geoJsonData = useMemo(() => {
    if (!visibleHexagons.length) {
      return { type: 'FeatureCollection', features: [] };
    }
    
    console.log(`🎨 Processing ${visibleHexagons.length} hexagons for coloring`);
    
    const features = visibleHexagons.map(hex => {
      // ВИПРАВЛЕННЯ: правильна обробка кольорів
      let baseColor;
      const category = hex.display_category || 'low';
      
      if (H3_COLOR_SCHEMES && H3_COLOR_SCHEMES[metric] && H3_COLOR_SCHEMES[metric][category]) {
        baseColor = H3_COLOR_SCHEMES[metric][category];
      } else {
        // Fallback кольори якщо схема не завантажена
        const fallbackColors = {
          opportunity: {
            high: [76, 175, 80],    // Зелений
            medium: [255, 193, 7],  // Жовтий  
            low: [244, 67, 54]      // Червоний
          },
          competition: {
            high: [244, 67, 54],    // Червоний
            medium: [255, 152, 0],  // Помаранчевий
            low: [76, 175, 80]      // Зелений
          }
        };
        baseColor = fallbackColors[metric]?.[category] || [128, 128, 128];
      }
      
      // Прозорість - робимо помітними
      const alpha = 180; // Збільшуємо прозорість
      const colorWithAlpha = [baseColor[0], baseColor[1], baseColor[2], alpha];
      
      return {
        type: 'Feature',
        properties: {
          ...hex,
          color: colorWithAlpha
        },
        geometry: hex.geometry
      };
    });
    
    console.log(`🎨 Created ${features.length} colored features`);
    return {
      type: 'FeatureCollection',
      features
    };
  }, [visibleHexagons, metric]);

  // 🗺️ ВИПРАВЛЕННЯ VIEWPORT: спрощена логіка встановлення початкового viewport
  useEffect(() => {
    if (!isPreloaded || visibleHexagons.length === 0) return;

    // Встановлюємо viewport тільки один раз при першому завантаженні даних
    const hasSetInitialView = sessionStorage.getItem('h3-map-initial-view');
    if (hasSetInitialView) return;

    console.log(`🗺️ Setting initial viewport for ${visibleHexagons.length} hexagons`);

    try {
      // Збираємо всі координати для обчислення bounds
      const allCoords = [];
      
      visibleHexagons.forEach(hex => {
        if (hex.geometry?.coordinates?.[0]) {
          hex.geometry.coordinates[0].forEach(coord => {
            if (Array.isArray(coord) && coord.length === 2) {
              allCoords.push(coord);
            }
          });
        }
      });

      if (allCoords.length > 0) {
        const lons = allCoords.map(c => c[0]);
        const lats = allCoords.map(c => c[1]);
        
        const minLon = Math.min(...lons);
        const maxLon = Math.max(...lons);
        const minLat = Math.min(...lats);
        const maxLat = Math.max(...lats);
        
        const centerLon = (minLon + maxLon) / 2;
        const centerLat = (minLat + maxLat) / 2;
        
        // Обчислюємо zoom для покриття всієї області
        const latRange = maxLat - minLat;
        const lonRange = maxLon - minLon;
        const maxRange = Math.max(latRange, lonRange);
        
        let zoom = 7;
        if (maxRange < 0.1) zoom = 10;
        else if (maxRange < 0.5) zoom = 9;
        else if (maxRange < 1) zoom = 8;
        else if (maxRange < 2) zoom = 7;
        else zoom = 6;

        console.log(`🗺️ Calculated viewport: center=[${centerLon.toFixed(4)}, ${centerLat.toFixed(4)}], zoom=${zoom}`);
        console.log(`🗺️ Data bounds: lon=[${minLon.toFixed(4)}, ${maxLon.toFixed(4)}], lat=[${minLat.toFixed(4)}, ${maxLat.toFixed(4)}]`);

        setViewState(prev => ({
          ...prev,
          longitude: centerLon,
          latitude: centerLat,
          zoom: zoom
        }));

        sessionStorage.setItem('h3-map-initial-view', 'true');
      }
    } catch (error) {
      console.error('Error setting viewport:', error);
    }
  }, [isPreloaded, visibleHexagons.length]);

  const layers = useMemo(() => [
    new GeoJsonLayer({
      id: 'h3-hexagons',
      data: geoJsonData,
      
      // Polygon rendering
      filled: true,
      getFillColor: d => d.properties.color,
      
      // Enhanced outline
      stroked: true,
      getLineColor: [255, 255, 255, 100], // Збільшуємо видимість контурів
      getLineWidth: 2,
      lineWidthMinPixels: 1,
      lineWidthMaxPixels: 3,
      
      // Interaction
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 255, 255, 200],
      
      // Hover handler
      onHover: (info) => {
        if (info.object) {
          setHoveredObject(info.object.properties);
          setMousePosition({ x: info.x, y: info.y });
        } else {
          setHoveredObject(null);
        }
      }
    })
  ], [geoJsonData]);

  if (!isPreloaded) {
    return (
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        alignItems: 'center',
        width: '100vw',
        height: '100vh',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", sans-serif'
      }}>
        <PreloadProgressBar 
          overallProgress={overallProgress}
          currentProgress={currentProgress}
          completedRequests={completedRequests}
          totalTasks={totalTasks}
          currentStep={currentStep}
          error={error}
          onRetry={reloadData}
        />
      </div>
    );
  }

  if (error && !isPreloaded) {
    return (
      <div style={{
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        alignItems: 'center',
        width: '100vw',
        height: '100vh',
        background: 'linear-gradient(135deg, #ff6b6b 0%, #ee5a24 100%)',
        color: 'white',
        fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", sans-serif',
        textAlign: 'center',
        padding: '2rem'
      }}>
        <div style={{
          background: 'white',
          color: '#d63031',
          padding: '2rem 3rem',
          borderRadius: '15px',
          boxShadow: '0 20px 40px rgba(0,0,0,0.1)',
          maxWidth: '500px'
        }}>
          <h1 style={{ margin: '0 0 1rem 0', fontSize: '1.5rem' }}>
            🚨 Помилка завантаження
          </h1>
          <p style={{ margin: '0 0 1rem 0', fontSize: '1rem' }}>
            {error}
          </p>
          <button 
            onClick={reloadData}
            style={{
              background: '#d63031',
              color: 'white',
              border: 'none',
              padding: '0.75rem 1.5rem',
              borderRadius: '8px',
              fontSize: '1rem',
              cursor: 'pointer',
              transition: 'background 0.2s'
            }}
          >
            🔄 Спробувати знову
          </button>
        </div>
      </div>
    );
  }

  return (
    <div style={{
      position: 'relative',
      width: '100vw',
      height: '100vh',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", "Roboto", sans-serif'
    }}>
      <DeckGL
        initialViewState={viewState}
        onViewStateChange={({ viewState }) => setViewState(viewState)}
        controller={true}
        layers={layers}
        views={new MapView({ id: 'map' })}
        getCursor={() => 'crosshair'}
      >
        <Map
          id="map"
          mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
          preventStyleDiffing={true}
          reuseMaps={true}
        />
      </DeckGL>

      <MetricSwitcher 
        currentMetric={metric}
        onMetricChange={setMetric}
      />

      <ResolutionControl 
        currentResolution={debouncedResolution}
        autoMode={autoResolution}
        onAutoModeChange={setAutoResolution}
        onManualResolutionChange={setManualResolution}
        currentZoom={viewState.zoom}
        loading={loading}
        error={error}
      />
      
      <HoverTooltip 
        hoveredObject={hoveredObject}
        x={mousePosition.x}
        y={mousePosition.y}
      />

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
          <div style={{ fontSize: '13px', opacity: '0.8', marginTop: '5px' }}>
            {getResolutionDescription(debouncedResolution)}
          </div>
        </div>
      )}

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
          
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            padding: '8px 12px',
            backgroundColor: '#f0f8ff',
            borderRadius: '6px'
          }}>
            <span>🔄 Прогрес завантаження:</span>
            <strong style={{color: '#2196f3'}}>
              {completedRequests}/{totalTasks} завершено
            </strong>
          </div>
          
          {stats.loadedDatasets > 0 && (
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              padding: '8px 12px',
              backgroundColor: '#e8f5e8',
              borderRadius: '6px'
            }}>
              <span>💾 Завантажених датасетів:</span>
              <strong style={{color: '#4caf50'}}>
                {stats.loadedDatasets}
              </strong>
            </div>
          )}
        </div>
      </div>
      
      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
      `}</style>
    </div>
  );
};

export default H3MapVisualization;