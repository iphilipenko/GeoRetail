import React, { useState, useEffect, useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { MapView } from '@deck.gl/core';
import 'maplibre-gl/dist/maplibre-gl.css';

// Hook for API data fetching
const useH3Data = (metric, resolution, limit) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const response = await fetch(
          `http://localhost:8000/api/v1/visualization/kyiv-h3?metric=${metric}&resolution=${resolution}&limit=${limit}`
        );
        
        if (!response.ok) {
          throw new Error(`API Error: ${response.status}`);
        }
        
        const result = await response.json();
        setData(result);
        setError(null);
      } catch (err) {
        setError(err.message);
        console.error('Failed to fetch H3 data:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [metric, resolution, limit]);

  return { data, loading, error };
};

// Оптимізовані кольорові схеми з градієнтами
const COLOR_SCHEMES = {
  competition: {
    low: [46, 125, 50, 220],       // Темно-зелений (найкраще)
    medium: [255, 193, 7, 220],    // Янтарний
    high: [255, 111, 0, 220],      // Темно-оранжевий
    maximum: [211, 47, 47, 220]    // Темно-червоний (найгірше)
  },
  opportunity: {
    high: [103, 58, 183, 220],     // Глибокий фіолетовий (найкраще)
    medium: [41, 121, 255, 220],   // Яскравий синій
    low: [117, 117, 117, 180]      // Темно-сірий (найгірше)
  }
};

// Metric Switcher Component - оновлений дизайн
const MetricSwitcher = ({ currentMetric, onMetricChange }) => {
  return (
    <div style={{
      position: 'absolute',
      top: '20px',
      left: '20px',
      backgroundColor: 'rgba(255, 255, 255, 0.98)',
      padding: '20px',
      borderRadius: '12px',
      boxShadow: '0 4px 20px rgba(0,0,0,0.15)',
      zIndex: 1000,
      minWidth: '280px',
      backdropFilter: 'blur(10px)'
    }}>
      <div className="metric-switcher-content">
        <h3 style={{
          margin: '0 0 15px 0',
          fontSize: '18px',
          fontWeight: '600',
          color: '#1a1a1a'
        }}>
          📊 Вибір метрики
        </h3>
        
        <div style={{display: 'flex', gap: '10px', marginBottom: '20px'}}>
          <button 
            style={{
              flex: 1,
              padding: '12px',
              backgroundColor: currentMetric === 'competition' 
                ? 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)' 
                : '#f5f5f5',
              background: currentMetric === 'competition'
                ? 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
                : '#f5f5f5',
              color: currentMetric === 'competition' ? 'white' : '#666',
              border: 'none',
              borderRadius: '8px',
              cursor: 'pointer',
              fontSize: '14px',
              fontWeight: '500',
              transition: 'all 0.3s ease',
              boxShadow: currentMetric === 'competition' 
                ? '0 4px 15px rgba(102, 126, 234, 0.4)' 
                : 'none'
            }}
            onClick={() => onMetricChange('competition')}
            onMouseEnter={(e) => {
              if (currentMetric !== 'competition') {
                e.target.style.backgroundColor = '#e8e8e8';
              }
            }}
            onMouseLeave={(e) => {
              if (currentMetric !== 'competition') {
                e.target.style.backgroundColor = '#f5f5f5';
              }
            }}
          >
            ⚔️ Конкуренція
          </button>
          
          <button 
            style={{
              flex: 1,
              padding: '12px',
              backgroundColor: currentMetric === 'opportunity' 
                ? 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)'
                : '#f5f5f5',
              background: currentMetric === 'opportunity'
                ? 'linear-gradient(135deg, #f093fb 0%, #f5576c 100%)'
                : '#f5f5f5',
              color: currentMetric === 'opportunity' ? 'white' : '#666',
              border: 'none',
              borderRadius: '8px',
              cursor: 'pointer',
              fontSize: '14px',
              fontWeight: '500',
              transition: 'all 0.3s ease',
              boxShadow: currentMetric === 'opportunity'
                ? '0 4px 15px rgba(240, 147, 251, 0.4)'
                : 'none'
            }}
            onClick={() => onMetricChange('opportunity')}
            onMouseEnter={(e) => {
              if (currentMetric !== 'opportunity') {
                e.target.style.backgroundColor = '#e8e8e8';
              }
            }}
            onMouseLeave={(e) => {
              if (currentMetric !== 'opportunity') {
                e.target.style.backgroundColor = '#f5f5f5';
              }
            }}
          >
            💡 Можливості
          </button>
        </div>
        
        <div style={{
          backgroundColor: '#fafafa',
          padding: '15px',
          borderRadius: '8px',
          border: '1px solid #e0e0e0'
        }}>
          <h4 style={{
            margin: '0 0 10px 0',
            fontSize: '14px',
            fontWeight: '600',
            color: '#555'
          }}>
            Легенда:
          </h4>
          
          {currentMetric === 'competition' && (
            <div>
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #2e7d32 0%, #4caf50 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Низька</strong> (0-20%) ✨ Найкраще
                </span>
              </div>
              
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #ffc107 0%, #ffeb3b 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Середня</strong> (20-40%)
                </span>
              </div>
              
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #ff6f00 0%, #ff9800 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Висока</strong> (40-60%)
                </span>
              </div>
              
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #d32f2f 0%, #f44336 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Максимальна</strong> (60%+) ⛔
                </span>
              </div>
            </div>
          )}
          
          {currentMetric === 'opportunity' && (
            <div>
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #673ab7 0%, #9c27b0 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Висока</strong> 🎯 Найкращі локації
                </span>
              </div>
              
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #2979ff 0%, #448aff 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Середня</strong> - Хороший потенціал
                </span>
              </div>
              
              <div style={{display: 'flex', alignItems: 'center', margin: '6px 0'}}>
                <div style={{
                  width: '24px',
                  height: '24px',
                  background: 'linear-gradient(135deg, #757575 0%, #9e9e9e 100%)',
                  borderRadius: '4px',
                  marginRight: '10px',
                  boxShadow: '0 2px 4px rgba(0,0,0,0.1)'
                }}></div>
                <span style={{fontSize: '13px', color: '#333'}}>
                  <strong>Низька</strong> - Обмежений потенціал
                </span>
              </div>
            </div>
          )}
        </div>
      </div>
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

// Main H3 Map Visualization Component
const H3MapVisualization = () => {
  const [metric, setMetric] = useState('opportunity');
  const [resolution, setResolution] = useState(10);
  const [limit, setLimit] = useState(50000);
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [viewState, setViewState] = useState({
    longitude: 30.5234,
    latitude: 50.4501,
    zoom: 9,
    pitch: 0,
    bearing: 0
  });

  // Fetch H3 data
  const { data, loading, error } = useH3Data(metric, resolution, limit);

  // Process data for GeoJsonLayer
  const geoJsonData = useMemo(() => {
    if (!data?.hexagons) {
      return { type: 'FeatureCollection', features: [] };
    }
    
    const features = data.hexagons.map(hex => ({
      type: 'Feature',
      properties: {
        ...hex,
        color: COLOR_SCHEMES[metric][hex.display_category] || [200, 200, 200, 180]
      },
      geometry: hex.geometry
    }));
    
    return {
      type: 'FeatureCollection',
      features
    };
  }, [data, metric]);

  // Update viewport when data loads
  useEffect(() => {
    if (!data?.hexagons?.length) return;

    try {
      const allCoords = data.hexagons.flatMap(hex => 
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
      
      const lonSpan = maxLon - minLon;
      const latSpan = maxLat - minLat;
      const maxSpan = Math.max(lonSpan, latSpan);
      
      let zoom = 10;
      if (maxSpan > 2) zoom = 7;
      else if (maxSpan > 1) zoom = 8;
      else if (maxSpan > 0.5) zoom = 9;
      else zoom = 10;
      
      setViewState({
        longitude: centerLon,
        latitude: centerLat,
        zoom: zoom,
        pitch: 0,
        bearing: 0
      });
    } catch (error) {
      console.error('Error calculating viewport:', error);
    }
  }, [data]);

  // Create deck.gl layers with enhanced styling
  const layers = useMemo(() => [
    new GeoJsonLayer({
      id: 'h3-hexagons',
      data: geoJsonData,
      
      // Polygon rendering
      filled: true,
      getFillColor: d => d.properties.color,
      
      // Subtle outline
      stroked: true,
      getLineColor: [255, 255, 255, 60],
      getLineWidth: 1,
      lineWidthMinPixels: 0.5,
      lineWidthMaxPixels: 1,
      
      // Interaction
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 255, 255, 100],
      
      // Smooth transitions
      transitions: {
        getFillColor: 300
      },
      
      // Events
      onHover: (info) => {
        if (info.object) {
          setHoveredObject(info.object.properties);
          setMousePosition({ x: info.x, y: info.y });
        } else {
          setHoveredObject(null);
        }
      },
      
      // Update triggers
      updateTriggers: {
        getFillColor: [metric]
      }
    })
  ], [geoJsonData, metric]);

  if (loading) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
      }}>
        <div style={{
          padding: '30px',
          backgroundColor: 'white',
          borderRadius: '12px',
          boxShadow: '0 10px 40px rgba(0,0,0,0.2)',
          textAlign: 'center'
        }}>
          <div style={{
            width: '50px',
            height: '50px',
            border: '3px solid #667eea',
            borderTopColor: 'transparent',
            borderRadius: '50%',
            margin: '0 auto 20px',
            animation: 'spin 1s linear infinite'
          }}></div>
          <div style={{fontSize: '18px', color: '#333'}}>
            Завантаження даних H3 для Київської області...
          </div>
        </div>
        <style>{`
          @keyframes spin {
            to { transform: rotate(360deg); }
          }
        `}</style>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{
        display: 'flex',
        justifyContent: 'center',
        alignItems: 'center',
        height: '100vh',
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)'
      }}>
        <div style={{
          padding: '30px',
          backgroundColor: 'white',
          borderRadius: '12px',
          boxShadow: '0 10px 40px rgba(0,0,0,0.2)',
          maxWidth: '400px',
          textAlign: 'center'
        }}>
          <div style={{
            fontSize: '48px',
            marginBottom: '20px'
          }}>❌</div>
          <div style={{
            color: '#d32f2f',
            marginBottom: '20px',
            fontSize: '16px'
          }}>
            Помилка завантаження даних: {error}
          </div>
          <button 
            onClick={() => window.location.reload()}
            style={{
              padding: '12px 30px',
              background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
              color: 'white',
              border: 'none',
              borderRadius: '6px',
              cursor: 'pointer',
              fontSize: '16px',
              fontWeight: '500',
              boxShadow: '0 4px 15px rgba(102, 126, 234, 0.4)'
            }}
          >
            🔄 Спробувати знову
          </button>
        </div>
      </div>
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
      
      {/* Tooltip */}
      <HoverTooltip 
        hoveredObject={hoveredObject}
        x={mousePosition.x}
        y={mousePosition.y}
      />

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
            <strong style={{color: '#1a1a1a'}}>Рівень {resolution}</strong>
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
        </div>
      </div>
    </div>
  );
};

export default H3MapVisualization;