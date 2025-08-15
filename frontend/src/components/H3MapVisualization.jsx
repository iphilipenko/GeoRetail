import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
import { MapView } from '@deck.gl/core';
import 'maplibre-gl/dist/maplibre-gl.css';

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

// Enhanced Hook for API data fetching with caching and fallback
const useH3Data = (metric, resolution, limit) => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [previousResolution, setPreviousResolution] = useState(resolution);
  
  // Simple in-memory cache
  const cacheRef = useRef({});
  
  useEffect(() => {
    const cacheKey = `${metric}-${resolution}`;
    
    // Clear cache on first load to avoid old URLs
    if (Object.keys(cacheRef.current).length === 0) {
      console.log('🗑️ Clearing cache on component mount');
      cacheRef.current = {};
    }
    
    // Check cache first
    if (cacheRef.current[cacheKey]) {
      console.log('💾 Using cached data for:', cacheKey);
      setData(cacheRef.current[cacheKey]);
      setLoading(false);
      setError(null);
      setPreviousResolution(resolution);
      return;
    }
    
    // Fetch new data
    const fetchData = async () => {
      try {
        setLoading(true);
        const url = `http://localhost:8000/api/v1/visualization/kyiv-h3?metric_type=${metric}&resolution=${resolution}&limit=${limit}`;
        console.log('🔍 Fetching H3 data from URL:', url);
        console.log('📊 Parameters:', { metric_type: metric, resolution, limit });
        
        const response = await fetch(url);
        
        console.log('📡 Response status:', response.status);
        console.log('📡 Response headers:', response.headers);
        
        if (!response.ok) {
          const errorText = await response.text();
          console.error('❌ API Error details:', errorText);
          throw new Error(`API Error: ${response.status} ${response.statusText} - ${errorText}`);
        }
        
        const result = await response.json();
        console.log('✅ API Response received:', { 
          total_hexagons: result.total_hexagons, 
          hexagons_count: result.hexagons?.length 
        });
        
        // Store in cache
        cacheRef.current[cacheKey] = result;
        
        setData(result);
        setError(null);
        setPreviousResolution(resolution);
      } catch (err) {
        const errorMessage = err.message;
        console.error('❌ Failed to fetch H3 data:', err);
        console.error('🔧 Attempted URL:', `http://localhost:8000/api/v1/visualization/kyiv-h3?metric_type=${metric}&resolution=${resolution}&limit=${limit}`);
        
        // Try to fallback to previous resolution data if available
        const fallbackKey = `${metric}-${previousResolution}`;
        if (cacheRef.current[fallbackKey] && resolution !== previousResolution) {
          console.log(`🔄 Falling back to resolution ${previousResolution}`);
          setData(cacheRef.current[fallbackKey]);
          setError(`Не вдалося завантажити H3-${resolution}, використовуємо H3-${previousResolution}`);
        } else {
          setData(null);
          setError(errorMessage);
        }
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, [metric, resolution, limit, previousResolution]);

  return { data, loading, error, actualResolution: previousResolution };
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

// Main H3 Map Visualization Component
const H3MapVisualization = () => {
  const [metric, setMetric] = useState('opportunity');
  const [autoResolution, setAutoResolution] = useState(true); // Авто-режим включений за замовчуванням
  const [manualResolution, setManualResolution] = useState(8); // Ручний вибір (початковий H3-8)
  const [limit, setLimit] = useState(1000000); // Збільшено до 1 мільйона гексагонів
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [viewState, setViewState] = useState({
    longitude: 30.5234,
    latitude: 50.4501,
    zoom: 9,
    pitch: 0,
    bearing: 0
  });

  // Визначаємо поточний resolution на основі режиму
  const currentResolution = autoResolution 
    ? getOptimalResolution(viewState.zoom)
    : manualResolution;
  
  // Використовуємо debounce для уникнення частих змін при зумі
  const debouncedResolution = useDebounce(currentResolution, 300);
  
  // Fetch H3 data з новим resolution
  const { data, loading, error, actualResolution } = useH3Data(metric, debouncedResolution, limit);

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

  if (loading && !data) {
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
      </div>
    );
  }

  if (error && !data) {
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
      
      {/* Resolution Control */}
      <ResolutionControl 
        currentResolution={actualResolution}
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
              Рівень {actualResolution}
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