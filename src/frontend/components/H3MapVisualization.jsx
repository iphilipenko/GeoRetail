import React, { useState, useEffect, useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { H3HexagonLayer } from '@deck.gl/geo-layers';
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

// Color schemes for different metrics
const COLOR_SCHEMES = {
  competition: {
    low: [76, 175, 80, 180],      // 🟢 Green - низька конкуренція
    medium: [255, 193, 7, 180],   // 🟡 Yellow - помірна конкуренція
    high: [255, 152, 0, 180],     // 🟠 Orange - висока конкуренція
    maximum: [244, 67, 54, 180]   // 🔴 Red - максимальна конкуренція
  },
  opportunity: {
    high: [156, 39, 176, 180],    // 💎 Purple - high opportunity
    medium: [33, 150, 243, 180],  // 🔵 Blue - medium opportunity
    low: [158, 158, 158, 180]     // ⚫ Gray - low opportunity
  }
};

// Metric Switcher Component
const MetricSwitcher = ({ currentMetric, onMetricChange }) => {
  return (
    <div className="metric-switcher">
      <div className="metric-switcher-content">
        <h3>📊 Metric Selection</h3>
        <div className="metric-buttons">
          <button 
            className={`metric-button ${currentMetric === 'competition' ? 'active' : ''}`}
            onClick={() => onMetricChange('competition')}
          >
            ⚔️ Competition Intensity
          </button>
          <button 
            className={`metric-button ${currentMetric === 'opportunity' ? 'active' : ''}`}
            onClick={() => onMetricChange('opportunity')}
          >
            💡 Market Opportunity
          </button>
        </div>
        
        <div className="legend">
          <h4>🎨 Legend:</h4>
          {currentMetric === 'competition' && (
            <div className="legend-items">
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(76, 175, 80)'}}></span>
                Low (0-20%) - Good opportunities
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(255, 193, 7)'}}></span>
                Medium (20-40%) - Moderate competition
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(255, 152, 0)'}}></span>
                High (40-60%) - Challenging
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(244, 67, 54)'}}></span>
                Maximum (60%+) - Avoid
              </div>
            </div>
          )}
          
          {currentMetric === 'opportunity' && (
            <div className="legend-items">
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(156, 39, 176)'}}></span>
                High Opportunity - Excellent locations
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(33, 150, 243)'}}></span>
                Medium Opportunity - Good potential
              </div>
              <div className="legend-item">
                <span className="legend-color" style={{backgroundColor: 'rgb(158, 158, 158)'}}></span>
                Low Opportunity - Limited potential
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

// Tooltip Component
const HoverTooltip = ({ hoveredObject, x, y }) => {
  if (!hoveredObject) return null;

  const { h3_index, competition_intensity, transport_accessibility_score, 
          residential_indicator_score, commercial_activity_score, 
          market_opportunity_score, poi_total_count, retail_count, 
          competitor_count } = hoveredObject;

  return (
    <div 
      className="tooltip"
      style={{
        position: 'absolute',
        left: x + 10,
        top: y + 10,
        zIndex: 1000,
        pointerEvents: 'none'
      }}
    >
      <div className="tooltip-content">
        <div className="tooltip-header">
          📍 H3: {h3_index.slice(-7)}
        </div>
        
        <div className="tooltip-metrics">
          <div className="tooltip-row">
            🏪 POI: {poi_total_count} ({competitor_count} competitors)
          </div>
          <div className="tooltip-row">
            ⚔️ Competition: {(competition_intensity * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row">
            🚌 Transport: {(transport_accessibility_score * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row">
            🏘️ Residential: {(residential_indicator_score * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row">
            💼 Commercial: {(commercial_activity_score * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row opportunity-score">
            📊 Opportunity Score: {market_opportunity_score.toFixed(1)}/1.0
          </div>
        </div>
      </div>
    </div>
  );
};

// Main H3 Map Visualization Component
const H3MapVisualization = () => {
  const [metric, setMetric] = useState('competition');
  const [resolution, setResolution] = useState(10);
  const [limit, setLimit] = useState(1000);
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });

  // Fetch H3 data
  const { data, loading, error } = useH3Data(metric, resolution, limit);

  // Process data for H3HexagonLayer
  const hexagonData = useMemo(() => {
    if (!data?.hexagons) return [];
    
    return data.hexagons.map(hex => ({
      ...hex,
      // Convert geometry coordinates to H3 format for deck.gl
      hex_id: hex.h3_index,
      // Add color based on category
      color: COLOR_SCHEMES[metric][hex.display_category] || [128, 128, 128, 180]
    }));
  }, [data, metric]);

  // Deck.gl layers
  const layers = [
    new H3HexagonLayer({
      id: 'h3-hexagon-layer',
      data: hexagonData,
      getHexagon: d => d.h3_index,
      getFillColor: d => d.color,
      getElevation: d => metric === 'competition' ? d.competition_intensity * 100 : d.market_opportunity_score * 100,
      elevationScale: 20,
      pickable: true,
      onHover: (info) => {
        setHoveredObject(info.object);
        setMousePosition({ x: info.x, y: info.y });
      },
      updateTriggers: {
        getFillColor: [metric],
        getElevation: [metric]
      }
    })
  ];

  // Map viewport - centered on Kyiv
  const INITIAL_VIEW_STATE = {
    longitude: 30.5234,  // Kyiv center
    latitude: 50.4501,
    zoom: 11,
    pitch: 30,
    bearing: 0
  };

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-spinner">
          🔄 Loading H3 data for Kyiv...
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-container">
        <div className="error-message">
          ❌ Error loading data: {error}
        </div>
        <button onClick={() => window.location.reload()}>
          🔄 Retry
        </button>
      </div>
    );
  }

  return (
    <div className="h3-map-container">
      {/* Controls Panel */}
      <MetricSwitcher 
        currentMetric={metric} 
        onMetricChange={setMetric} 
      />
      
      {/* Map */}
      <div className="map-wrapper">
        <DeckGL
          initialViewState={INITIAL_VIEW_STATE}
          controller={true}
          layers={layers}
          getTooltip={({object}) => object && `H3: ${object.h3_index}`}
        >
          <Map
            mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            attributionControl={false}
          />
        </DeckGL>
        
        {/* Custom Tooltip */}
        <HoverTooltip 
          hoveredObject={hoveredObject}
          x={mousePosition.x}
          y={mousePosition.y}
        />
      </div>

      {/* Info Panel */}
      <div className="info-panel">
        <h2>🇺🇦 Kyiv Retail Location Intelligence</h2>
        <div className="stats">
          <div className="stat">
            <strong>📊 Total Hexagons:</strong> {data?.total_hexagons || 0}
          </div>
          <div className="stat">
            <strong>🔍 Resolution:</strong> H3-{resolution}
          </div>
          <div className="stat">
            <strong>📈 Current Metric:</strong> {metric === 'competition' ? 'Competition Intensity' : 'Market Opportunity'}
          </div>
        </div>
      </div>
    </div>
  );
};

export default H3MapVisualization;