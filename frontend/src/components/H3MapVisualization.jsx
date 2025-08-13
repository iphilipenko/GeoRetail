import React, { useState, useEffect, useMemo } from 'react';
import { Map } from 'react-map-gl/maplibre';
import { DeckGL } from '@deck.gl/react';
import { GeoJsonLayer } from '@deck.gl/layers';
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
    low: [76, 175, 80, 200],      // 🟢 Green
    medium: [255, 193, 7, 200],   // 🟡 Yellow
    high: [255, 152, 0, 200],     // 🟠 Orange
    maximum: [244, 67, 54, 200]   // 🔴 Red
  },
  opportunity: {
    high: [156, 39, 176, 200],    // 💎 Purple
    medium: [33, 150, 243, 200],  // 🔵 Blue
    low: [158, 158, 158, 200]     // ⚫ Gray
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
          📍 H3: {h3_index ? h3_index.slice(-7) : 'unknown'}
        </div>
        
        <div className="tooltip-metrics">
          <div className="tooltip-row">
            🏪 POI: {poi_total_count || 0} ({competitor_count || 0} competitors)
          </div>
          <div className="tooltip-row">
            ⚔️ Competition: {((competition_intensity || 0) * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row">
            🚌 Transport: {((transport_accessibility_score || 0) * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row">
            🏘️ Residential: {((residential_indicator_score || 0) * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row">
            💼 Commercial: {((commercial_activity_score || 0) * 100).toFixed(0)}%
          </div>
          <div className="tooltip-row opportunity-score">
            📊 Opportunity Score: {(market_opportunity_score || 0).toFixed(1)}/1.0
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
  const [limit, setLimit] = useState(50000); // Збільшуємо до 50K для всієї області
  const [hoveredObject, setHoveredObject] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });

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
        color: COLOR_SCHEMES[metric][hex.display_category] || [255, 0, 0, 255] // Red fallback
      },
      geometry: hex.geometry
    }));

    console.log('✅ GeoJSON features created:', features.length);
    if (features.length > 0) {
      console.log('✅ First feature:', features[0]);
    }
    
    return {
      type: 'FeatureCollection',
      features
    };
  }, [data, metric]);

  // Calculate viewport from ALL data bounds
  const mapViewState = useMemo(() => {
    if (!data?.hexagons?.length) {
      console.log('🗺️ Using default Kyiv center');
      return {
        longitude: 30.5234,
        latitude: 50.4501,
        zoom: 8,
        pitch: 0,
        bearing: 0
      };
    }

    try {
      // Calculate bounds from ALL hexagons for proper viewport
      const allCoords = data.hexagons.flatMap(hex => 
        hex.geometry?.coordinates?.[0] || []
      ).filter(coord => coord && coord.length === 2);

      if (allCoords.length === 0) {
        console.log('🗺️ No valid coordinates found');
        return { longitude: 30.5234, latitude: 50.4501, zoom: 8, pitch: 0, bearing: 0 };
      }

      const lons = allCoords.map(c => c[0]);
      const lats = allCoords.map(c => c[1]);
      
      const minLon = Math.min(...lons);
      const maxLon = Math.max(...lons);
      const minLat = Math.min(...lats);
      const maxLat = Math.max(...lats);
      
      const centerLon = (minLon + maxLon) / 2;
      const centerLat = (minLat + maxLat) / 2;
      
      // Calculate zoom based on bounds
      const lonSpan = maxLon - minLon;
      const latSpan = maxLat - minLat;
      const maxSpan = Math.max(lonSpan, latSpan);
      
      let zoom = 10;
      if (maxSpan > 2) zoom = 7;      // Вся область
      else if (maxSpan > 1) zoom = 8; // Великий регіон  
      else if (maxSpan > 0.5) zoom = 9; // Середній регіон
      else zoom = 10;                 // Детальний zoom
      
      console.log(`🗺️ Kyiv Oblast bounds: [${minLon.toFixed(3)}, ${minLat.toFixed(3)}] to [${maxLon.toFixed(3)}, ${maxLat.toFixed(3)}]`);
      console.log(`🗺️ Center: [${centerLon.toFixed(3)}, ${centerLat.toFixed(3)}], zoom: ${zoom}`);
      
      return {
        longitude: centerLon,
        latitude: centerLat,
        zoom: zoom,
        pitch: 0,
        bearing: 0
      };
    } catch (error) {
      console.error('❌ Error calculating viewport:', error);
      return { longitude: 30.5234, latitude: 50.4501, zoom: 8, pitch: 0, bearing: 0 };
    }
  }, [data]);

  // Create deck.gl layers
  const layers = useMemo(() => [
    new GeoJsonLayer({
      id: 'h3-polygons',
      data: geoJsonData,
      getFillColor: d => {
        const color = d.properties.color;
        console.log(`🎨 Polygon ${d.properties.h3_index?.slice(-4)}: [${color.join(',')}]`);
        return color;
      },
      getLineColor: [0, 255, 0, 255], // Green outline
      getLineWidth: 3,
      lineWidthMinPixels: 1,
      pickable: true,
      autoHighlight: true,
      highlightColor: [255, 255, 0, 200],
      opacity: 1.0,
      onHover: (info) => {
        if (info.object) {
          console.log('🖱️ Hover:', info.object.properties.h3_index);
          setHoveredObject(info.object.properties);
          setMousePosition({ x: info.x, y: info.y });
        } else {
          setHoveredObject(null);
        }
      },
      updateTriggers: {
        getFillColor: [metric]
      }
    })
  ], [geoJsonData, metric]);

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

  console.log('🚀 Rendering map with:', {
    hexagons: data?.total_hexagons || 0,
    features: geoJsonData.features.length,
    viewState: mapViewState
  });

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
          initialViewState={mapViewState}
          controller={true}
          layers={layers}
        >
          <Map
            mapStyle="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
            attributionControl={false}
          />
        </DeckGL>
        
        {/* Tooltip */}
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
            <strong>📊 Loaded Hexagons:</strong> {geoJsonData.features?.length || 0} / {data?.total_hexagons || 0}
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