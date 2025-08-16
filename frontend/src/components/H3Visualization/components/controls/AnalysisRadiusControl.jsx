// AnalysisRadiusControl.jsx
// 🎚️ Складний контрол для вибору радіусу аналізу з preview

import React, { useState, useEffect, useMemo } from 'react';

const AnalysisRadiusControl = ({ 
  resolution, 
  currentAnalysisType, 
  currentRings,
  onAnalysisTypeChange,
  onRingsChange,
  availableAnalyses = []
}) => {
  const [customRings, setCustomRings] = useState(currentRings || 1);
  const [showCustom, setShowCustom] = useState(currentAnalysisType === 'custom');

  // Розраховуємо coverage для поточних налаштувань
  const currentCoverage = useMemo(() => {
    const baseAreas = { 7: 5.16, 8: 0.74, 9: 0.105, 10: 0.015 };
    const baseArea = baseAreas[resolution] || 0.1;
    const rings = showCustom ? customRings : currentRings;
    const hexagonCount = rings === 0 ? 1 : 1 + 3 * rings * (rings + 1);
    const totalArea = hexagonCount * baseArea;
    const radiusEstimate = Math.round(Math.sqrt(totalArea / Math.PI) * 1000);
    
    return {
      area: totalArea.toFixed(2),
      hexagonCount,
      radius: radiusEstimate,
      rings
    };
  }, [resolution, currentRings, customRings, showCustom]);

  // Обробляємо зміну типу аналізу
  const handleAnalysisTypeChange = (analysisType) => {
    setShowCustom(analysisType === 'custom');
    onAnalysisTypeChange(analysisType);
    
    if (analysisType !== 'custom') {
      const selectedAnalysis = availableAnalyses.find(a => a.analysis_type === analysisType);
      if (selectedAnalysis) {
        onRingsChange(selectedAnalysis.optimal_rings);
      }
    }
  };

  // Обробляємо зміну custom rings
  const handleCustomRingsChange = (rings) => {
    setCustomRings(rings);
    if (showCustom) {
      onRingsChange(rings);
    }
  };

  return (
    <div className="analysis-radius-control">
      <div className="control-header">
        <h4>🎯 Стратегія аналізу</h4>
        <div className="current-coverage">
          Покриття: <strong>{currentCoverage.area} км²</strong> 
          ({currentCoverage.hexagonCount} гексагонів)
        </div>
      </div>

      {/* Селектор типу аналізу */}
      <div className="analysis-type-selector">
        <label>Тип аналізу:</label>
        <select 
          value={currentAnalysisType}
          onChange={(e) => handleAnalysisTypeChange(e.target.value)}
          className="analysis-select"
        >
          {availableAnalyses.map(analysis => (
            <option key={analysis.analysis_type} value={analysis.analysis_type}>
              {analysis.name}
            </option>
          ))}
        </select>
      </div>

      {/* Опис поточного аналізу */}
      <div className="analysis-description">
        {availableAnalyses.find(a => a.analysis_type === currentAnalysisType)?.description}
      </div>

      {/* Custom радіус контрол */}
      {showCustom && (
        <div className="custom-radius-control">
          <label>Радіус аналізу:</label>
          <div className="slider-container">
            <input 
              type="range" 
              min="0" 
              max="8" 
              value={customRings}
              onChange={(e) => handleCustomRingsChange(parseInt(e.target.value))}
              className="radius-slider"
            />
            <div className="slider-labels">
              <span>0</span>
              <span>2</span>
              <span>4</span>
              <span>6</span>
              <span>8</span>
            </div>
          </div>
          
          <div className="rings-info">
            <span className="rings-count">
              {customRings === 0 ? 'Тільки центр' : `+${customRings} кільце${customRings > 1 ? (customRings < 5 ? 'а' : 'ець') : ''}`}
            </span>
          </div>
        </div>
      )}

      {/* Preview радіусу */}
      <div className="coverage-preview">
        <div className="preview-grid">
          <div className="preview-item">
            <span className="preview-label">📏 Площа покриття:</span>
            <span className="preview-value">{currentCoverage.area} км²</span>
          </div>
          <div className="preview-item">
            <span className="preview-label">🔢 Кількість гексагонів:</span>
            <span className="preview-value">{currentCoverage.hexagonCount}</span>
          </div>
          <div className="preview-item">
            <span className="preview-label">📐 Приблизний радіус:</span>
            <span className="preview-value">{currentCoverage.radius}м</span>
          </div>
          <div className="preview-item">
            <span className="preview-label">🎯 Кілець аналізу:</span>
            <span className="preview-value">{currentCoverage.rings}</span>
          </div>
        </div>

        {/* Рекомендації по радіусу */}
        <div className="radius-recommendations">
          <div className={`recommendation ${currentCoverage.rings <= 2 ? 'active' : ''}`}>
            🚶 Пішохідна доступність: 0-2 кільця
          </div>
          <div className={`recommendation ${currentCoverage.rings >= 2 && currentCoverage.rings <= 4 ? 'active' : ''}`}>
            🚗 Автомобільна доступність: 2-4 кільця  
          </div>
          <div className={`recommendation ${currentCoverage.rings >= 4 ? 'active' : ''}`}>
            📊 Ринковий огляд: 4+ кілець
          </div>
        </div>
      </div>

      {/* Швидкі пресети */}
      <div className="quick-presets">
        <h5>🚀 Швидкий вибір:</h5>
        <div className="preset-buttons">
          <button 
            className={`preset-btn ${currentAnalysisType === 'pedestrian_competition' ? 'active' : ''}`}
            onClick={() => handleAnalysisTypeChange('pedestrian_competition')}
          >
            🚶 Пішохідна
          </button>
          <button 
            className={`preset-btn ${currentAnalysisType === 'site_selection' ? 'active' : ''}`}
            onClick={() => handleAnalysisTypeChange('site_selection')}
          >
            🏪 Вибір локації
          </button>
          <button 
            className={`preset-btn ${currentAnalysisType === 'market_overview' ? 'active' : ''}`}
            onClick={() => handleAnalysisTypeChange('market_overview')}
          >
            📊 Огляд ринку
          </button>
          <button 
            className={`preset-btn ${currentAnalysisType === 'custom' ? 'active' : ''}`}
            onClick={() => handleAnalysisTypeChange('custom')}
          >
            ⚙️ Налаштування
          </button>
        </div>
      </div>

      {/* Детальна інформація про поточний аналіз */}
      {!showCustom && (
        <div className="analysis-details">
          {(() => {
            const analysis = availableAnalyses.find(a => a.analysis_type === currentAnalysisType);
            if (!analysis) return null;
            
            return (
              <div className="details-grid">
                <div>
                  <strong>Оптимальні кільця:</strong> {analysis.optimal_rings}
                </div>
                <div>
                  <strong>Максимум:</strong> {analysis.max_rings}
                </div>
                <div>
                  <strong>Цільова площа:</strong> {analysis.estimated_area_km2} км²
                </div>
                <div>
                  <strong>Гексагонів:</strong> {analysis.hexagon_count}
                </div>
              </div>
            );
          })()}
        </div>
      )}
    </div>
  );
};

export default AnalysisRadiusControl;
