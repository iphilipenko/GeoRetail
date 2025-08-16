// HexagonDetailsModal.jsx
// 🎯 Головний Modal компонент для детальної інформації про гексагон
// Показує slide-in modal з 50% ширини екрану

import React, { useEffect, useState } from 'react';
import { LocationInfoSection, MetricsOverviewSection, POIDetailsSection, RecommendationsSection } from './sections';
import AnalysisRadiusControl from '../controls/AnalysisRadiusControl';
import useHexagonDetails from '../../hooks/useHexagonDetails';
import '../../styles/HexagonDetailsModal.css';

const HexagonDetailsModal = ({ h3Index, resolution, isOpen, onClose }) => {
  const [activeTab, setActiveTab] = useState('overview');
  
  const {
    data,
    loading,
    error,
    analysisType,
    customRings,
    changeAnalysisType,
    changeCustomRings,
    refresh,
    locationInfo,
    metrics,
    poiDetails,
    influenceAnalysis,
    neighborCoverage,
    availableAnalyses
  } = useHexagonDetails(h3Index, resolution);

  // Анімація відкриття/закриття
  useEffect(() => {
    if (isOpen) {
      document.body.style.overflow = 'hidden';
    } else {
      document.body.style.overflow = 'unset';
    }
    
    return () => {
      document.body.style.overflow = 'unset';
    };
  }, [isOpen]);

  // Обробка Escape key
  useEffect(() => {
    const handleEscape = (e) => {
      if (e.key === 'Escape' && isOpen) {
        onClose();
      }
    };
    
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [isOpen, onClose]);

  if (!isOpen) return null;

  return (
    <>
      {/* Backdrop */}
      <div 
        className="modal-backdrop"
        onClick={onClose}
      />
      
      {/* Modal Container */}
      <div className={`hexagon-details-modal ${isOpen ? 'open' : ''}`}>
        {/* Header */}
        <div className="modal-header">
          <div className="header-info">
            <h2>🎯 Аналіз гексагона</h2>
            <div className="hexagon-id">
              <span className="h3-badge">H3-{resolution}</span>
              <code className="h3-index">{h3Index}</code>
            </div>
          </div>
          
          <div className="header-controls">
            <button 
              className="refresh-btn"
              onClick={refresh}
              disabled={loading}
              title="Оновити дані"
            >
              🔄
            </button>
            <button 
              className="close-btn"
              onClick={onClose}
              title="Закрити"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Analysis Controls */}
        <div className="analysis-controls-section">
          <AnalysisRadiusControl 
            resolution={resolution}
            currentAnalysisType={analysisType}
            currentRings={neighborCoverage?.rings || 1}
            onAnalysisTypeChange={changeAnalysisType}
            onRingsChange={changeCustomRings}
            availableAnalyses={availableAnalyses}
          />
        </div>

        {/* Tab Navigation */}
        <div className="tab-navigation">
          <button 
            className={`tab-btn ${activeTab === 'overview' ? 'active' : ''}`}
            onClick={() => setActiveTab('overview')}
          >
            📊 Огляд
          </button>
          <button 
            className={`tab-btn ${activeTab === 'poi' ? 'active' : ''}`}
            onClick={() => setActiveTab('poi')}
          >
            🏦 POI ({poiDetails.length})
          </button>
          <button 
            className={`tab-btn ${activeTab === 'recommendations' ? 'active' : ''}`}
            onClick={() => setActiveTab('recommendations')}
          >
            💡 Рекомендації
          </button>
        </div>

        {/* Content */}
        <div className="modal-content">
          {error && (
            <div className="error-banner">
              ❌ Помилка: {error}
              <button onClick={refresh} className="retry-btn">Повторити</button>
            </div>
          )}

          {loading && (
            <div className="loading-banner">
              <div className="loading-spinner"></div>
              Завантаження аналізу...
            </div>
          )}

          {/* Tab Content */}
          {activeTab === 'overview' && (
            <div className="tab-content">
              <LocationInfoSection 
                h3Index={h3Index}
                resolution={resolution}
                locationData={locationInfo}
                coverageData={neighborCoverage}
              />
              
              <MetricsOverviewSection 
                metrics={metrics}
                poiData={{ poi_details: poiDetails, influence_analysis: influenceAnalysis }}
                resolution={resolution}
              />
            </div>
          )}

          {activeTab === 'poi' && (
            <div className="tab-content">
              <POIDetailsSection 
                poiData={poiDetails}
                influenceAnalysis={influenceAnalysis}
                h3Index={h3Index}
                coverageInfo={neighborCoverage}
              />
            </div>
          )}

          {activeTab === 'recommendations' && (
            <div className="tab-content">
              <RecommendationsSection 
                h3Index={h3Index}
                metrics={metrics}
                poiData={poiDetails}
                isPlaceholder={true}
              />
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="modal-footer">
          <div className="footer-stats">
            {data && (
              <>
                <span>📊 {poiDetails.length} POI</span>
                <span>📏 {neighborCoverage?.area_km2 || 0} км²</span>
                <span>🕘 {new Date(data.generated_at).toLocaleTimeString()}</span>
              </>
            )}
          </div>
          
          <div className="footer-controls">
            <span className="analysis-type-indicator">
              {availableAnalyses.find(a => a.analysis_type === analysisType)?.name || 'Невідомо'}
            </span>
          </div>
        </div>
      </div>
    </>
  );
};

export default HexagonDetailsModal;
