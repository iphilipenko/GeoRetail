// frontend/src/components/H3Visualization/components/ui/SmartLoadingIndicator.jsx
import React, { useState, useEffect } from 'react';
import './SmartLoadingIndicator.css';

/**
 * Smart Loading Indicator - Progressive Loading UI
 * Показує прогрес завантаження 5 tier'ів з галочками та timing'ом
 */

// Status icons
const StatusIcon = ({ status, isRetrying }) => {
  switch (status) {
    case 'success':
      return <span className="status-icon success">✅</span>;
    case 'error':
      return <span className="status-icon error">❌</span>;
    case 'loading':
      return <span className="status-icon loading">⏳</span>;
    case 'retrying':
      return <span className="status-icon retrying">🔄</span>;
    default:
      return <span className="status-icon pending">⏸️</span>;
  }
};

// Tier progress bar
const TierProgressBar = ({ tier, isActive }) => {
  const completedDatasets = tier.datasets.filter(ds => ds.status === 'success').length;
  const totalDatasets = tier.datasets.length;
  const progress = totalDatasets > 0 ? (completedDatasets / totalDatasets) * 100 : 0;
  
  return (
    <div className={`tier-progress ${isActive ? 'active' : ''}`}>
      <div 
        className="tier-progress-fill" 
        style={{ width: `${progress}%` }}
      />
    </div>
  );
};

// Individual tier component
const TierItem = ({ tier, isActive, analytics }) => {
  const formatTime = (ms) => {
    if (!ms) return '--';
    return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`;
  };
  
  const getStatusText = () => {
    switch (tier.status) {
      case 'success':
        return `Завершено за ${formatTime(tier.totalTime)}`;
      case 'loading':
        return 'Завантаження...';
      case 'error':
        return 'Помилка';
      case 'retrying':
        return 'Повтор...';
      default:
        return 'Очікування';
    }
  };
  
  const retryCount = analytics.retryStats[`tier-${tier.id}`] || 0;
  
  return (
    <div className={`tier-item ${tier.status} ${isActive ? 'active' : ''}`}>
      <div className="tier-header">
        <StatusIcon status={tier.status} />
        <div className="tier-info">
          <div className="tier-title">
            Tier {tier.id}: {tier.name}
            {retryCount > 0 && (
              <span className="retry-badge">retry {retryCount}</span>
            )}
          </div>
          <div className="tier-status">{getStatusText()}</div>
        </div>
        <div className="tier-timing">
          {tier.totalTime && (
            <span className="load-time">{formatTime(tier.totalTime)}</span>
          )}
        </div>
      </div>
      
      <TierProgressBar tier={tier} isActive={isActive} />
      
      {/* Dataset details (expandable) */}
      <div className="datasets-container">
        {tier.datasets.map((dataset, index) => (
          <div key={index} className={`dataset-item ${dataset.status}`}>
            <span className="dataset-name">
              {dataset.metric} H3-{dataset.resolution}
            </span>
            <span className="dataset-status">
              <StatusIcon status={dataset.status} />
              {dataset.loadTime && (
                <span className="dataset-time">{formatTime(dataset.loadTime)}</span>
              )}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
};

// Main indicator component
const SmartLoadingIndicator = ({ 
  tiers, 
  currentTier, 
  isLoadingActive, 
  analytics,
  onRetry,
  onClose,
  isVisible = true 
}) => {
  const [isExpanded, setIsExpanded] = useState(true);
  const [showAnalytics, setShowAnalytics] = useState(false);
  
  // Auto-collapse after successful completion
  useEffect(() => {
    const allCompleted = tiers.every(tier => 
      tier.status === 'success' || tier.status === 'error'
    );
    
    if (allCompleted && isLoadingActive) {
      setTimeout(() => {
        setIsExpanded(false);
      }, 3000); // Auto-collapse after 3 seconds
    }
  }, [tiers, isLoadingActive]);
  
  if (!isVisible) return null;
  
  const completedTiers = tiers.filter(tier => tier.status === 'success').length;
  const errorTiers = tiers.filter(tier => tier.status === 'error').length;
  const totalProgress = (completedTiers / tiers.length) * 100;
  
  const formatTime = (ms) => {
    if (!ms) return '--';
    return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`;
  };
  
  return (
    <div className={`smart-loading-indicator ${isExpanded ? 'expanded' : 'collapsed'}`}>
      {/* Header */}
      <div className="indicator-header" onClick={() => setIsExpanded(!isExpanded)}>
        <div className="header-left">
          <h3>Smart Loading Progress</h3>
          <div className="progress-summary">
            {completedTiers}/{tiers.length} tier'ів завершено
            {errorTiers > 0 && (
              <span className="error-count"> ({errorTiers} помилок)</span>
            )}
          </div>
        </div>
        
        <div className="header-right">
          <div className="overall-progress">
            <div className="progress-circle">
              <svg viewBox="0 0 36 36" className="circular-chart">
                <path
                  className="circle-bg"
                  d="M18 2.0845
                    a 15.9155 15.9155 0 0 1 0 31.831
                    a 15.9155 15.9155 0 0 1 0 -31.831"
                />
                <path
                  className="circle"
                  strokeDasharray={`${totalProgress}, 100`}
                  d="M18 2.0845
                    a 15.9155 15.9155 0 0 1 0 31.831
                    a 15.9155 15.9155 0 0 1 0 -31.831"
                />
              </svg>
              <div className="progress-text">{Math.round(totalProgress)}%</div>
            </div>
          </div>
          
          <button 
            className="expand-toggle"
            onClick={(e) => {
              e.stopPropagation();
              setIsExpanded(!isExpanded);
            }}
          >
            {isExpanded ? '▼' : '▶'}
          </button>
        </div>
      </div>
      
      {/* Expanded content */}
      {isExpanded && (
        <div className="indicator-content">
          {/* Tier list */}
          <div className="tiers-list">
            {tiers.map((tier) => (
              <TierItem
                key={tier.id}
                tier={tier}
                isActive={currentTier === tier.id}
                analytics={analytics}
              />
            ))}
          </div>
          
          {/* Analytics toggle */}
          <div className="analytics-section">
            <button 
              className="analytics-toggle"
              onClick={() => setShowAnalytics(!showAnalytics)}
            >
              📊 {showAnalytics ? 'Сховати' : 'Показати'} аналітику
            </button>
            
            {showAnalytics && (
              <div className="analytics-details">
                <div className="analytics-grid">
                  <div className="analytics-item">
                    <span className="analytics-label">Перша взаємодія:</span>
                    <span className="analytics-value">
                      {formatTime(analytics.timeToFirstInteraction)}
                    </span>
                  </div>
                  <div className="analytics-item">
                    <span className="analytics-label">Повна функціональність:</span>
                    <span className="analytics-value">
                      {formatTime(analytics.timeToFullFunctionality)}
                    </span>
                  </div>
                  <div className="analytics-item">
                    <span className="analytics-label">Загальний час:</span>
                    <span className="analytics-value">
                      {formatTime(analytics.totalLoadTime)}
                    </span>
                  </div>
                  <div className="analytics-item">
                    <span className="analytics-label">Провалені tier'и:</span>
                    <span className="analytics-value">
                      {analytics.failedTiers.length || 'Немає'}
                    </span>
                  </div>
                </div>
              </div>
            )}
          </div>
          
          {/* Action buttons */}
          <div className="indicator-actions">
            {errorTiers > 0 && (
              <button className="retry-button" onClick={onRetry}>
                🔄 Повторити помилки ({errorTiers})
              </button>
            )}
            
            <button className="close-button" onClick={onClose}>
              ✕ Закрити
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

// Mini indicator for when main indicator is closed
export const MiniLoadingIndicator = ({ 
  tiers, 
  currentTier, 
  isLoadingActive,
  onClick 
}) => {
  if (!isLoadingActive) return null;
  
  const completedTiers = tiers.filter(tier => tier.status === 'success').length;
  const errorTiers = tiers.filter(tier => tier.status === 'error').length;
  
  return (
    <div className="mini-loading-indicator" onClick={onClick}>
      <div className="mini-progress">
        {tiers.map((tier) => (
          <div 
            key={tier.id}
            className={`mini-tier ${tier.status} ${currentTier === tier.id ? 'active' : ''}`}
            title={`Tier ${tier.id}: ${tier.name} - ${tier.status}`}
          >
            {tier.status === 'success' && '✅'}
            {tier.status === 'error' && '❌'}
            {tier.status === 'loading' && '⏳'}
            {tier.status === 'pending' && '⏸️'}
            {tier.status === 'retrying' && '🔄'}
          </div>
        ))}
      </div>
      <div className="mini-summary">
        {completedTiers}/{tiers.length}
        {errorTiers > 0 && ` (${errorTiers} errors)`}
      </div>
    </div>
  );
};

// Progress bar component for legacy compatibility
export const ProgressBarCompat = ({ 
  overallProgress, 
  currentStep, 
  isSmartLoading = false 
}) => {
  if (isSmartLoading) {
    return (
      <div className="progress-bar-compat smart">
        <div className="smart-progress-message">
          🚀 Smart Loading активний - детальний прогрес в панелі
        </div>
      </div>
    );
  }
  
  // Legacy progress bar
  return (
    <div className="progress-bar-compat legacy">
      <div className="progress-bar">
        <div 
          className="progress-fill" 
          style={{ width: `${overallProgress}%` }}
        />
      </div>
      <div className="progress-text">{currentStep}</div>
      <div className="progress-percentage">{overallProgress}%</div>
    </div>
  );
};

export default SmartLoadingIndicator;