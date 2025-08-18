// frontend/src/components/H3Visualization/components/ui/SmartLoadingIndicator.jsx

import React from 'react';

/**
 * 🎨 Smart Loading Indicator - Progressive UI Component
 * 
 * Показує різні індикатори в залежності від стану завантаження:
 * - Tier 1: Instant Loading (червоний) - базове завантаження
 * - Tier 2: Enhancing Functionality (оранжевий) - покращення функціональності
 * - Tier 3: Background Optimization (синій) - оптимізація в фоні
 * - Complete: Success State (зелений) - готово
 */

// ===============================================
// MAIN SMART LOADING INDICATOR
// ===============================================

const SmartLoadingIndicator = ({ 
  loadingTiers, 
  isBasicReady, 
  isFullyFunctional, 
  isCompletelyLoaded,
  performanceMetrics = {},
  currentActivity = '',
  debugMode = false 
}) => {
  
  // Complete state - система готова
  if (isCompletelyLoaded) {
    return (
      <div style={{
        position: 'absolute',
        top: '20px',
        left: '20px',
        background: 'linear-gradient(135deg, rgba(50, 255, 126, 0.95), rgba(46, 213, 115, 0.9))',
        color: 'white',
        padding: '12px 18px',
        borderRadius: '12px',
        fontSize: '14px',
        fontWeight: '600',
        boxShadow: '0 8px 25px rgba(50, 255, 126, 0.4)',
        border: '1px solid rgba(50, 255, 126, 0.6)',
        backdropFilter: 'blur(10px)',
        zIndex: 1000,
        minWidth: '280px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{ 
            width: '20px', 
            height: '20px', 
            background: 'white',
            borderRadius: '50%',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            fontSize: '12px',
            color: '#32ff7e'
          }}>✓</div>
          <span>🎉 Система готова до роботи</span>
        </div>
        {performanceMetrics.timeToInteractive && (
          <div style={{ 
            fontSize: '12px', 
            opacity: 0.9, 
            marginTop: '6px',
            display: 'flex',
            justifyContent: 'space-between'
          }}>
            <span>⚡ Готовність: {performanceMetrics.timeToInteractive}мс</span>
            <span>📊 Повних даних: {Math.round(performanceMetrics.dataUtilizationRate || 0)}%</span>
          </div>
        )}
      </div>
    );
  }
  
  // Tier 3: Background optimization - повна функціональність доступна
  if (isFullyFunctional) {
    return (
      <div style={{
        position: 'absolute',
        top: '20px',
        left: '20px',
        background: 'linear-gradient(135deg, rgba(55, 66, 250, 0.95), rgba(116, 185, 255, 0.9))',
        color: 'white',
        padding: '12px 18px',
        borderRadius: '12px',
        fontSize: '14px',
        fontWeight: '500',
        boxShadow: '0 8px 25px rgba(55, 66, 250, 0.3)',
        border: '1px solid rgba(55, 66, 250, 0.5)',
        backdropFilter: 'blur(10px)',
        zIndex: 1000,
        minWidth: '280px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            background: 'rgba(255,255,255,0.3)',
            borderRadius: '50%',
            position: 'relative',
            overflow: 'hidden'
          }}>
            <div style={{
              width: `${loadingTiers.tier3?.progress || 0}%`,
              height: '100%',
              background: 'white',
              transition: 'width 0.3s ease',
              borderRadius: '50%'
            }} />
          </div>
          <span>⚡ Повна функціональність доступна</span>
        </div>
        <div style={{ 
          fontSize: '12px', 
          opacity: 0.9, 
          marginTop: '6px',
          display: 'flex',
          justifyContent: 'space-between'
        }}>
          <span>📦 Оптимізація в фоні... {Math.round(loadingTiers.tier3?.progress || 0)}%</span>
          {performanceMetrics.timeToFullyFunctional && (
            <span>🚀 {performanceMetrics.timeToFullyFunctional}мс</span>
          )}
        </div>
      </div>
    );
  }
  
  // Tier 2: Enhancing functionality - базова готовність досягнута
  if (isBasicReady) {
    return (
      <div style={{
        position: 'absolute',
        top: '20px',
        left: '20px',
        background: 'linear-gradient(135deg, rgba(255, 179, 71, 0.95), rgba(255, 202, 40, 0.9))',
        color: 'white',
        padding: '12px 18px',
        borderRadius: '12px',
        fontSize: '14px',
        fontWeight: '500',
        boxShadow: '0 8px 25px rgba(255, 179, 71, 0.3)',
        border: '1px solid rgba(255, 179, 71, 0.5)',
        backdropFilter: 'blur(10px)',
        zIndex: 1000,
        minWidth: '280px'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div style={{ 
            width: '16px', 
            height: '16px', 
            border: '2px solid rgba(255,255,255,0.4)',
            borderTop: '2px solid white',
            borderRadius: '50%',
            animation: 'spin 1s linear infinite'
          }} />
          <span>Покращення функціональності...</span>
        </div>
        <div style={{ 
          fontSize: '12px', 
          opacity: 0.9, 
          marginTop: '6px' 
        }}>
          <div style={{ marginBottom: '2px' }}>✅ Базовий перегляд доступний</div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span>🔄 Завантаження додаткових даних... {Math.round(loadingTiers.tier2?.progress || 0)}%</span>
            {performanceMetrics.timeToInteractive && (
              <span>⚡ {performanceMetrics.timeToInteractive}мс</span>
            )}
          </div>
        </div>
        <style jsx>{`
          @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
          }
        `}</style>
      </div>
    );
  }
  
  // Tier 1: Initial loading - самий початок
  return (
    <div style={{
      position: 'absolute',
      top: '20px',
      left: '20px',
      background: 'linear-gradient(135deg, rgba(255, 107, 107, 0.95), rgba(255, 71, 87, 0.9))',
      color: 'white',
      padding: '12px 18px',
      borderRadius: '12px',
      fontSize: '14px',
      fontWeight: '500',
      boxShadow: '0 8px 25px rgba(255, 107, 107, 0.3)',
      border: '1px solid rgba(255, 107, 107, 0.5)',
      backdropFilter: 'blur(10px)',
      zIndex: 1000,
      minWidth: '280px'
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
        <div style={{ 
          width: '16px', 
          height: '16px', 
          background: 'white',
          borderRadius: '50%',
          animation: 'pulse 1.5s ease-in-out infinite'
        }} />
        <span>Завантаження базових даних...</span>
      </div>
      <div style={{ 
        fontSize: '12px', 
        opacity: 0.9, 
        marginTop: '6px',
        display: 'flex',
        justifyContent: 'space-between'
      }}>
        <span>⚡ {Math.round(loadingTiers.tier1?.progress || 0)}% (~2 секунди)</span>
        <span>🎯 opportunity • H3-8</span>
      </div>
      {loadingTiers.tier1?.error && (
        <div style={{ 
          fontSize: '11px', 
          marginTop: '4px', 
          color: '#ffcccc',
          background: 'rgba(255,255,255,0.1)',
          padding: '4px 8px',
          borderRadius: '6px'
        }}>
          ⚠️ {loadingTiers.tier1.error}
        </div>
      )}
      {debugMode && (
        <div style={{ 
          fontSize: '10px', 
          marginTop: '4px', 
          opacity: 0.7,
          fontFamily: 'monospace'
        }}>
          🐛 Activity: {currentActivity}
        </div>
      )}
      <style jsx>{`
        @keyframes pulse {
          0%, 100% { opacity: 1; transform: scale(1); }
          50% { opacity: 0.6; transform: scale(0.9); }
        }
      `}</style>
    </div>
  );
};

// ===============================================
// MINI PROGRESS INDICATOR (for sidebar/compact spaces)
// ===============================================

export const MiniProgressIndicator = ({ 
  loadingTiers, 
  isBasicReady, 
  isFullyFunctional, 
  isCompletelyLoaded 
}) => {
  const getTierStatus = () => {
    if (isCompletelyLoaded) return { tier: 3, color: '#32ff7e', text: 'Готово' };
    if (isFullyFunctional) return { tier: 3, color: '#3742fa', text: 'Оптимізація' };
    if (isBasicReady) return { tier: 2, color: '#ffb347', text: 'Покращення' };
    return { tier: 1, color: '#ff6b6b', text: 'Завантаження' };
  };

  const { tier, color, text } = getTierStatus();
  const progress = loadingTiers[`tier${tier}`]?.progress || 0;

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      gap: '8px',
      padding: '6px 10px',
      background: `${color}20`,
      border: `1px solid ${color}40`,
      borderRadius: '8px',
      fontSize: '12px',
      fontWeight: '500'
    }}>
      <div style={{
        width: '12px',
        height: '12px',
        background: `${color}30`,
        borderRadius: '50%',
        position: 'relative',
        overflow: 'hidden'
      }}>
        <div style={{
          width: `${progress}%`,
          height: '100%',
          background: color,
          transition: 'width 0.3s ease',
          borderRadius: '50%'
        }} />
      </div>
      <span style={{ color }}>{text}</span>
      <span style={{ color: `${color}80`, fontSize: '10px' }}>
        {Math.round(progress)}%
      </span>
    </div>
  );
};

// ===============================================
// LOADING STATES DEBUGGER (development only)
// ===============================================

export const LoadingStatesDebugger = ({ 
  loadingTiers, 
  performanceMetrics, 
  debugInfo 
}) => {
  if (process.env.NODE_ENV !== 'development') return null;

  return (
    <div style={{
      position: 'absolute',
      bottom: '20px',
      right: '20px',
      background: 'rgba(0, 0, 0, 0.9)',
      color: '#00ff41',
      padding: '12px',
      borderRadius: '8px',
      fontSize: '11px',
      fontFamily: 'monospace',
      maxWidth: '400px',
      zIndex: 1000,
      border: '1px solid #00ff41'
    }}>
      <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>
        🐛 Smart Loading Debug Panel
      </div>
      
      {/* Tier States */}
      <div style={{ marginBottom: '8px' }}>
        <div style={{ color: '#ffff00' }}>📊 Tier States:</div>
        {Object.entries(loadingTiers).map(([tier, state]) => (
          <div key={tier} style={{ marginLeft: '10px' }}>
            {tier}: {state.status} ({state.progress}%)
            {state.error && <span style={{ color: '#ff6b6b' }}> - {state.error}</span>}
          </div>
        ))}
      </div>

      {/* Performance Metrics */}
      <div style={{ marginBottom: '8px' }}>
        <div style={{ color: '#ffff00' }}>⚡ Performance:</div>
        <div style={{ marginLeft: '10px' }}>
          Interactive: {performanceMetrics.timeToInteractive || 'N/A'}ms
        </div>
        <div style={{ marginLeft: '10px' }}>
          Functional: {performanceMetrics.timeToFullyFunctional || 'N/A'}ms
        </div>
        <div style={{ marginLeft: '10px' }}>
          Complete: {performanceMetrics.timeToComplete || 'N/A'}ms
        </div>
      </div>

      {/* Cache Info */}
      <div>
        <div style={{ color: '#ffff00' }}>💾 Cache:</div>
        <div style={{ marginLeft: '10px' }}>
          Size: {debugInfo?.cacheSize || 0} entries
        </div>
        <div style={{ marginLeft: '10px' }}>
          Keys: {debugInfo?.cacheKeys?.slice(0, 3).join(', ') || 'none'}
          {debugInfo?.cacheKeys?.length > 3 && '...'}
        </div>
      </div>
    </div>
  );
};

// ===============================================
// LEGACY COMPATIBILITY WRAPPER
// ===============================================

export const ProgressBarCompat = ({ 
  overallProgress, 
  currentStep, 
  isLegacyMode = false 
}) => {
  // Wrapper для compatibility з існуючим PreloadProgressBar
  if (isLegacyMode) {
    return (
      <div style={{
        position: 'absolute',
        top: '20px',
        left: '20px',
        background: 'rgba(255, 255, 255, 0.95)',
        padding: '15px',
        borderRadius: '8px',
        boxShadow: '0 4px 15px rgba(0,0,0,0.1)',
        minWidth: '300px',
        zIndex: 1000
      }}>
        <div style={{ marginBottom: '10px', fontSize: '14px', fontWeight: '500' }}>
          {currentStep || 'Завантаження...'}
        </div>
        <div style={{
          width: '100%',
          height: '8px',
          background: '#e0e0e0',
          borderRadius: '4px',
          overflow: 'hidden'
        }}>
          <div style={{
            width: `${overallProgress || 0}%`,
            height: '100%',
            background: 'linear-gradient(90deg, #4CAF50, #45a049)',
            transition: 'width 0.3s ease'
          }} />
        </div>
        <div style={{ 
          marginTop: '8px', 
          fontSize: '12px', 
          color: '#666',
          textAlign: 'right'
        }}>
          {Math.round(overallProgress || 0)}%
        </div>
      </div>
    );
  }

  return null;
};

export default SmartLoadingIndicator;