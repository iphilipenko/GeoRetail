// src/components/H3Visualization/components/controls/MetricSwitcher.jsx
import React from 'react';

/**
 * MetricSwitcher - Компонент для перемикання між метриками
 * Винесений з головного файлу для кращої модульності
 */
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

export default MetricSwitcher;