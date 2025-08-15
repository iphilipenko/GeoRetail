// src/components/H3Visualization/components/ui/PreloadProgressBar.jsx
import React from 'react';

/**
 * Красивий прогрес-бар для завантаження H3 даних
 * Показує загальний прогрес, поточний запит і статистику
 */
const PreloadProgressBar = ({ 
  overallProgress,     // 0-100% загальний прогрес
  currentProgress,     // 0-100% поточного запиту
  completedRequests,   // Кількість завершених
  totalTasks,          // Загальна кількість
  currentStep, 
  error, 
  onRetry 
}) => {
  return (
    <div style={{
      position: 'fixed',
      top: 0,
      left: 0,
      right: 0,
      bottom: 0,
      backgroundColor: 'rgba(0, 0, 0, 0.9)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 10000,
      backdropFilter: 'blur(10px)'
    }}>
      <div style={{
        backgroundColor: 'white',
        borderRadius: '20px',
        padding: '40px',
        maxWidth: '500px',
        width: '90%',
        boxShadow: '0 20px 60px rgba(0,0,0,0.3)',
        textAlign: 'center'
      }}>
        {/* Заголовок */}
        <div style={{
          marginBottom: '30px'
        }}>
          <div style={{
            fontSize: '32px',
            marginBottom: '10px'
          }}>
            🗺️
          </div>
          <h2 style={{
            margin: '0 0 10px 0',
            fontSize: '24px',
            fontWeight: '700',
            color: '#2c3e50',
            background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
            WebkitBackgroundClip: 'text',
            WebkitTextFillColor: 'transparent',
            backgroundClip: 'text'
          }}>
            Завантаження Geo Intelligence
          </h2>
          <p style={{
            margin: 0,
            color: '#7f8c8d',
            fontSize: '16px'
          }}>
            Підготовка аналітичних даних по Київській області
          </p>
        </div>

        {/* Progress Bar з двома рівнями */}
        <div style={{
          marginBottom: '25px'
        }}>
          {/* Загальний прогрес */}
          <div style={{
            marginBottom: '15px'
          }}>
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              marginBottom: '8px',
              fontSize: '14px',
              fontWeight: '600',
              color: '#2c3e50'
            }}>
              <span>Загальний прогрес</span>
              <span>{completedRequests}/{totalTasks} datasets ({overallProgress}%)</span>
            </div>
            
            <div style={{
              width: '100%',
              height: '12px',
              backgroundColor: '#ecf0f1',
              borderRadius: '6px',
              overflow: 'hidden',
              position: 'relative',
              boxShadow: 'inset 0 2px 4px rgba(0,0,0,0.1)'
            }}>
              <div style={{
                width: `${overallProgress}%`,
                height: '100%',
                background: error 
                  ? 'linear-gradient(90deg, #e74c3c, #c0392b)'
                  : 'linear-gradient(90deg, #667eea, #764ba2)',
                borderRadius: '6px',
                transition: 'width 0.5s ease-in-out',
                position: 'relative',
                overflow: 'hidden'
              }}>
                {/* Animated shimmer effect для загального прогресу */}
                {!error && overallProgress < 100 && (
                  <div style={{
                    position: 'absolute',
                    top: 0,
                    left: '-100%',
                    width: '100%',
                    height: '100%',
                    background: 'linear-gradient(90deg, transparent, rgba(255,255,255,0.4), transparent)',
                    animation: 'shimmer 2s infinite'
                  }}></div>
                )}
              </div>
            </div>
          </div>

          {/* Прогрес поточного запиту */}
          {overallProgress < 100 && !error && (
            <div>
              <div style={{
                display: 'flex',
                justifyContent: 'space-between',
                marginBottom: '8px',
                fontSize: '13px',
                color: '#7f8c8d'
              }}>
                <span>Поточний запит</span>
                <span>{currentProgress}%</span>
              </div>
              
              <div style={{
                width: '100%',
                height: '8px',
                backgroundColor: '#f8f9fa',
                borderRadius: '4px',
                overflow: 'hidden',
                position: 'relative'
              }}>
                <div style={{
                  width: `${currentProgress}%`,
                  height: '100%',
                  background: 'linear-gradient(90deg, #48CAE4, #0077B6)',
                  borderRadius: '4px',
                  transition: 'width 0.3s ease-in-out'
                }}></div>
              </div>
            </div>
          )}
        </div>

        {/* Current Step */}
        <div style={{
          marginBottom: '20px',
          minHeight: '24px'
        }}>
          {error ? (
            <div style={{
              color: '#e74c3c',
              fontSize: '16px',
              fontWeight: '500',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '8px'
            }}>
              <span>❌</span>
              <span>Помилка завантаження</span>
            </div>
          ) : (
            <div style={{
              color: '#2c3e50',
              fontSize: '16px',
              fontWeight: '500',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              gap: '8px'
            }}>
              {overallProgress < 100 ? (
                <>
                  <div style={{
                    width: '16px',
                    height: '16px',
                    border: '2px solid #667eea',
                    borderTopColor: 'transparent',
                    borderRadius: '50%',
                    animation: 'spin 1s linear infinite'
                  }}></div>
                  <span>{currentStep}</span>
                </>
              ) : (
                <>
                  <span>✅</span>
                  <span>Завантаження завершено!</span>
                </>
              )}
            </div>
          )}
        </div>

        {/* Error details і retry button */}
        {error && (
          <div style={{
            backgroundColor: '#fdf2f2',
            border: '1px solid #fecaca',
            borderRadius: '8px',
            padding: '15px',
            marginBottom: '20px'
          }}>
            <div style={{
              fontSize: '14px',
              color: '#dc2626',
              marginBottom: '10px'
            }}>
              {error}
            </div>
            
            {onRetry && (
              <button
                onClick={onRetry}
                style={{
                  backgroundColor: '#667eea',
                  color: 'white',
                  border: 'none',
                  padding: '10px 20px',
                  borderRadius: '6px',
                  fontSize: '14px',
                  fontWeight: '500',
                  cursor: 'pointer',
                  transition: 'background-color 0.2s',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '8px',
                  margin: '0 auto'
                }}
                onMouseEnter={(e) => e.target.style.backgroundColor = '#5a67d8'}
                onMouseLeave={(e) => e.target.style.backgroundColor = '#667eea'}
              >
                <span>🔄</span>
                <span>Спробувати знову</span>
              </button>
            )}
          </div>
        )}

        {/* Fun facts про дані */}
        <div style={{
          fontSize: '13px',
          color: '#95a5a6',
          fontStyle: 'italic'
        }}>
          💡 Завантажено {completedRequests} з {totalTasks} datasets ({overallProgress < 100 ? 'до' : ''} 1 млн гексагонів)
        </div>
      </div>

      {/* CSS Animation */}
      <style>{`
        @keyframes spin {
          to { transform: rotate(360deg); }
        }
        
        @keyframes shimmer {
          0% { transform: translateX(-100%); }
          100% { transform: translateX(100%); }
        }
      `}</style>
    </div>
  );
};

export default PreloadProgressBar;