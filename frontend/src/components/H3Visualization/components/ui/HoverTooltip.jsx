// frontend/src/components/H3Visualization/components/ui/HoverTooltip.jsx
import React from 'react';

/**
 * Компактний і сучасний HoverTooltip з виправленою логікою кольорів
 */
const HoverTooltip = ({ hoveredObject, x, y }) => {
  if (!hoveredObject || x === undefined || y === undefined) {
    return null;
  }

  // ✅ ПРАВИЛЬНІ ПОЛЯ З API
  const {
    h3_index,
    display_category,
    competition_intensity,
    market_opportunity_score,
    transport_accessibility_score,
    residential_indicator_score,
    commercial_activity_score,
    poi_total_count,
    retail_count,
    competitor_count
  } = hoveredObject;

  // 🎯 ВИПРАВЛЕНА логіка кольорів для конкуренції (низька = зелений)
  const getCompetitionColor = (score) => {
    if (score >= 0.7) return '#ef4444';   // Висока конкуренція = червоний
    if (score >= 0.4) return '#f97316';   // Середня = помаранчевий
    if (score >= 0.2) return '#eab308';   // Помірна = жовтий
    return '#10b981';                     // Низька конкуренція = зелений
  };

  const getOpportunityColor = (score) => {
    if (score >= 0.7) return '#10b981';   // Високі можливості = зелений
    if (score >= 0.4) return '#22c55e';   
    if (score >= 0.2) return '#eab308';   
    return '#ef4444';                     // Низькі можливості = червоний
  };

  const formatNumber = (num) => {
    if (!num) return '0';
    return new Intl.NumberFormat('uk-UA').format(num);
  };

  const formatPercent = (score) => {
    return `${((score || 0) * 100).toFixed(0)}%`;
  };

  // 📐 Компактне позиціонування з відступом від курсору
  const tooltipWidth = 280;
  const tooltipHeight = 320;
  const offsetX = 25; // Збільшений відступ від курсору
  const offsetY = 15;
  
  const adjustedX = x + tooltipWidth + offsetX > window.innerWidth 
    ? x - tooltipWidth - offsetX 
    : x + offsetX;
  const adjustedY = y + tooltipHeight + offsetY > window.innerHeight 
    ? y - tooltipHeight - offsetY 
    : y + offsetY;

  return (
    <div style={{
      position: 'absolute',
      left: adjustedX,
      top: adjustedY,
      width: tooltipWidth,
      zIndex: 1001,
      pointerEvents: 'none',
      fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif'
    }}>
      {/* Компактна карточка */}
      <div style={{
        background: 'rgba(15, 23, 42, 0.95)',
        backdropFilter: 'blur(20px) saturate(180%)',
        borderRadius: '12px',
        boxShadow: '0 20px 40px -8px rgba(0, 0, 0, 0.4), 0 0 0 1px rgba(255, 255, 255, 0.1)',
        border: '1px solid rgba(255, 255, 255, 0.1)',
        overflow: 'hidden',
        color: 'white'
      }}>
        
        {/* Компактний заголовок */}
        <div style={{
          padding: '12px 16px',
          borderBottom: '1px solid rgba(255, 255, 255, 0.1)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between'
        }}>
          <div style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px'
          }}>
            <div style={{
              width: '8px',
              height: '8px',
              borderRadius: '50%',
              background: display_category === 'high' ? '#10b981' : 
                         display_category === 'medium' ? '#eab308' : '#ef4444',
              boxShadow: `0 0 8px ${display_category === 'high' ? '#10b981' : 
                                  display_category === 'medium' ? '#eab308' : '#ef4444'}40`
            }}></div>
            <span style={{
              fontSize: '13px',
              fontWeight: '600',
              opacity: 0.9
            }}>
              H3 • {h3_index?.slice(-6) || 'N/A'}
            </span>
          </div>
          <div style={{
            fontSize: '10px',
            padding: '2px 6px',
            borderRadius: '4px',
            backgroundColor: 'rgba(255, 255, 255, 0.1)',
            textTransform: 'uppercase',
            letterSpacing: '0.5px',
            fontWeight: '600'
          }}>
            {display_category || 'N/A'}
          </div>
        </div>

        {/* Головні метрики в 2 колонки */}
        <div style={{
          padding: '16px',
          display: 'grid',
          gap: '12px'
        }}>
          
          {/* Топ метрики */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: '1fr 1fr',
            gap: '12px'
          }}>
            {/* Можливості */}
            <div style={{
              background: 'rgba(16, 185, 129, 0.1)',
              borderRadius: '8px',
              padding: '12px',
              border: '1px solid rgba(16, 185, 129, 0.2)',
              textAlign: 'center'
            }}>
              <div style={{
                fontSize: '10px',
                color: '#10b981',
                fontWeight: '600',
                marginBottom: '4px',
                textTransform: 'uppercase',
                letterSpacing: '0.5px'
              }}>
                🎯 Можливості
              </div>
              <div style={{
                fontSize: '18px',
                fontWeight: '700',
                color: getOpportunityColor(market_opportunity_score || 0)
              }}>
                {(market_opportunity_score || 0).toFixed(2)}
              </div>
            </div>

            {/* Конкуренція */}
            <div style={{
              background: 'rgba(239, 68, 68, 0.1)',
              borderRadius: '8px',
              padding: '12px',
              border: '1px solid rgba(239, 68, 68, 0.2)',
              textAlign: 'center'
            }}>
              <div style={{
                fontSize: '10px',
                color: '#ef4444',
                fontWeight: '600',
                marginBottom: '4px',
                textTransform: 'uppercase',
                letterSpacing: '0.5px'
              }}>
                ⚔️ Конкуренція
              </div>
              <div style={{
                fontSize: '18px',
                fontWeight: '700',
                color: getCompetitionColor(competition_intensity || 0)
              }}>
                {formatPercent(competition_intensity)}
              </div>
            </div>
          </div>

          {/* Компактні прогрес-бари */}
          <div style={{
            display: 'grid',
            gap: '8px'
          }}>
            {/* Транспорт */}
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px'
            }}>
              <div style={{
                fontSize: '12px',
                color: '#60a5fa',
                width: '65px',
                fontWeight: '500'
              }}>
                🚌 Транспорт
              </div>
              <div style={{
                flex: 1,
                height: '6px',
                backgroundColor: 'rgba(96, 165, 250, 0.2)',
                borderRadius: '3px',
                overflow: 'hidden'
              }}>
                <div style={{
                  width: `${(transport_accessibility_score || 0) * 100}%`,
                  height: '100%',
                  background: 'linear-gradient(90deg, #3b82f6, #60a5fa)',
                  borderRadius: '3px',
                  transition: 'width 0.4s ease'
                }}></div>
              </div>
              <div style={{
                fontSize: '11px',
                color: '#9ca3af',
                width: '30px',
                textAlign: 'right'
              }}>
                {formatPercent(transport_accessibility_score)}
              </div>
            </div>

            {/* Житло */}
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px'
            }}>
              <div style={{
                fontSize: '12px',
                color: '#a855f7',
                width: '65px',
                fontWeight: '500'
              }}>
                🏠 Житло
              </div>
              <div style={{
                flex: 1,
                height: '6px',
                backgroundColor: 'rgba(168, 85, 247, 0.2)',
                borderRadius: '3px',
                overflow: 'hidden'
              }}>
                <div style={{
                  width: `${(residential_indicator_score || 0) * 100}%`,
                  height: '100%',
                  background: 'linear-gradient(90deg, #9333ea, #a855f7)',
                  borderRadius: '3px',
                  transition: 'width 0.4s ease'
                }}></div>
              </div>
              <div style={{
                fontSize: '11px',
                color: '#9ca3af',
                width: '30px',
                textAlign: 'right'
              }}>
                {formatPercent(residential_indicator_score)}
              </div>
            </div>

            {/* Комерція */}
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px'
            }}>
              <div style={{
                fontSize: '12px',
                color: '#fb923c',
                width: '65px',
                fontWeight: '500'
              }}>
                🏢 Бізнес
              </div>
              <div style={{
                flex: 1,
                height: '6px',
                backgroundColor: 'rgba(251, 146, 60, 0.2)',
                borderRadius: '3px',
                overflow: 'hidden'
              }}>
                <div style={{
                  width: `${(commercial_activity_score || 0) * 100}%`,
                  height: '100%',
                  background: 'linear-gradient(90deg, #f97316, #fb923c)',
                  borderRadius: '3px',
                  transition: 'width 0.4s ease'
                }}></div>
              </div>
              <div style={{
                fontSize: '11px',
                color: '#9ca3af',
                width: '30px',
                textAlign: 'right'
              }}>
                {formatPercent(commercial_activity_score)}
              </div>
            </div>
          </div>

          {/* Компактна статистика */}
          <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(3, 1fr)',
            gap: '8px',
            marginTop: '8px',
            paddingTop: '12px',
            borderTop: '1px solid rgba(255, 255, 255, 0.1)'
          }}>
            <div style={{ textAlign: 'center' }}>
              <div style={{ 
                fontSize: '10px', 
                color: '#10b981', 
                fontWeight: '600',
                marginBottom: '2px'
              }}>
                POI
              </div>
              <div style={{ 
                fontSize: '14px', 
                fontWeight: '600', 
                color: 'white'
              }}>
                {formatNumber(poi_total_count || 0)}
              </div>
            </div>

            <div style={{ textAlign: 'center' }}>
              <div style={{ 
                fontSize: '10px', 
                color: '#eab308', 
                fontWeight: '600',
                marginBottom: '2px'
              }}>
                RETAIL
              </div>
              <div style={{ 
                fontSize: '14px', 
                fontWeight: '600', 
                color: 'white'
              }}>
                {formatNumber(retail_count || 0)}
              </div>
            </div>

            <div style={{ textAlign: 'center' }}>
              <div style={{ 
                fontSize: '10px', 
                color: '#ef4444', 
                fontWeight: '600',
                marginBottom: '2px'
              }}>
                RIVALS
              </div>
              <div style={{ 
                fontSize: '14px', 
                fontWeight: '600', 
                color: getCompetitionColor(competitor_count / 10) // Нормалізуємо для кольору
              }}>
                {formatNumber(competitor_count || 0)}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Стрілочка до курсору */}
      <div style={{
        position: 'absolute',
        top: x + tooltipWidth + 25 > window.innerWidth ? '20px' : 'auto',
        bottom: x + tooltipWidth + 25 > window.innerWidth ? 'auto' : '20px',
        left: x + tooltipWidth + 25 > window.innerWidth ? '100%' : '-6px',
        right: x + tooltipWidth + 25 > window.innerWidth ? '-6px' : 'auto',
        width: '12px',
        height: '12px',
        background: 'rgba(15, 23, 42, 0.95)',
        border: '1px solid rgba(255, 255, 255, 0.1)',
        borderTop: x + tooltipWidth + 25 > window.innerWidth ? '1px solid rgba(255, 255, 255, 0.1)' : 'none',
        borderLeft: x + tooltipWidth + 25 > window.innerWidth ? '1px solid rgba(255, 255, 255, 0.1)' : 'none',
        borderRight: x + tooltipWidth + 25 > window.innerWidth ? 'none' : '1px solid rgba(255, 255, 255, 0.1)',
        borderBottom: x + tooltipWidth + 25 > window.innerWidth ? 'none' : '1px solid rgba(255, 255, 255, 0.1)',
        transform: x + tooltipWidth + 25 > window.innerWidth 
          ? 'rotate(135deg)' 
          : 'rotate(-45deg)',
        zIndex: -1
      }}></div>
    </div>
  );
};

export default HoverTooltip;