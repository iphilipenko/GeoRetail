// src/components/H3Visualization/hooks/useDynamicOpacity.js
import { useState, useCallback, useMemo } from 'react';

/**
 * Hook для управління динамічною прозорістю гексагонів
 * Адаптує прозорість залежно від zoom рівня та hover стану
 */
const useDynamicOpacity = (viewState) => {
  const [hoveredHexagon, setHoveredHexagon] = useState(null);
  const [focusMode, setFocusMode] = useState(false);
  const [focusedHexagon, setFocusedHexagon] = useState(null);

  // Базова прозорість залежно від zoom
  const baseOpacity = useMemo(() => {
    const zoom = viewState?.zoom || 9;
    
    // Формула: чим більший zoom, тим менш прозорі гексагони
    const minOpacity = 80;  // Мінімальна прозорість (далекий zoom)
    const maxOpacity = 180; // Максимальна прозорість (близький zoom)
    
    // Smooth interpolation between min and max
    const normalizedZoom = Math.max(0, Math.min(1, (zoom - 7) / (12 - 7)));
    const opacity = Math.floor(minOpacity + (maxOpacity - minOpacity) * normalizedZoom);
    
    return opacity;
  }, [viewState?.zoom]);

  // Функція для отримання прозорості конкретного гексагона
  const getHexagonOpacity = useCallback((hexagonId) => {
    let opacity = baseOpacity;

    // Focus mode - приглушити всі крім вибраного
    if (focusMode && focusedHexagon) {
      if (hexagonId === focusedHexagon) {
        opacity = Math.min(220, opacity + 40); // Вибраний яскравіший
      } else {
        opacity = Math.max(40, opacity - 60);  // Інші приглушені
      }
    }

    // Hover effect - збільшити прозорість при наведенні
    if (hoveredHexagon === hexagonId) {
      opacity = Math.min(255, opacity + 50);
    }

    return opacity;
  }, [baseOpacity, hoveredHexagon, focusMode, focusedHexagon]);

  // Функція для отримання кольору з правильною прозорістю
  const getHexagonColor = useCallback((color, hexagonId) => {
    if (!Array.isArray(color) || color.length < 3) {
      return [200, 200, 200, baseOpacity];
    }

    const opacity = getHexagonOpacity(hexagonId);
    return [color[0], color[1], color[2], opacity];
  }, [getHexagonOpacity, baseOpacity]);

  // Обробники подій
  const handleHexagonHover = useCallback((hexagonId) => {
    setHoveredHexagon(hexagonId);
  }, []);

  const handleHexagonLeave = useCallback(() => {
    setHoveredHexagon(null);
  }, []);

  const handleHexagonClick = useCallback((hexagonId) => {
    if (focusMode && focusedHexagon === hexagonId) {
      // Скасувати focus якщо клікнули на той самий
      setFocusMode(false);
      setFocusedHexagon(null);
    } else {
      // Увімкнути focus на новий гексагон
      setFocusMode(true);
      setFocusedHexagon(hexagonId);
    }
  }, [focusMode, focusedHexagon]);

  const clearFocus = useCallback(() => {
    setFocusMode(false);
    setFocusedHexagon(null);
  }, []);

  // Отримання transition параметрів для smooth анімацій
  const getTransitionSettings = useMemo(() => ({
    getFillColor: {
      duration: 300,
      easing: 'ease-out'
    },
    getLineColor: {
      duration: 200,
      easing: 'ease-out'
    }
  }), []);

  // Debug info для розробки
  const debugInfo = useMemo(() => ({
    baseOpacity,
    zoom: viewState?.zoom || 0,
    hoveredHexagon,
    focusMode,
    focusedHexagon
  }), [baseOpacity, viewState?.zoom, hoveredHexagon, focusMode, focusedHexagon]);

  return {
    // Основні функції
    getHexagonColor,
    getHexagonOpacity,
    
    // Обробники подій
    handleHexagonHover,
    handleHexagonLeave,
    handleHexagonClick,
    clearFocus,
    
    // Стан
    hoveredHexagon,
    focusMode,
    focusedHexagon,
    baseOpacity,
    
    // Налаштування
    transitionSettings: getTransitionSettings,
    
    // Debug
    debugInfo
  };
};

export default useDynamicOpacity;