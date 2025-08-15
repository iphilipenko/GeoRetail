// src/components/H3Visualization/hooks/usePreloadedH3Data.js
import { useState, useEffect, useRef, useMemo } from 'react';

/**
 * Hook для preloading всіх H3 даних і viewport culling
 * Завантажує ВСІ комбінації metric + resolution на початку
 * Показує тільки видимі в viewport гексагони
 */
const usePreloadedH3Data = (limit = 1000000) => {
  const [overallProgress, setOverallProgress] = useState(0);
  const [currentProgress, setCurrentProgress] = useState(0);
  const [completedRequests, setCompletedRequests] = useState(0);
  const [isPreloaded, setIsPreloaded] = useState(false);
  const [preloadError, setPreloadError] = useState(null);
  const [currentStep, setCurrentStep] = useState('');
  
  // Кеш для всіх завантажених даних
  const allDataRef = useRef({});
  
  // Всі комбінації для завантаження
  const preloadTasks = useMemo(() => [
    { metric: 'competition', resolution: 7, name: 'Конкуренція H3-7' },
    { metric: 'competition', resolution: 8, name: 'Конкуренція H3-8' },
    { metric: 'competition', resolution: 9, name: 'Конкуренція H3-9' },
    { metric: 'competition', resolution: 10, name: 'Конкуренція H3-10' },
    { metric: 'opportunity', resolution: 7, name: 'Можливості H3-7' },
    { metric: 'opportunity', resolution: 8, name: 'Можливості H3-8' },
    { metric: 'opportunity', resolution: 9, name: 'Можливості H3-9' },
    { metric: 'opportunity', resolution: 10, name: 'Можливості H3-10' }
  ], []);

  const totalTasks = preloadTasks.length;

  // Функція для завантаження одного dataset з прогресом
  const fetchSingleDataset = async (metric, resolution, taskIndex) => {
    const url = `http://localhost:8000/api/v1/visualization/kyiv-h3?metric_type=${metric}&resolution=${resolution}&limit=${limit}`;
    
    console.log(`🔄 [${taskIndex + 1}/${totalTasks}] Fetching ${metric} H3-${resolution}...`);
    
    // Симуляція прогресу завантаження (можна замінити на реальний прогрес)
    setCurrentProgress(0);
    
    const response = await fetch(url);
    
    setCurrentProgress(50); // Halfway через отримання response
    
    if (!response.ok) {
      throw new Error(`Failed to fetch ${metric} H3-${resolution}: ${response.status}`);
    }
    
    const data = await response.json();
    
    setCurrentProgress(100); // Complete current request
    
    console.log(`✅ [${taskIndex + 1}/${totalTasks}] Loaded ${metric} H3-${resolution}: ${data.hexagons?.length || 0} hexagons`);
    
    return data;
  };

  // ПОСЛІДОВНЕ завантаження для контрольованого прогресу
  const preloadAllData = async () => {
    try {
      setOverallProgress(0);
      setCurrentProgress(0);
      setCompletedRequests(0);
      setPreloadError(null);
      setCurrentStep('Ініціалізація...');
      
      console.log('🚀 Starting sequential preload of all H3 data...');
      
      // ПОСЛІДОВНО завантажуємо кожен dataset
      for (let i = 0; i < preloadTasks.length; i++) {
        const task = preloadTasks[i];
        
        try {
          // Оновлюємо поточний крок
          setCurrentStep(`Завантаження ${task.name}...`);
          setCurrentProgress(0);
          
          // Завантажуємо dataset
          const data = await fetchSingleDataset(task.metric, task.resolution, i);
          
          // Зберігаємо в кеш
          const cacheKey = `${task.metric}-${task.resolution}`;
          allDataRef.current[cacheKey] = data;
          
          // Оновлюємо прогрес
          const newCompletedRequests = i + 1;
          setCompletedRequests(newCompletedRequests);
          
          // Розраховуємо загальний прогрес
          const newOverallProgress = Math.round((newCompletedRequests / totalTasks) * 100);
          setOverallProgress(newOverallProgress);
          
          console.log(`📊 Progress: ${newCompletedRequests}/${totalTasks} (${newOverallProgress}%)`);
          
        } catch (error) {
          console.error(`❌ Failed to load ${task.metric} H3-${task.resolution}:`, error);
          // Продовжуємо завантаження інших datasets
          setPreloadError(`Помилка завантаження ${task.name}: ${error.message}`);
        }
      }
      
      // Перевіряємо результати
      const loadedDatasets = Object.keys(allDataRef.current).length;
      
      if (loadedDatasets > 0) {
        setIsPreloaded(true);
        setCurrentStep('Готово!');
        setCurrentProgress(100);
        console.log(`🎉 Preload completed! ${loadedDatasets}/${totalTasks} datasets loaded`);
        
        // Статистика по пам'яті
        const totalHexagons = Object.values(allDataRef.current).reduce((sum, data) => sum + (data.hexagons?.length || 0), 0);
        const estimatedMemory = Math.round(totalHexagons * 0.5 / 1024); // KB
        console.log(`📊 Total hexagons: ${totalHexagons}, Estimated memory: ~${estimatedMemory}KB`);
      } else {
        throw new Error('Не вдалося завантажити жодного dataset');
      }
      
    } catch (error) {
      console.error('❌ Preload failed:', error);
      setPreloadError(error.message);
      setIsPreloaded(false);
    }
  };

  // Запускаємо preload при ініціалізації
  useEffect(() => {
    preloadAllData();
  }, [limit]);

  // Функція для отримання даних з кешу
  const getCachedData = (metric, resolution) => {
    const cacheKey = `${metric}-${resolution}`;
    return allDataRef.current[cacheKey] || null;
  };

  // Viewport culling - фільтрація видимих гексагонів
  const getVisibleHexagons = (metric, resolution, viewState, bufferFactor = 0.75) => {
    const cachedData = getCachedData(metric, resolution);
    if (!cachedData?.hexagons) return [];

    // Якщо zoom дуже далекий - показуємо всі гексагони
    if (viewState.zoom <= 6) {
      return cachedData.hexagons;
    }

    try {
      // Розрахунок viewport bounds з buffer
      const { longitude, latitude, zoom } = viewState;
      
      // Приблизний розрахунок видимої області
      const latSpan = 360 / Math.pow(2, zoom) * bufferFactor;
      const lonSpan = latSpan * Math.cos(latitude * Math.PI / 180);
      
      const bounds = {
        north: latitude + latSpan,
        south: latitude - latSpan,
        east: longitude + lonSpan,
        west: longitude - lonSpan
      };

      // Фільтруємо гексагони в межах viewport
      const visibleHexagons = cachedData.hexagons.filter(hex => {
        if (!hex.geometry?.coordinates?.[0]) return false;
        
        // Перевіряємо центр гексагона
        const coords = hex.geometry.coordinates[0];
        const centerLon = coords.reduce((sum, coord) => sum + coord[0], 0) / coords.length;
        const centerLat = coords.reduce((sum, coord) => sum + coord[1], 0) / coords.length;
        
        return centerLat >= bounds.south && 
               centerLat <= bounds.north &&
               centerLon >= bounds.west && 
               centerLon <= bounds.east;
      });

      console.log(`🔍 Viewport culling: ${visibleHexagons.length}/${cachedData.hexagons.length} hexagons visible`);
      
      return visibleHexagons;
      
    } catch (error) {
      console.error('Error in viewport culling:', error);
      // Fallback - повертаємо всі гексагони
      return cachedData.hexagons;
    }
  };

  // Статистика завантажених даних
  const getStats = () => {
    const loadedKeys = Object.keys(allDataRef.current);
    const totalHexagons = loadedKeys.reduce((sum, key) => {
      return sum + (allDataRef.current[key].hexagons?.length || 0);
    }, 0);
    
    return {
      loadedDatasets: loadedKeys.length,
      totalDatasets: preloadTasks.length,
      totalHexagons,
      estimatedMemoryKB: Math.round(totalHexagons * 0.5)
    };
  };

  // Функція для примусового перезавантаження
  const reloadData = () => {
    allDataRef.current = {};
    setIsPreloaded(false);
    setOverallProgress(0);
    setCurrentProgress(0);
    setCompletedRequests(0);
    setPreloadError(null);
    preloadAllData();
  };

  return {
    // Стан завантаження з детальним прогресом
    isPreloaded,
    overallProgress,     // 0-100% загальний прогрес
    currentProgress,     // 0-100% поточного запиту  
    completedRequests,   // Кількість завершених запитів
    totalTasks,          // Загальна кількість запитів
    preloadError,
    currentStep,
    
    // Основні функції
    getVisibleHexagons,
    getCachedData,
    
    // Утиліти
    getStats,
    reloadData,
    
    // Debug
    allDataKeys: Object.keys(allDataRef.current)
  };
};

export default usePreloadedH3Data;