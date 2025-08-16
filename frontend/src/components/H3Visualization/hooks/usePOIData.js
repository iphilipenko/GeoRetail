// usePOIData.js
// 🏪 Hook для завантаження POI даних в гексагоні та сусідніх
// Використовує H3 k-ring для сусідів

import { useState, useEffect } from 'react';

const usePOIData = (h3Index, resolution, includeNeighbors = false) => {
  // TODO: Реалізувати POI data fetching з brands information
  
  return {
    poiData: null,
    loading: false,
    error: null
  };
};

export default usePOIData;
