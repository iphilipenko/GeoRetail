// src/components/H3Visualization/constants/colorSchemes.js

/**
 * Кольорові схеми для H3 візуалізації
 * Використовуємо Neon Glow схему як основну
 */

// Основні кольори для H3 гексагонів (без alpha каналу)
export const H3_COLOR_SCHEMES = {
  competition: {
    low: [46, 125, 50],        // Темно-зелений (найкраще)
    medium: [255, 193, 7],     // Янтарний
    high: [255, 111, 0],       // Темно-оранжевий
    maximum: [211, 47, 47]     // Темно-червоний (найгірше)
  },
  opportunity: {
    high: [103, 58, 183],      // Глибокий фіолетовий (найкраще)
    medium: [41, 121, 255],    // Яскравий синій
    low: [117, 117, 117]       // Темно-сірий (найгірше)
  }
};

// Neon Glow кольори для POI overlay
export const POI_COLORS = {
  // Основні категорії
  grocery: [50, 255, 126],      // #32FF7E - Neon Green
  fashion: [255, 56, 56],       // #FF3838 - Neon Red
  electronics: [55, 66, 250],   // #3742FA - Neon Blue
  restaurants: [255, 149, 0],   // #FF9500 - Neon Orange
  pharmacy: [46, 213, 115],     // #2ED573 - Neon Mint
  
  // Спеціальні категорії
  my_network: [255, 215, 0],    // #FFD700 - Golden Glow
  competitors: [255, 68, 68],   // #FF4444 - Danger Red
  anchors: [0, 210, 255],       // #00D2FF - Cyber Blue
  
  // Додаткові категорії
  office: [138, 43, 226],       // #8A2BE2 - Blue Violet
  education: [0, 191, 255],     // #00BFFF - Deep Sky Blue
  healthcare: [255, 20, 147],   // #FF1493 - Deep Pink
  transport: [50, 205, 50],     // #32CD32 - Lime Green
  entertainment: [255, 165, 0], // #FFA500 - Orange
  
  // Default fallback
  default: [200, 200, 200]      // Light gray
};

// Hex кольори для CSS (з # префіксом)
export const POI_HEX_COLORS = {
  grocery: '#32FF7E',
  fashion: '#FF3838', 
  electronics: '#3742FA',
  restaurants: '#FF9500',
  pharmacy: '#2ED573',
  my_network: '#FFD700',
  competitors: '#FF4444',
  anchors: '#00D2FF',
  office: '#8A2BE2',
  education: '#00BFFF',
  healthcare: '#FF1493',
  transport: '#32CD32',
  entertainment: '#FFA500',
  default: '#C8C8C8'
};

// Функція для отримання кольору з alpha каналом
export const getColorWithAlpha = (colorArray, alpha = 255) => {
  if (!Array.isArray(colorArray) || colorArray.length < 3) {
    return [200, 200, 200, alpha];
  }
  return [colorArray[0], colorArray[1], colorArray[2], alpha];
};

// Функція для конвертації RGB в hex
export const rgbToHex = (r, g, b) => {
  return "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
};

// Функція для отримання контрастного кольору тексту
export const getContrastTextColor = (bgColor) => {
  if (!Array.isArray(bgColor) || bgColor.length < 3) {
    return '#000000';
  }
  
  const [r, g, b] = bgColor;
  const brightness = (r * 299 + g * 587 + b * 114) / 1000;
  return brightness > 128 ? '#000000' : '#FFFFFF';
};

// Градієнти для 3D ефектів
export const GRADIENT_COLORS = {
  competition: {
    low: 'linear-gradient(135deg, #2e7d32 0%, #4caf50 100%)',
    medium: 'linear-gradient(135deg, #ffc107 0%, #ffeb3b 100%)',
    high: 'linear-gradient(135deg, #ff6f00 0%, #ff9800 100%)',
    maximum: 'linear-gradient(135deg, #d32f2f 0%, #f44336 100%)'
  },
  opportunity: {
    high: 'linear-gradient(135deg, #673ab7 0%, #9c27b0 100%)',
    medium: 'linear-gradient(135deg, #2979ff 0%, #448aff 100%)',
    low: 'linear-gradient(135deg, #757575 0%, #9e9e9e 100%)'
  }
};

// Налаштування освітлення для 3D режиму
export const LIGHTING_CONFIG = {
  ambient: 0.4,
  diffuse: 0.6, 
  shininess: 32,
  specularColor: [255, 255, 255]
};

// Експорт всіх кольорів для зручності
export default {
  H3_COLOR_SCHEMES,
  POI_COLORS,
  POI_HEX_COLORS,
  GRADIENT_COLORS,
  LIGHTING_CONFIG,
  getColorWithAlpha,
  rgbToHex,
  getContrastTextColor
};