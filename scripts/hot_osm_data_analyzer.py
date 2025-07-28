#!/usr/bin/env python3
"""
Corrected HOT OSM Data Analyzer
Виправлений аналізатор для HOT OSM експортів з правильним розумінням структури
"""

import geopandas as gpd
import pandas as pd
import json
import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import logging
from collections import defaultdict, Counter
import numpy as np
from datetime import datetime
import ast
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CorrectedHOTOSMAnalyzer:
    """Виправлений аналізатор для HOT OSM експортів"""
    
    def __init__(self, data_directory: str):
        self.data_directory = Path(data_directory)
        self.analysis_results = {}
        
    def analyze_hot_osm_file(self, gpkg_path: Path, sample_size: int = 10000) -> Dict[str, Any]:
        """Повний аналіз HOT OSM файлу з правильним розумінням структури"""
        
        logger.info(f"📊 Аналіз HOT OSM файлу: {gpkg_path.name}")
        
        analysis = {
            'file_info': {
                'filename': gpkg_path.name,
                'size_mb': round(gpkg_path.stat().st_size / (1024 * 1024), 2),
                'region_name': self._extract_region_name(gpkg_path.name)
            },
            'data_structure': {},
            'osm_content_analysis': {},
            'spatial_analysis': {},
            'tag_analysis': {},
            'geometry_analysis': {},
            'postgis_schema_recommendations': {},
            'h3_integration_plan': {},
            'performance_estimates': {}
        }
        
        try:
            # 1. Отримання основної інформації про таблицю
            table_name = self._get_main_table_name(gpkg_path)
            analysis['data_structure'] = self._analyze_table_structure(gpkg_path, table_name)
            
            # 2. Аналіз вибірки даних
            sample_data = self._load_data_sample(gpkg_path, table_name, sample_size)
            
            if not sample_data.empty:
                # 3. Аналіз OSM контенту
                analysis['osm_content_analysis'] = self._analyze_osm_content(sample_data)
                
                # 4. Просторовий аналіз
                analysis['spatial_analysis'] = self._analyze_spatial_data(sample_data)
                
                # 5. Аналіз тегів
                analysis['tag_analysis'] = self._analyze_osm_tags(sample_data)
                
                # 6. Аналіз геометрії
                analysis['geometry_analysis'] = self._analyze_geometry_distribution(sample_data)
                
                # 7. Рекомендації PostGIS схеми
                analysis['postgis_schema_recommendations'] = self._create_postgis_schema_recommendations(
                    analysis, table_name
                )
                
                # 8. План H3 інтеграції
                analysis['h3_integration_plan'] = self._create_h3_integration_plan(analysis)
                
                # 9. Оцінки продуктивності
                analysis['performance_estimates'] = self._estimate_performance(analysis)
            
        except Exception as e:
            logger.error(f"❌ Помилка аналізу {gpkg_path.name}: {e}")
            analysis['error'] = str(e)
            
        return analysis
    
    def _extract_region_name(self, filename: str) -> str:
        """Витягування назви регіону з назви файлу"""
        if filename.startswith('UA_MAP_'):
            region_name = filename[7:]  # видаляємо 'UA_MAP_'
        else:
            region_name = filename
        
        if region_name.endswith('.gpkg'):
            region_name = region_name[:-5]  # видаляємо '.gpkg'
        
        return region_name
    
    def _get_main_table_name(self, gpkg_path: Path) -> str:
        """Отримання назви основної таблиці з даними"""
        try:
            with sqlite3.connect(gpkg_path) as conn:
                cursor = conn.cursor()
                
                # Шукаємо таблицю з максимальною кількістю записів (крім службових)
                cursor.execute("""
                    SELECT name FROM sqlite_master 
                    WHERE type='table' 
                    AND name NOT LIKE 'gpkg_%' 
                    AND name NOT LIKE 'rtree_%' 
                    AND name NOT LIKE 'sqlite_%'
                    ORDER BY name
                """)
                
                tables = cursor.fetchall()
                if tables:
                    # Повертаємо першу знайдену таблицю (зазвичай це UA_MAP_*)
                    return tables[0][0]
                else:
                    raise ValueError("Не знайдено основну таблицю з даними")
                    
        except Exception as e:
            logger.error(f"Помилка отримання назви таблиці: {e}")
            return "unknown_table"
    
    def _analyze_table_structure(self, gpkg_path: Path, table_name: str) -> Dict[str, Any]:
        """Аналіз структури основної таблиці"""
        
        structure = {
            'table_name': table_name,
            'total_records': 0,
            'columns': {},
            'indexes': [],
            'spatial_info': {}
        }
        
        try:
            with sqlite3.connect(gpkg_path) as conn:
                cursor = conn.cursor()
                
                # Загальна кількість записів
                cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
                structure['total_records'] = cursor.fetchone()[0]
                
                # Структура колонок
                cursor.execute(f"PRAGMA table_info(\"{table_name}\")")
                columns_info = cursor.fetchall()
                
                for col_info in columns_info:
                    col_name = col_info[1]
                    structure['columns'][col_name] = {
                        'type': col_info[2],
                        'nullable': not col_info[3],
                        'default': col_info[4],
                        'primary_key': bool(col_info[5])
                    }
                
                # Просторова інформація з gpkg_geometry_columns
                cursor.execute("""
                    SELECT geometry_type_name, srs_id 
                    FROM gpkg_geometry_columns 
                    WHERE table_name = ?
                """, (table_name,))
                
                spatial_result = cursor.fetchone()
                if spatial_result:
                    structure['spatial_info'] = {
                        'geometry_type': spatial_result[0],
                        'srs_id': spatial_result[1],
                        'has_spatial_index': True  # R-tree завжди є в GPKG
                    }
                
                # Bounds з gpkg_contents
                cursor.execute("""
                    SELECT min_x, min_y, max_x, max_y 
                    FROM gpkg_contents 
                    WHERE table_name = ?
                """, (table_name,))
                
                bounds_result = cursor.fetchone()
                if bounds_result and all(b is not None for b in bounds_result):
                    structure['spatial_info']['bounds'] = {
                        'minx': bounds_result[0],
                        'miny': bounds_result[1],
                        'maxx': bounds_result[2],
                        'maxy': bounds_result[3]
                    }
                
        except Exception as e:
            logger.error(f"Помилка аналізу структури таблиці: {e}")
            structure['error'] = str(e)
            
        return structure
    
    def _load_data_sample(self, gpkg_path: Path, table_name: str, sample_size: int) -> gpd.GeoDataFrame:
        """Завантаження вибірки даних для аналізу"""
        
        try:
            # Читаємо вибірку через GeoPandas
            logger.info(f"Завантаження вибірки {sample_size} записів...")
            
            # Використовуємо SQL для отримання вибірки
            sql_query = f"""
                SELECT * FROM "{table_name}" 
                WHERE geom IS NOT NULL 
                LIMIT {sample_size}
            """
            
            gdf = gpd.read_file(gpkg_path, sql=sql_query)
            logger.info(f"Завантажено {len(gdf)} записів для аналізу")
            
            return gdf
            
        except Exception as e:
            logger.error(f"Помилка завантаження вибірки: {e}")
            return gpd.GeoDataFrame()
    
    def _analyze_osm_content(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Аналіз OSM контенту"""
        
        content_analysis = {
            'osm_id_analysis': {},
            'osm_type_distribution': {},
            'version_analysis': {},
            'temporal_analysis': {},
            'user_analysis': {}
        }
        
        try:
            # Аналіз OSM ID
            if 'osm_id' in gdf.columns:
                osm_ids = gdf['osm_id'].dropna()
                content_analysis['osm_id_analysis'] = {
                    'total_unique_ids': int(osm_ids.nunique()),
                    'id_range': {
                        'min': int(osm_ids.min()),
                        'max': int(osm_ids.max())
                    },
                    'negative_ids_count': int((osm_ids < 0).sum()),  # Зазвичай relation members
                    'positive_ids_count': int((osm_ids > 0).sum())
                }
            
            # Аналіз типів OSM об'єктів
            if 'osm_type' in gdf.columns:
                osm_types = gdf['osm_type'].value_counts()
                content_analysis['osm_type_distribution'] = {
                    'types': osm_types.to_dict(),
                    'dominant_type': osm_types.index[0] if len(osm_types) > 0 else None
                }
            
            # Аналіз версій
            if 'version' in gdf.columns:
                versions = gdf['version'].dropna()
                content_analysis['version_analysis'] = {
                    'version_range': {
                        'min': int(versions.min()) if len(versions) > 0 else None,
                        'max': int(versions.max()) if len(versions) > 0 else None
                    },
                    'avg_version': float(versions.mean()) if len(versions) > 0 else None,
                    'single_version_objects': int((versions == 1).sum())
                }
            
            # Темпоральний аналіз
            if 'timestamp' in gdf.columns:
                timestamps = pd.to_datetime(gdf['timestamp'], errors='coerce').dropna()
                if len(timestamps) > 0:
                    content_analysis['temporal_analysis'] = {
                        'date_range': {
                            'earliest': timestamps.min().isoformat(),
                            'latest': timestamps.max().isoformat()
                        },
                        'data_freshness_days': (datetime.now() - timestamps.max()).days,
                        'temporal_span_days': (timestamps.max() - timestamps.min()).days
                    }
            
            # Аналіз користувачів
            if 'user' in gdf.columns:
                users = gdf['user'].dropna()
                user_counts = users.value_counts()
                content_analysis['user_analysis'] = {
                    'unique_contributors': int(users.nunique()),
                    'top_contributors': user_counts.head(10).to_dict(),
                    'single_edit_users': int((user_counts == 1).sum())
                }
            
        except Exception as e:
            content_analysis['error'] = str(e)
            
        return content_analysis
    
    def _analyze_spatial_data(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Просторовий аналіз даних"""
        
        spatial_analysis = {
            'coordinate_system': {},
            'spatial_extent': {},
            'density_analysis': {},
            'geometric_validity': {}
        }
        
        try:
            if 'geometry' not in gdf.columns or gdf.geometry.empty:
                return spatial_analysis
            
            # Координатна система
            spatial_analysis['coordinate_system'] = {
                'crs': str(gdf.crs),
                'epsg_code': gdf.crs.to_epsg() if gdf.crs else None,
                'is_geographic': gdf.crs.is_geographic if gdf.crs else None
            }
            
            # Просторове покриття
            bounds = gdf.total_bounds
            spatial_analysis['spatial_extent'] = {
                'bounds': {
                    'minx': float(bounds[0]),
                    'miny': float(bounds[1]),
                    'maxx': float(bounds[2]),
                    'maxy': float(bounds[3])
                },
                'center': {
                    'lat': float((bounds[1] + bounds[3]) / 2),
                    'lon': float((bounds[0] + bounds[2]) / 2)
                },
                'extent_degrees': {
                    'width': float(bounds[2] - bounds[0]),
                    'height': float(bounds[3] - bounds[1])
                }
            }
            
            # Приблизна площа покриття (для малих областей можна використати просту формулу)
            area_deg2 = (bounds[2] - bounds[0]) * (bounds[3] - bounds[1])
            # Приблизне переведення в км² для широт близько 50° (Україна)
            lat_center = (bounds[1] + bounds[3]) / 2
            km_per_deg_lat = 111.0
            km_per_deg_lon = 111.0 * np.cos(np.radians(lat_center))
            area_km2 = area_deg2 * km_per_deg_lat * km_per_deg_lon
            
            spatial_analysis['spatial_extent']['approximate_area_km2'] = float(area_km2)
            
            # Аналіз щільності
            feature_density = len(gdf) / area_km2 if area_km2 > 0 else 0
            spatial_analysis['density_analysis'] = {
                'features_per_km2': float(feature_density),
                'total_features_in_sample': len(gdf)
            }
            
            # Валідність геометрії
            valid_geoms = gdf.geometry.is_valid
            spatial_analysis['geometric_validity'] = {
                'valid_geometries': int(valid_geoms.sum()),
                'invalid_geometries': int((~valid_geoms).sum()),
                'validity_ratio': float(valid_geoms.mean()),
                'null_geometries': int(gdf.geometry.isna().sum())
            }
            
        except Exception as e:
            spatial_analysis['error'] = str(e)
            
        return spatial_analysis
    
    def _analyze_osm_tags(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Детальний аналіз OSM тегів з поля 'tags'"""
        
        tag_analysis = {
            'tags_structure': {},
            'key_retail_tags': {},
            'all_tag_keys': {},
            'tag_patterns': {},
            'retail_relevance': {}
        }
        
        try:
            if 'tags' not in gdf.columns:
                tag_analysis['error'] = "Колонка 'tags' не знайдена"
                return tag_analysis
            
            # Парсинг тегів
            parsed_tags = []
            valid_tags_count = 0
            
            for tags_str in gdf['tags'].dropna():
                try:
                    if tags_str and tags_str.strip():
                        # HOT експорти зазвичай зберігають теги як JSON або key=value
                        if tags_str.startswith('{'):
                            # JSON формат
                            tags_dict = json.loads(tags_str)
                        else:
                            # Інші формати - спробуємо різні варіанти
                            tags_dict = self._parse_tags_string(tags_str)
                        
                        if tags_dict:
                            parsed_tags.append(tags_dict)
                            valid_tags_count += 1
                except Exception:
                    continue
            
            tag_analysis['tags_structure'] = {
                'total_records_with_tags': valid_tags_count,
                'parsing_success_rate': float(valid_tags_count / len(gdf)) if len(gdf) > 0 else 0
            }
            
            if not parsed_tags:
                tag_analysis['error'] = "Не вдалося розпарсити жодного тегу"
                return tag_analysis
            
            # Збір всіх ключів тегів
            all_keys = Counter()
            key_value_pairs = defaultdict(Counter)
            
            for tags_dict in parsed_tags:
                for key, value in tags_dict.items():
                    all_keys[key] += 1
                    if value and str(value).strip():
                        key_value_pairs[key][str(value)] += 1
            
            # Топ ключі тегів
            tag_analysis['all_tag_keys'] = {
                'total_unique_keys': len(all_keys),
                'top_keys': dict(all_keys.most_common(30)),
                'keys_with_single_occurrence': sum(1 for count in all_keys.values() if count == 1)
            }
            
            # Ключові теги для ретейлу
            retail_tags = [
                'amenity', 'shop', 'building', 'landuse', 'highway', 'railway',
                'natural', 'leisure', 'tourism', 'office', 'name', 'brand',
                'addr:housenumber', 'addr:street', 'addr:city', 'addr:postcode',
                'opening_hours', 'phone', 'website', 'cuisine', 'level'
            ]
            
            for tag_key in retail_tags:
                if tag_key in all_keys:
                    values = key_value_pairs[tag_key]
                    tag_analysis['key_retail_tags'][tag_key] = {
                        'total_occurrences': all_keys[tag_key],
                        'occurrence_rate': float(all_keys[tag_key] / valid_tags_count),
                        'unique_values': len(values),
                        'top_values': dict(values.most_common(10))
                    }
            
            # Паттерни тегів
            tag_analysis['tag_patterns'] = self._analyze_tag_patterns(all_keys, key_value_pairs)
            
            # Релевантність для ретейлу
            retail_score = self._calculate_retail_relevance(tag_analysis['key_retail_tags'])
            tag_analysis['retail_relevance'] = retail_score
            
        except Exception as e:
            tag_analysis['error'] = str(e)
            
        return tag_analysis
    
    def _parse_tags_string(self, tags_str: str) -> Dict[str, str]:
        """Парсинг рядка тегів у словник"""
        
        tags_dict = {}
        
        try:
            # Спроба JSON
            if tags_str.startswith('{'):
                return json.loads(tags_str)
            
            # Спроба eval для Python dict literal
            if '=>' in tags_str:
                # Perl/Ruby стиль хешів
                tags_str = tags_str.replace('=>', ':')
                tags_str = re.sub(r'(\w+):', r'"\1":', tags_str)
                return json.loads(tags_str)
            
            # Key=value pairs розділені комами або новими рядками
            if '=' in tags_str:
                pairs = re.split(r'[,\n\r]+', tags_str)
                for pair in pairs:
                    if '=' in pair:
                        key, value = pair.split('=', 1)
                        tags_dict[key.strip().strip('"\'')] = value.strip().strip('"\'')
            
        except Exception:
            pass
            
        return tags_dict
    
    def _analyze_tag_patterns(self, all_keys: Counter, key_value_pairs: Dict) -> Dict[str, Any]:
        """Аналіз паттернів у тегах"""
        
        patterns = {
            'address_completeness': 0,
            'multilingual_names': 0,
            'commercial_indicators': 0,
            'accessibility_info': 0
        }
        
        try:
            # Адресна інформація
            address_keys = ['addr:housenumber', 'addr:street', 'addr:city', 'addr:postcode']
            address_present = sum(1 for key in address_keys if key in all_keys)
            patterns['address_completeness'] = address_present / len(address_keys)
            
            # Багатомовні назви
            name_keys = [key for key in all_keys if key.startswith('name:')]
            patterns['multilingual_names'] = len(name_keys)
            
            # Комерційні індикатори
            commercial_keys = ['shop', 'amenity', 'office', 'commercial', 'brand', 'operator']
            commercial_present = sum(1 for key in commercial_keys if key in all_keys)
            patterns['commercial_indicators'] = commercial_present
            
            # Інформація про доступність
            accessibility_keys = [key for key in all_keys if 'wheelchair' in key or 'access' in key]
            patterns['accessibility_info'] = len(accessibility_keys)
            
        except Exception:
            pass
            
        return patterns
    
    def _calculate_retail_relevance(self, key_retail_tags: Dict) -> Dict[str, Any]:
        """Розрахунок релевантності для ретейл аналізу"""
        
        relevance = {
            'overall_score': 0.0,
            'poi_richness': 0.0,
            'address_quality': 0.0,
            'commercial_density': 0.0,
            'suitability_assessment': 'unknown'
        }
        
        try:
            scores = []
            
            # POI багатство
            poi_tags = ['amenity', 'shop', 'office', 'leisure', 'tourism']
            poi_score = sum(key_retail_tags.get(tag, {}).get('occurrence_rate', 0) for tag in poi_tags)
            relevance['poi_richness'] = min(poi_score, 1.0)
            scores.append(relevance['poi_richness'])
            
            # Якість адрес
            address_tags = ['addr:housenumber', 'addr:street', 'addr:city']
            address_score = sum(key_retail_tags.get(tag, {}).get('occurrence_rate', 0) for tag in address_tags) / len(address_tags)
            relevance['address_quality'] = min(address_score, 1.0)
            scores.append(relevance['address_quality'])
            
            # Комерційна щільність
            commercial_tags = ['shop', 'amenity', 'building']
            commercial_score = sum(key_retail_tags.get(tag, {}).get('occurrence_rate', 0) for tag in commercial_tags) / len(commercial_tags)
            relevance['commercial_density'] = min(commercial_score, 1.0)
            scores.append(relevance['commercial_density'])
            
            # Загальна оцінка
            relevance['overall_score'] = np.mean(scores) if scores else 0.0
            
            # Оцінка придатності
            if relevance['overall_score'] > 0.7:
                relevance['suitability_assessment'] = 'excellent'
            elif relevance['overall_score'] > 0.5:
                relevance['suitability_assessment'] = 'good'
            elif relevance['overall_score'] > 0.3:
                relevance['suitability_assessment'] = 'moderate'
            else:
                relevance['suitability_assessment'] = 'poor'
            
        except Exception:
            relevance['suitability_assessment'] = 'error'
            
        return relevance
    
    def _analyze_geometry_distribution(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Аналіз розподілу геометрій"""
        
        geometry_analysis = {
            'geometry_types': {},
            'complexity_analysis': {},
            'spatial_clustering': {},
            'h3_compatibility': {}
        }
        
        try:
            if 'geometry' not in gdf.columns or gdf.geometry.empty:
                return geometry_analysis
            
            # Типи геометрій
            geom_types = gdf.geometry.geom_type.value_counts()
            geometry_analysis['geometry_types'] = {
                'distribution': geom_types.to_dict(),
                'primary_type': geom_types.index[0] if len(geom_types) > 0 else None,
                'type_diversity': len(geom_types)
            }
            
            # Аналіз складності
            complexity_stats = []
            for geom in gdf.geometry.dropna().head(1000):  # Обмежуємо для швидкості
                try:
                    if hasattr(geom, 'coords'):
                        complexity_stats.append(len(list(geom.coords)))
                    elif hasattr(geom, 'exterior'):
                        complexity_stats.append(len(list(geom.exterior.coords)))
                    elif hasattr(geom, 'geoms'):
                        total_coords = sum(len(list(g.coords)) if hasattr(g, 'coords') 
                                         else len(list(g.exterior.coords)) if hasattr(g, 'exterior') 
                                         else 0 for g in geom.geoms)
                        complexity_stats.append(total_coords)
                except:
                    continue
            
            if complexity_stats:
                geometry_analysis['complexity_analysis'] = {
                    'avg_vertices': float(np.mean(complexity_stats)),
                    'max_vertices': int(np.max(complexity_stats)),
                    'min_vertices': int(np.min(complexity_stats)),
                    'complexity_variance': float(np.var(complexity_stats))
                }
            
            # H3 сумісність
            primary_type = geometry_analysis['geometry_types']['primary_type']
            if primary_type:
                geometry_analysis['h3_compatibility'] = self._assess_h3_compatibility(primary_type, len(gdf))
            
        except Exception as e:
            geometry_analysis['error'] = str(e)
            
        return geometry_analysis
    
    def _assess_h3_compatibility(self, primary_geom_type: str, feature_count: int) -> Dict[str, Any]:
        """Оцінка сумісності з H3 індексацією"""
        
        compatibility = {
            'suitable': False,
            'recommended_resolutions': [],
            'indexing_strategy': None,
            'performance_expectation': 'unknown'
        }
        
        try:
            if primary_geom_type == 'Point':
                compatibility.update({
                    'suitable': True,
                    'recommended_resolutions': [8, 9, 10],
                    'indexing_strategy': 'direct_point_to_cell',
                    'performance_expectation': 'excellent'
                })
            elif primary_geom_type in ['Polygon', 'MultiPolygon']:
                compatibility.update({
                    'suitable': True,
                    'recommended_resolutions': [7, 8, 9],
                    'indexing_strategy': 'polygon_coverage',
                    'performance_expectation': 'good'
                })
            elif primary_geom_type in ['LineString', 'MultiLineString']:
                compatibility.update({
                    'suitable': True,
                    'recommended_resolutions': [8, 9],
                    'indexing_strategy': 'line_intersection',
                    'performance_expectation': 'moderate'
                })
            
            # Корекція залежно від кількості об'єктів
            if feature_count > 1000000 and compatibility['performance_expectation'] != 'unknown':
                if compatibility['performance_expectation'] == 'excellent':
                    compatibility['performance_expectation'] = 'good'
                elif compatibility['performance_expectation'] == 'good':
                    compatibility['performance_expectation'] = 'moderate'
            
        except Exception:
            pass
            
        return compatibility
    
    def _create_postgis_schema_recommendations(self, analysis: Dict, table_name: str) -> Dict[str, Any]:
        """Створення рекомендацій для PostGIS схеми"""
        
        schema_recommendations = {
            'main_table': {},
            'tag_extraction_tables': {},
            'indexes': [],
            'materialized_views': [],
            'h3_integration': {},
            'partitioning_strategy': {}
        }
        
        try:
            data_structure = analysis.get('data_structure', {})
            tag_analysis = analysis.get('tag_analysis', {})
            geometry_analysis = analysis.get('geometry_analysis', {})
            
            total_records = data_structure.get('total_records', 0)
            region_name = analysis.get('file_info', {}).get('region_name', 'unknown')
            
            # Основна таблиця
            schema_recommendations['main_table'] = {
                'table_name': f'osm_raw_{region_name.lower()}',
                'columns': [
                    'id SERIAL PRIMARY KEY',
                    'fid INTEGER UNIQUE',  # Оригінальний fid з GPKG
                    'osm_id BIGINT',
                    'osm_type VARCHAR(20)',
                    'version INTEGER',
                    'changeset INTEGER',
                    'uid INTEGER',
                    'username VARCHAR(255)',
                    'timestamp TIMESTAMP WITH TIME ZONE',
                    'geom GEOMETRY(GEOMETRY, 4326)',  # Змішані типи геометрії
                    'tags JSONB',  # Для зберігання всіх тегів
                    'region_name VARCHAR(50) DEFAULT \'{}\' '.format(region_name),
                    'created_at TIMESTAMP DEFAULT NOW()',
                    'updated_at TIMESTAMP DEFAULT NOW()'
                ],
                'estimated_size_gb': round(total_records * 0.5 / 1000000, 2)  # Приблизна оцінка
            }
            
            # H3 колонки
            h3_columns = []
            for res in [7, 8, 9, 10]:
                h3_columns.append(f'h3_res_{res} VARCHAR(15)')
            
            schema_recommendations['main_table']['columns'].extend(h3_columns)
            
            # Таблиці для витягнення тегів
            retail_tags = tag_analysis.get('key_retail_tags', {})
            if retail_tags:
                # Створюємо normalized таблицю для швидких запитів
                schema_recommendations['tag_extraction_tables']['osm_poi_normalized'] = {
                    'table_name': f'osm_poi_{region_name.lower()}',
                    'columns': [
                        'id SERIAL PRIMARY KEY',
                        'osm_raw_id INTEGER REFERENCES osm_raw_{} (id)'.format(region_name.lower()),
                        'osm_id BIGINT',
                        'geom GEOMETRY(GEOMETRY, 4326)',
                        'poi_type VARCHAR(50)',  # amenity, shop, etc.
                        'poi_value VARCHAR(255)', # restaurant, supermarket, etc.
                        'name VARCHAR(255)',
                        'brand VARCHAR(255)',
                        'addr_housenumber VARCHAR(20)',
                        'addr_street VARCHAR(255)',
                        'addr_city VARCHAR(255)',
                        'addr_postcode VARCHAR(20)',
                        'opening_hours TEXT',
                        'phone VARCHAR(50)',
                        'website VARCHAR(500)',
                        'cuisine VARCHAR(255)',
                        'level VARCHAR(20)',
                        'building VARCHAR(100)',
                        'landuse VARCHAR(100)',
                        'h3_res_8 VARCHAR(15)',
                        'h3_res_9 VARCHAR(15)',
                        'h3_res_10 VARCHAR(15)',
                        'created_at TIMESTAMP DEFAULT NOW()'
                    ],
                    'purpose': 'Normalized POI data for fast retail analysis'
                }
            
            # Індекси
            main_table_name = schema_recommendations['main_table']['table_name']
            
            # Основні індекси
            schema_recommendations['indexes'].extend([
                f'CREATE INDEX idx_{main_table_name}_geom ON {main_table_name} USING GIST (geom)',
                f'CREATE INDEX idx_{main_table_name}_osm_id ON {main_table_name} (osm_id)',
                f'CREATE INDEX idx_{main_table_name}_osm_type ON {main_table_name} (osm_type)',
                f'CREATE INDEX idx_{main_table_name}_tags_gin ON {main_table_name} USING GIN (tags)',
                f'CREATE INDEX idx_{main_table_name}_region ON {main_table_name} (region_name)'
            ])
            
            # H3 індекси
            for res in [7, 8, 9, 10]:
                schema_recommendations['indexes'].append(
                    f'CREATE INDEX idx_{main_table_name}_h3_res_{res} ON {main_table_name} (h3_res_{res})'
                )
            
            # JSONB індекси для ключових тегів
            for tag_key in ['amenity', 'shop', 'building', 'landuse', 'highway', 'name']:
                if tag_key in retail_tags:
                    schema_recommendations['indexes'].append(
                        f"CREATE INDEX idx_{main_table_name}_tags_{tag_key} ON {main_table_name} USING GIN ((tags->'{tag_key}'))"
                    )
            
            # Партиціонування для великих таблиць
            if total_records > 5000000:  # 5М+ записів
                schema_recommendations['partitioning_strategy'] = {
                    'recommended': True,
                    'strategy': 'hash_partitioning_by_h3',
                    'partition_count': min(16, max(4, total_records // 1000000)),
                    'partition_key': 'h3_res_8',
                    'benefits': [
                        'Faster queries on H3 cells',
                        'Parallel processing',
                        'Better index performance'
                    ]
                }
            
            # Материализованные представления
            if total_records > 100000:
                schema_recommendations['materialized_views'].extend([
                    {
                        'name': f'mv_poi_summary_{region_name.lower()}',
                        'sql': f"""
                        CREATE MATERIALIZED VIEW mv_poi_summary_{region_name.lower()} AS
                        SELECT 
                            h3_res_8,
                            COUNT(*) as total_features,
                            COUNT(*) FILTER (WHERE tags->>'amenity' IS NOT NULL) as amenity_count,
                            COUNT(*) FILTER (WHERE tags->>'shop' IS NOT NULL) as shop_count,
                            COUNT(*) FILTER (WHERE tags->>'building' IS NOT NULL) as building_count,
                            ST_Centroid(ST_Collect(geom)) as center_point
                        FROM {main_table_name}
                        WHERE h3_res_8 IS NOT NULL
                        GROUP BY h3_res_8
                        """,
                        'refresh_schedule': 'daily',
                        'purpose': 'Fast H3-based aggregations for dashboards'
                    },
                    {
                        'name': f'mv_retail_density_{region_name.lower()}',
                        'sql': f"""
                        CREATE MATERIALIZED VIEW mv_retail_density_{region_name.lower()} AS
                        SELECT 
                            h3_res_9,
                            COUNT(*) FILTER (WHERE tags->>'shop' IN ('supermarket', 'convenience', 'mall')) as retail_count,
                            COUNT(*) FILTER (WHERE tags->>'amenity' IN ('restaurant', 'cafe', 'fast_food')) as food_count,
                            COUNT(*) FILTER (WHERE tags->>'building' = 'commercial') as commercial_buildings,
                            ST_Centroid(ST_Collect(geom)) as center_point
                        FROM {main_table_name}
                        WHERE h3_res_9 IS NOT NULL
                        GROUP BY h3_res_9
                        """,
                        'refresh_schedule': 'weekly',
                        'purpose': 'Retail density analysis for location intelligence'
                    }
                ])
            
        except Exception as e:
            schema_recommendations['error'] = str(e)
            
        return schema_recommendations
    
    def _create_h3_integration_plan(self, analysis: Dict) -> Dict[str, Any]:
        """Створення плану H3 інтеграції"""
        
        h3_plan = {
            'processing_strategy': {},
            'resolution_usage': {},
            'performance_optimization': {},
            'sql_functions_needed': []
        }
        
        try:
            geometry_analysis = analysis.get('geometry_analysis', {})
            data_structure = analysis.get('data_structure', {})
            
            total_records = data_structure.get('total_records', 0)
            primary_geom_type = geometry_analysis.get('geometry_types', {}).get('primary_type')
            
            # Стратегія обробки
            if primary_geom_type == 'Point':
                h3_plan['processing_strategy'] = {
                    'method': 'direct_geocoding',
                    'sql_template': '''
                    UPDATE osm_raw_table SET 
                        h3_res_7 = h3_geo_to_h3(ST_Y(geom), ST_X(geom), 7),
                        h3_res_8 = h3_geo_to_h3(ST_Y(geom), ST_X(geom), 8),
                        h3_res_9 = h3_geo_to_h3(ST_Y(geom), ST_X(geom), 9),
                        h3_res_10 = h3_geo_to_h3(ST_Y(geom), ST_X(geom), 10)
                    WHERE geom IS NOT NULL;
                    ''',
                    'estimated_processing_time_hours': max(0.1, total_records / 1000000)
                }
            elif primary_geom_type in ['Polygon', 'MultiPolygon']:
                h3_plan['processing_strategy'] = {
                    'method': 'centroid_based',
                    'sql_template': '''
                    UPDATE osm_raw_table SET 
                        h3_res_7 = h3_geo_to_h3(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 7),
                        h3_res_8 = h3_geo_to_h3(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 8),
                        h3_res_9 = h3_geo_to_h3(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 9),
                        h3_res_10 = h3_geo_to_h3(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 10)
                    WHERE geom IS NOT NULL;
                    ''',
                    'estimated_processing_time_hours': max(0.2, total_records / 500000),
                    'alternative_method': 'polygon_coverage_for_large_areas'
                }
            else:
                h3_plan['processing_strategy'] = {
                    'method': 'mixed_geometry_handling',
                    'sql_template': '''
                    UPDATE osm_raw_table SET 
                        h3_res_7 = CASE 
                            WHEN ST_GeometryType(geom) = 'ST_Point' THEN h3_geo_to_h3(ST_Y(geom), ST_X(geom), 7)
                            ELSE h3_geo_to_h3(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 7)
                        END,
                        h3_res_8 = CASE 
                            WHEN ST_GeometryType(geom) = 'ST_Point' THEN h3_geo_to_h3(ST_Y(geom), ST_X(geom), 8)
                            ELSE h3_geo_to_h3(ST_Y(ST_Centroid(geom)), ST_X(ST_Centroid(geom)), 8)
                        END
                    WHERE geom IS NOT NULL;
                    ''',
                    'estimated_processing_time_hours': max(0.3, total_records / 300000)
                }
            
            # Використання резолюцій
            h3_plan['resolution_usage'] = {
                '7': {
                    'cell_edge_km': 5.16,
                    'area_km2': 23.24,
                    'use_case': 'Regional analysis, macro location planning'
                },
                '8': {
                    'cell_edge_km': 1.95,
                    'area_km2': 3.32,
                    'use_case': 'City district analysis, catchment areas'
                },
                '9': {
                    'cell_edge_km': 0.74,
                    'area_km2': 0.47,
                    'use_case': 'Neighborhood analysis, competition mapping'
                },
                '10': {
                    'cell_edge_km': 0.28,
                    'area_km2': 0.067,
                    'use_case': 'Micro-location analysis, site selection'
                }
            }
            
            # SQL функції
            h3_plan['sql_functions_needed'] = [
                'h3_geo_to_h3(lat, lon, resolution) - Convert coordinates to H3',
                'h3_h3_to_parent(h3_index, parent_resolution) - Get parent cell',
                'h3_h3_to_children(h3_index, child_resolution) - Get child cells',
                'h3_k_ring(h3_index, k) - Get neighboring cells',
                'h3_h3_to_geo(h3_index) - Convert H3 to coordinates',
                'h3_hex_area_km2(resolution) - Get cell area',
                'h3_edge_length_km(resolution) - Get edge length'
            ]
            
            # Оптимізація продуктивності
            if total_records > 1000000:
                h3_plan['performance_optimization'] = {
                    'batch_processing': True,
                    'batch_size': 50000,
                    'parallel_workers': min(8, max(2, total_records // 500000)),
                    'memory_limit_gb': max(4, total_records // 1000000),
                    'vacuum_between_batches': True
                }
            
        except Exception as e:
            h3_plan['error'] = str(e)
            
        return h3_plan
    
    def _estimate_performance(self, analysis: Dict) -> Dict[str, Any]:
        """Оцінка продуктивності системи"""
        
        performance = {
            'import_estimates': {},
            'query_performance': {},
            'storage_requirements': {},
            'scalability_assessment': {}
        }
        
        try:
            data_structure = analysis.get('data_structure', {})
            total_records = data_structure.get('total_records', 0)
            file_size_mb = analysis.get('file_info', {}).get('size_mb', 0)
            
            # Оцінки імпорту
            performance['import_estimates'] = {
                'gpkg_to_postgis_hours': max(0.1, file_size_mb / 1000),  # ~1GB/hour
                'h3_calculation_hours': max(0.1, total_records / 500000),  # ~500k records/hour
                'index_creation_hours': max(0.1, total_records / 1000000),  # ~1M records/hour
                'total_setup_time_hours': 0
            }
            
            performance['import_estimates']['total_setup_time_hours'] = sum([
                performance['import_estimates']['gpkg_to_postgis_hours'],
                performance['import_estimates']['h3_calculation_hours'],
                performance['import_estimates']['index_creation_hours']
            ])
            
            # Продуктивність запитів
            if total_records < 100000:
                query_performance = 'excellent'
                typical_query_ms = 50
            elif total_records < 1000000:
                query_performance = 'good'
                typical_query_ms = 200
            elif total_records < 5000000:
                query_performance = 'moderate'
                typical_query_ms = 1000
            else:
                query_performance = 'needs_optimization'
                typical_query_ms = 5000
            
            performance['query_performance'] = {
                'assessment': query_performance,
                'typical_h3_query_ms': typical_query_ms,
                'spatial_query_ms': typical_query_ms * 2,
                'full_text_search_ms': typical_query_ms * 1.5,
                'recommendations': []
            }
            
            if query_performance == 'needs_optimization':
                performance['query_performance']['recommendations'].extend([
                    'Enable partitioning',
                    'Use materialized views',
                    'Consider read replicas',
                    'Implement connection pooling'
                ])
            
            # Вимоги до зберігання
            raw_data_gb = file_size_mb / 1024
            indexes_gb = raw_data_gb * 0.3  # Індекси зазвичай 30% від даних
            h3_overhead_gb = total_records * 4 * 15 / (1024**3)  # 4 H3 cols * 15 chars
            materialized_views_gb = raw_data_gb * 0.1  # 10% для MV
            
            performance['storage_requirements'] = {
                'raw_data_gb': round(raw_data_gb, 2),
                'indexes_gb': round(indexes_gb, 2),
                'h3_overhead_gb': round(h3_overhead_gb, 2),
                'materialized_views_gb': round(materialized_views_gb, 2),
                'total_estimated_gb': round(raw_data_gb + indexes_gb + h3_overhead_gb + materialized_views_gb, 2),
                'recommended_disk_gb': round((raw_data_gb + indexes_gb + h3_overhead_gb + materialized_views_gb) * 2, 2)  # 2x для безпеки
            }
            
            # Оцінка масштабованості
            if total_records < 1000000:
                scalability = 'single_server_sufficient'
            elif total_records < 10000000:
                scalability = 'single_server_with_optimization'
            else:
                scalability = 'consider_distributed_setup'
            
            performance['scalability_assessment'] = {
                'current_data_scale': scalability,
                'all_ukraine_projection': {
                    'estimated_total_records': total_records * 24,  # 24 regions
                    'estimated_storage_gb': performance['storage_requirements']['total_estimated_gb'] * 24,
                    'recommended_architecture': 'distributed_with_sharding' if total_records > 2000000 else 'single_server_optimized'
                }
            }
            
        except Exception as e:
            performance['error'] = str(e)
            
        return performance
    
    def analyze_multiple_regions(self, target_files: List[str] = None, sample_size: int = 5000) -> Dict[str, Any]:
        """Аналіз декількох регіонів"""
        
        logger.info("🗺️ Запуск аналізу декількох регіонів")
        
        # Знаходимо файли
        gpkg_files = list(self.data_directory.glob("*.gpkg"))
        
        if not gpkg_files:
            logger.error(f"❌ Не знайдено .gpkg файлів в {self.data_directory}")
            return {}
        
        # Вибір файлів для аналізу
        if target_files:
            files_to_analyze = [self.data_directory / f for f in target_files if (self.data_directory / f).exists()]
        else:
            # Аналізуємо 3 файли різних розмірів для репрезентативності
            sorted_files = sorted(gpkg_files, key=lambda f: f.stat().st_size)
            files_to_analyze = [
                sorted_files[0],  # Найменший
                sorted_files[len(sorted_files)//2],  # Середній
                sorted_files[-1]  # Найбільший
            ]
        
        logger.info(f"📋 Обрано для аналізу: {[f.name for f in files_to_analyze]}")
        
        # Аналіз кожного файлу
        regional_analyses = {}
        
        for gpkg_file in files_to_analyze:
            logger.info(f"🔍 Аналіз {gpkg_file.name}...")
            regional_analysis = self.analyze_hot_osm_file(gpkg_file, sample_size)
            regional_analyses[gpkg_file.name] = regional_analysis
        
        # Зведений аналіз
        consolidated_analysis = self._create_consolidated_analysis(regional_analyses)
        
        complete_analysis = {
            'analysis_timestamp': datetime.now().isoformat(),
            'analyzed_regions': list(regional_analyses.keys()),
            'regional_details': regional_analyses,
            'consolidated_analysis': consolidated_analysis,
            'ukraine_wide_projections': self._project_ukraine_wide(consolidated_analysis),
            'implementation_roadmap': self._create_implementation_roadmap(consolidated_analysis)
        }
        
        return complete_analysis
    
    def _create_consolidated_analysis(self, regional_analyses: Dict) -> Dict[str, Any]:
        """Створення зведеного аналізу"""
        
        consolidated = {
            'data_volume_summary': {},
            'tag_richness_comparison': {},
            'spatial_coverage': {},
            'unified_schema_recommendations': {},
            'performance_projections': {}
        }
        
        try:
            total_records = 0
            total_size_mb = 0
            all_regions_tags = {}
            spatial_bounds = []
            
            for region_name, analysis in regional_analyses.items():
                # Обсяг даних
                data_structure = analysis.get('data_structure', {})
                file_info = analysis.get('file_info', {})
                
                region_records = data_structure.get('total_records', 0)
                region_size = file_info.get('size_mb', 0)
                
                total_records += region_records
                total_size_mb += region_size
                
                # Теги
                tag_analysis = analysis.get('tag_analysis', {})
                key_retail_tags = tag_analysis.get('key_retail_tags', {})
                all_regions_tags[region_name] = key_retail_tags
                
                # Просторове покриття
                spatial_analysis = analysis.get('spatial_analysis', {})
                extent = spatial_analysis.get('spatial_extent', {})
                if 'bounds' in extent:
                    spatial_bounds.append(extent['bounds'])
            
            # Зведена статистика
            consolidated['data_volume_summary'] = {
                'total_records_analyzed': total_records,
                'total_size_mb_analyzed': total_size_mb,
                'average_records_per_region': total_records // len(regional_analyses) if regional_analyses else 0,
                'projected_all_ukraine_records': total_records * (24 // len(regional_analyses)) if regional_analyses else 0,
                'projected_all_ukraine_size_gb': (total_size_mb * (24 // len(regional_analyses))) / 1024 if regional_analyses else 0
            }
            
            # Порівняння багатства тегів
            tag_consistency = self._analyze_tag_consistency(all_regions_tags)
            consolidated['tag_richness_comparison'] = tag_consistency
            
            # Просторове покриття
            if spatial_bounds:
                overall_bounds = {
                    'minx': min(b['minx'] for b in spatial_bounds),
                    'miny': min(b['miny'] for b in spatial_bounds),
                    'maxx': max(b['maxx'] for b in spatial_bounds),
                    'maxy': max(b['maxy'] for b in spatial_bounds)
                }
                
                consolidated['spatial_coverage'] = {
                    'analyzed_regions_bounds': overall_bounds,
                    'coverage_area_km2': self._calculate_approximate_area(overall_bounds),
                    'regions_analyzed': len(regional_analyses)
                }
            
            # Уніфікована схема
            consolidated['unified_schema_recommendations'] = self._create_unified_schema(regional_analyses)
            
        except Exception as e:
            consolidated['error'] = str(e)
            
        return consolidated
    
    def _analyze_tag_consistency(self, all_regions_tags: Dict) -> Dict[str, Any]:
        """Аналіз консистентності тегів між регіонами"""
        
        consistency = {
            'common_tags_across_regions': {},
            'region_specific_tags': {},
            'tag_coverage_variance': {}
        }
        
        try:
            # Збір всіх тегів
            all_possible_tags = set()
            for region_tags in all_regions_tags.values():
                all_possible_tags.update(region_tags.keys())
            
            # Аналіз поширеності тегів
            for tag in all_possible_tags:
                regions_with_tag = []
                coverage_rates = []
                
                for region_name, region_tags in all_regions_tags.items():
                    if tag in region_tags:
                        regions_with_tag.append(region_name)
                        coverage_rates.append(region_tags[tag].get('occurrence_rate', 0))
                
                consistency['common_tags_across_regions'][tag] = {
                    'present_in_regions': len(regions_with_tag),
                    'coverage_variance': float(np.var(coverage_rates)) if coverage_rates else 0,
                    'average_coverage': float(np.mean(coverage_rates)) if coverage_rates else 0
                }
            
            # Топ консистентні теги
            consistent_tags = sorted(
                consistency['common_tags_across_regions'].items(),
                key=lambda x: (x[1]['present_in_regions'], -x[1]['coverage_variance']),
                reverse=True
            )
            
            consistency['most_consistent_tags'] = dict(consistent_tags[:10])
            
        except Exception:
            pass
            
        return consistency
    
    def _calculate_approximate_area(self, bounds: Dict) -> float:
        """Приблизний розрахунок площі в км²"""
        try:
            width_deg = bounds['maxx'] - bounds['minx']
            height_deg = bounds['maxy'] - bounds['miny']
            
            # Для України (приблизно 50° північної широти)
            lat_center = (bounds['miny'] + bounds['maxy']) / 2
            km_per_deg_lat = 111.0
            km_per_deg_lon = 111.0 * np.cos(np.radians(lat_center))
            
            area_km2 = width_deg * km_per_deg_lon * height_deg * km_per_deg_lat
            return round(area_km2, 2)
        except:
            return 0.0
    
    def _create_unified_schema(self, regional_analyses: Dict) -> Dict[str, Any]:
        """Створення уніфікованої схеми для всіх регіонів"""
        
        unified_schema = {
            'base_table_structure': {},
            'region_specific_adaptations': {},
            'global_indexes': [],
            'federation_strategy': {}
        }
        
        try:
            # Базова структура (найбільш консистентна)
            unified_schema['base_table_structure'] = {
                'table_name': 'osm_ukraine_unified',
                'columns': [
                    'id BIGSERIAL PRIMARY KEY',
                    'region_name VARCHAR(50) NOT NULL',
                    'original_fid INTEGER',
                    'osm_id BIGINT',
                    'osm_type VARCHAR(20)',
                    'version INTEGER',
                    'changeset INTEGER',
                    'uid INTEGER',
                    'username VARCHAR(255)',
                    'timestamp TIMESTAMP WITH TIME ZONE',
                    'geom GEOMETRY(GEOMETRY, 4326)',
                    'tags JSONB',
                    'h3_res_7 VARCHAR(15)',
                    'h3_res_8 VARCHAR(15)',
                    'h3_res_9 VARCHAR(15)',
                    'h3_res_10 VARCHAR(15)',
                    'created_at TIMESTAMP DEFAULT NOW()',
                    'updated_at TIMESTAMP DEFAULT NOW()'
                ],
                'partitioning': {
                    'method': 'PARTITION BY LIST (region_name)',
                    'benefits': ['Parallel queries', 'Regional data isolation', 'Easier maintenance']
                }
            }
            
            # Глобальні індекси
            unified_schema['global_indexes'] = [
                'CREATE INDEX idx_osm_ukraine_geom ON osm_ukraine_unified USING GIST (geom)',
                'CREATE INDEX idx_osm_ukraine_h3_8 ON osm_ukraine_unified (h3_res_8)',
                'CREATE INDEX idx_osm_ukraine_h3_9 ON osm_ukraine_unified (h3_res_9)',
                'CREATE INDEX idx_osm_ukraine_tags_gin ON osm_ukraine_unified USING GIN (tags)',
                'CREATE INDEX idx_osm_ukraine_region ON osm_ukraine_unified (region_name)',
                'CREATE INDEX idx_osm_ukraine_osm_id ON osm_ukraine_unified (osm_id)',
                'CREATE INDEX idx_osm_ukraine_poi ON osm_ukraine_unified USING GIN ((tags->\'amenity\'), (tags->\'shop\'))'
            ]
            
            # Стратегія федерації
            unified_schema['federation_strategy'] = {
                'approach': 'single_database_partitioned',
                'alternative': 'federated_databases_per_region',
                'recommended': 'single_database_partitioned',
                'reasoning': 'Easier cross-region analytics, unified H3 grid, simpler maintenance'
            }
            
        except Exception as e:
            unified_schema['error'] = str(e)
            
        return unified_schema
    
    def _project_ukraine_wide(self, consolidated_analysis: Dict) -> Dict[str, Any]:
        """Проекція на всю Україну"""
        
        ukraine_projection = {
            'data_volume_projections': {},
            'infrastructure_requirements': {},
            'implementation_phases': {},
            'cost_estimates': {}
        }
        
        try:
            data_summary = consolidated_analysis.get('data_volume_summary', {})
            
            # Проекція обсягу даних
            ukraine_projection['data_volume_projections'] = {
                'total_records_estimate': data_summary.get('projected_all_ukraine_records', 0),
                'total_storage_gb_estimate': data_summary.get('projected_all_ukraine_size_gb', 0),
                'daily_update_volume_mb': data_summary.get('projected_all_ukraine_size_gb', 0) * 1024 * 0.01,  # 1% daily changes
                'h3_index_storage_gb': data_summary.get('projected_all_ukraine_records', 0) * 4 * 15 / (1024**3),
                'materialized_views_gb': data_summary.get('projected_all_ukraine_size_gb', 0) * 0.15
            }
            
            total_storage_needed = (
                ukraine_projection['data_volume_projections']['total_storage_gb_estimate'] * 2.5  # Raw data + indexes + overhead
            )
            
            # Вимоги до інфраструктури
            ukraine_projection['infrastructure_requirements'] = {
                'database_server': {
                    'cpu_cores': max(16, total_storage_needed // 100),
                    'ram_gb': max(64, total_storage_needed // 10),
                    'storage_gb': total_storage_needed * 1.5,  # З запасом
                    'storage_type': 'NVMe SSD для optimal performance',
                    'network_gbps': 10
                },
                'application_servers': {
                    'count': max(2, total_storage_needed // 500),
                    'cpu_cores_each': 8,
                    'ram_gb_each': 32
                },
                'backup_requirements': {
                    'storage_gb': total_storage_needed * 1.2,
                    'backup_frequency': 'daily_incremental',
                    'retention_days': 30
                }
            }
            
            # Фази впровадження
            ukraine_projection['implementation_phases'] = {
                'phase_1_pilot': {
                    'regions': ['Kyiv', 'Lviv', 'Kharkiv'],
                    'duration_weeks': 4,
                    'goals': ['Schema validation', 'Performance testing', 'H3 integration testing']
                },
                'phase_2_expansion': {
                    'regions': ['All major cities'],
                    'duration_weeks': 8,
                    'goals': ['Scale testing', 'ETL pipeline optimization', 'API development']
                },
                'phase_3_complete': {
                    'regions': ['All 24 regions'],
                    'duration_weeks': 6,
                    'goals': ['Full deployment', 'Monitoring setup', 'Documentation']
                }
            }
            
            # Приблизні оцінки витрат
            ukraine_projection['cost_estimates'] = {
                'infrastructure_monthly_usd': {
                    'database_server': max(500, total_storage_needed * 2),
                    'application_servers': ukraine_projection['infrastructure_requirements']['application_servers']['count'] * 200,
                    'storage_backup': total_storage_needed * 0.5,
                    'network_bandwidth': 200,
                    'monitoring_tools': 100
                },
                'development_costs_usd': {
                    'etl_pipeline_development': 15000,
                    'api_development': 25000,
                    'frontend_dashboard': 20000,
                    'testing_qa': 10000,
                    'documentation': 5000
                },
                'operational_monthly_usd': {
                    'devops_engineer_partial': 2000,
                    'data_engineer_partial': 1500,
                    'monitoring_maintenance': 500
                }
            }
            
        except Exception as e:
            ukraine_projection['error'] = str(e)
            
        return ukraine_projection
    
    def _create_implementation_roadmap(self, consolidated_analysis: Dict) -> Dict[str, Any]:
        """Створення дорожньої карти впровадження"""
        
        roadmap = {
            'immediate_next_steps': [],
            'technical_milestones': {},
            'risk_mitigation': {},
            'success_metrics': {}
        }
        
        try:
            # Негайні наступні кроки
            roadmap['immediate_next_steps'] = [
                {
                    'step': 'Setup PostGIS environment',
                    'duration_days': 2,
                    'requirements': ['PostgreSQL 15+', 'PostGIS 3.4+', 'H3 extension'],
                    'deliverable': 'Working PostGIS instance with H3 support'
                },
                {
                    'step': 'Implement pilot ETL pipeline',
                    'duration_days': 5,
                    'requirements': ['Python environment', 'GeoPandas', 'SQLAlchemy'],
                    'deliverable': 'Working ETL for 1-2 regions'
                },
                {
                    'step': 'Create unified schema',
                    'duration_days': 3,
                    'requirements': ['Schema analysis results', 'H3 integration plan'],
                    'deliverable': 'Production-ready database schema'
                },
                {
                    'step': 'Develop H3 processing functions',
                    'duration_days': 4,
                    'requirements': ['H3 PostGIS extension', 'Batch processing logic'],
                    'deliverable': 'Automated H3 indexing pipeline'
                },
                {
                    'step': 'Performance testing',
                    'duration_days': 3,
                    'requirements': ['Sample data loaded', 'Query benchmark suite'],
                    'deliverable': 'Performance baseline and optimization recommendations'
                }
            ]
            
            # Технічні віхи
            roadmap['technical_milestones'] = {
                'week_2': {
                    'milestone': 'Single region successfully imported',
                    'criteria': ['All data imported', 'H3 indexing complete', 'Basic queries working']
                },
                'week_4': {
                    'milestone': 'Multi-region federation working',
                    'criteria': ['3+ regions loaded', 'Cross-region queries', 'Performance acceptable']
                },
                'week_8': {
                    'milestone': 'API and analytics ready',
                    'criteria': ['REST API functional', 'Basic analytics working', 'Dashboard prototype']
                },
                'week_12': {
                    'milestone': 'Production ready system',
                    'criteria': ['All regions loaded', 'Monitoring in place', 'Documentation complete']
                }
            }
            
            # Ризики та мітигація
            roadmap['risk_mitigation'] = {
                'data_quality_issues': {
                    'risk': 'Inconsistent OSM data across regions',
                    'probability': 'medium',
                    'impact': 'medium',
                    'mitigation': 'Robust error handling, data validation pipelines, fallback strategies'
                },
                'performance_bottlenecks': {
                    'risk': 'Slow queries on large datasets',
                    'probability': 'high',
                    'impact': 'high',
                    'mitigation': 'Partitioning, materialized views, query optimization, caching'
                },
                'h3_integration_complexity': {
                    'risk': 'H3 extension issues or performance problems',
                    'probability': 'low',
                    'impact': 'high',
                    'mitigation': 'Fallback to custom H3 implementation, pre-testing, alternative indexing'
                },
                'storage_costs': {
                    'risk': 'Higher than expected storage requirements',
                    'probability': 'medium',
                    'impact': 'medium',
                    'mitigation': 'Data compression, archiving strategies, cloud auto-scaling'
                }
            }
            
            # Метрики успіху
            roadmap['success_metrics'] = {
                'data_metrics': {
                    'data_coverage': '>95% of Ukraine territory',
                    'data_freshness': '<30 days old',
                    'data_quality': '>90% valid geometries'
                },
                'performance_metrics': {
                    'query_response_time': '<500ms for H3 queries',
                    'api_availability': '>99.5%',
                    'concurrent_users': '>50 simultaneous'
                },
                'business_metrics': {
                    'location_analysis_time': '<1 hour vs 1 day manual',
                    'analysis_accuracy': '>85% vs manual methods',
                    'user_satisfaction': '>4.5/5'
                }
            }
            
        except Exception as e:
            roadmap['error'] = str(e)
            
        return roadmap
    
    def save_analysis_report(self, analysis_results: Dict, output_path: str = None):
        """Збереження звіту аналізу"""
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"hot_osm_corrected_analysis_{timestamp}.json"
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(analysis_results, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"✅ Звіт збережено: {output_path}")
            
        except Exception as e:
            logger.error(f"❌ Помилка збереження звіту: {e}")
    
    def print_analysis_summary(self, analysis_results: Dict):
        """Виведення короткого звіту аналізу"""
        
        print("\n" + "="*100)
        print("🎯 CORRECTED HOT OSM ANALYSIS - SUMMARY")
        print("="*100)
        
        if 'regional_details' in analysis_results:
            # Multi-region analysis
            self._print_multi_region_summary(analysis_results)
        else:
            # Single region analysis
            self._print_single_region_summary(analysis_results)
        
        print("="*100 + "\n")
    
    def _print_single_region_summary(self, analysis: Dict):
        """Виведення підсумку для одного регіону"""
        
        file_info = analysis.get('file_info', {})
        data_structure = analysis.get('data_structure', {})
        osm_content = analysis.get('osm_content_analysis', {})
        tag_analysis = analysis.get('tag_analysis', {})
        spatial_analysis = analysis.get('spatial_analysis', {})
        
        print(f"📁 Регіон: {file_info.get('region_name', 'Unknown')}")
        print(f"📊 Розмір файлу: {file_info.get('size_mb', 0):.1f} MB")
        print(f"🔢 Всього записів: {data_structure.get('total_records', 0):,}")
        
        # OSM контент
        if osm_content:
            osm_type_dist = osm_content.get('osm_type_distribution', {})
            types = osm_type_dist.get('types', {})
            print(f"\n📋 OSM ТИПИ:")
            for osm_type, count in types.items():
                print(f"  • {osm_type}: {count:,}")
        
        # Теги
        if tag_analysis:
            retail_tags = tag_analysis.get('key_retail_tags', {})
            retail_relevance = tag_analysis.get('retail_relevance', {})
            
            print(f"\n🏷️ КЛЮЧОВІ ТЕГИ ДЛЯ РЕТЕЙЛУ:")
            for tag, info in list(retail_tags.items())[:5]:
                occurrence_rate = info.get('occurrence_rate', 0)
                print(f"  • {tag}: {occurrence_rate:.1%} покриття")
            
            relevance_score = retail_relevance.get('overall_score', 0)
            suitability = retail_relevance.get('suitability_assessment', 'unknown')
            print(f"\n🎯 ПРИДАТНІСТЬ ДЛЯ РЕТЕЙЛУ: {suitability.upper()} (Score: {relevance_score:.2f})")
        
        # Просторові дані
        if spatial_analysis:
            extent = spatial_analysis.get('spatial_extent', {})
            density = spatial_analysis.get('density_analysis', {})
            
            area_km2 = extent.get('approximate_area_km2', 0)
            feature_density = density.get('features_per_km2', 0)
            
            print(f"\n🗺️ ПРОСТОРОВЕ ПОКРИТТЯ:")
            print(f"  • Площа: ~{area_km2:,.0f} км²")
            print(f"  • Щільність об'єктів: {feature_density:.1f} об'єктів/км²")
        
        # Рекомендації
        schema_recs = analysis.get('postgis_schema_recommendations', {})
        if schema_recs:
            main_table = schema_recs.get('main_table', {})
            estimated_size = main_table.get('estimated_size_gb', 0)
            print(f"\n💾 ОЦІНКА РОЗМІРУ В POSTGIS: ~{estimated_size:.1f} GB")
    
    def _print_multi_region_summary(self, analysis_results: Dict):
        """Виведення підсумку для декількох регіонів"""
        
        consolidated = analysis_results.get('consolidated_analysis', {})
        ukraine_projection = analysis_results.get('ukraine_wide_projections', {})
        
        # Загальна статистика
        data_summary = consolidated.get('data_volume_summary', {})
        print(f"📊 ПРОАНАЛІЗОВАНО РЕГІОНІВ: {len(analysis_results.get('analyzed_regions', []))}")
        print(f"🔢 ЗАГАЛОМ ЗАПИСІВ: {data_summary.get('total_records_analyzed', 0):,}")
        print(f"💾 ЗАГАЛЬНИЙ РОЗМІР: {data_summary.get('total_size_mb_analyzed', 0):,.1f} MB")
        
        # Проекція на Україну
        if ukraine_projection:
            volume_proj = ukraine_projection.get('data_volume_projections', {})
            infra_req = ukraine_projection.get('infrastructure_requirements', {})
            
            print(f"\n🇺🇦 ПРОЕКЦІЯ НА ВСЮ УКРАЇНУ:")
            print(f"  • Очікувана кількість записів: {volume_proj.get('total_records_estimate', 0):,}")
            print(f"  • Очікуваний розмір: {volume_proj.get('total_storage_gb_estimate', 0):,.0f} GB")
            
            db_server = infra_req.get('database_server', {})
            print(f"\n🖥️ РЕКОМЕНДОВАНА ІНФРАСТРУКТУРА:")
            print(f"  • CPU cores: {db_server.get('cpu_cores', 0)}")
            print(f"  • RAM: {db_server.get('ram_gb', 0)} GB")
            print(f"  • Storage: {db_server.get('storage_gb', 0):,.0f} GB")
        
        # Консистентність тегів
        tag_richness = consolidated.get('tag_richness_comparison', {})
        if tag_richness:
            consistent_tags = tag_richness.get('most_consistent_tags', {})
            print(f"\n🏷️ НАЙБІЛЬШ КОНСИСТЕНТНІ ТЕГИ:")
            for tag, info in list(consistent_tags.items())[:5]:
                regions_count = info.get('present_in_regions', 0)
                avg_coverage = info.get('average_coverage', 0)
                print(f"  • {tag}: {regions_count} регіонів, {avg_coverage:.1%} покриття")
        
        # Наступні кроки
        roadmap = analysis_results.get('implementation_roadmap', {})
        if roadmap:
            next_steps = roadmap.get('immediate_next_steps', [])
            print(f"\n🎯 НАСТУПНІ КРОКИ:")
            for i, step in enumerate(next_steps[:3], 1):
                step_name = step.get('step', 'Unknown')
                duration = step.get('duration_days', 0)
                print(f"  {i}. {step_name} ({duration} днів)")


def main():
    """Запуск виправленого аналізу HOT OSM експортів"""
    
    data_directory = r"C:\OSMData"
    
    print("🚀 Запуск виправленого HOT OSM аналізу...")
    print(f"📁 Директорія: {data_directory}")
    
    analyzer = CorrectedHOTOSMAnalyzer(data_directory)
    
    try:
        # Вибір типу аналізу
        analysis_type = input("\nОберіть тип аналізу:\n1. Один файл (швидко)\n2. Декілька файлів (повний)\nВаш вибір (1/2): ").strip()
        
        if analysis_type == "1":
            # Аналіз одного файлу
            gpkg_files = list(Path(data_directory).glob("*.gpkg"))
            if not gpkg_files:
                print("❌ Не знайдено .gpkg файлів")
                return
            
            # Вибираємо найменший для швидкості
            selected_file = min(gpkg_files, key=lambda f: f.stat().st_size)
            print(f"📋 Обрано: {selected_file.name}")
            
            analysis_results = analyzer.analyze_hot_osm_file(selected_file)
            
        else:
            # Аналіз декількох файлів
            analysis_results = analyzer.analyze_multiple_regions()
        
        if analysis_results:
            # Збереження звіту
            analyzer.save_analysis_report(analysis_results)
            
            # Виведення підсумку
            analyzer.print_analysis_summary(analysis_results)
            
            print(f"\n✅ Аналіз завершено успішно!")
        else:
            print(f"❌ Аналіз не дав результатів")
            
    except Exception as e:
        print(f"❌ Помилка аналізу: {e}")
        logger.exception("Детальна інформація про помилку:")


if __name__ == "__main__":
    main()