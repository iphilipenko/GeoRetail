#!/usr/bin/env python3
"""
OSM HOT Export Diagnostic Analyzer
Спеціалізований аналізатор для діагностики структури HOT OSM експортів (.gpkg)
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HOTOSMDiagnosticAnalyzer:
    """Діагностичний аналізатор структури HOT OSM експортів"""
    
    def __init__(self, data_directory: str):
        self.data_directory = Path(data_directory)
        self.analysis_results = {}
        
    def diagnose_single_file(self, gpkg_path: Path, max_sample_size: int = 10) -> Dict[str, Any]:
        """Повна діагностика одного .gpkg файлу"""
        
        logger.info(f"🔍 Діагностика файлу: {gpkg_path.name}")
        
        file_analysis = {
            'file_info': {
                'filename': gpkg_path.name,
                'size_mb': round(gpkg_path.stat().st_size / (1024 * 1024), 2),
                'full_path': str(gpkg_path)
            },
            'sqlite_structure': {},
            'layers_detailed': {},
            'osm_tag_analysis': {},
            'geometry_analysis': {},
            'recommendations': {}
        }
        
        try:
            # 1. Аналіз SQLite структури
            file_analysis['sqlite_structure'] = self._analyze_sqlite_structure(gpkg_path)
            
            # 2. Аналіз шарів
            layers_info = gpd.list_layers(gpkg_path)
            logger.info(f"Знайдено шарів: {len(layers_info)}")
            
            for layer_info in layers_info:
                layer_name = layer_info[0] if isinstance(layer_info, tuple) else layer_info
                logger.info(f"  📊 Аналіз шару: {layer_name}")
                
                layer_analysis = self._diagnose_layer_comprehensive(
                    gpkg_path, layer_name, max_sample_size
                )
                file_analysis['layers_detailed'][layer_name] = layer_analysis
            
            # 3. Глобальний аналіз OSM тегів
            file_analysis['osm_tag_analysis'] = self._analyze_osm_tags_globally(
                file_analysis['layers_detailed']
            )
            
            # 4. Аналіз геометрій
            file_analysis['geometry_analysis'] = self._analyze_geometries_globally(
                file_analysis['layers_detailed']
            )
            
            # 5. Рекомендації для PostGIS схеми
            file_analysis['recommendations'] = self._generate_schema_recommendations(
                file_analysis
            )
            
        except Exception as e:
            logger.error(f"❌ Помилка діагностики: {e}")
            file_analysis['error'] = str(e)
            
        return file_analysis
    
    def _analyze_sqlite_structure(self, gpkg_path: Path) -> Dict[str, Any]:
        """Аналіз внутрішньої структури SQLite/GPKG"""
        
        structure = {
            'tables': [],
            'indexes': [],
            'triggers': [],
            'views': [],
            'metadata': {}
        }
        
        try:
            with sqlite3.connect(gpkg_path) as conn:
                cursor = conn.cursor()
                
                # Отримання списку таблиць
                cursor.execute("""
                    SELECT name, type, sql 
                    FROM sqlite_master 
                    WHERE type IN ('table', 'index', 'trigger', 'view')
                    ORDER BY type, name
                """)
                
                for name, obj_type, sql in cursor.fetchall():
                    if obj_type == 'table':
                        # Аналіз структури таблиці
                        cursor.execute(f"PRAGMA table_info('{name}')")
                        columns = cursor.fetchall()
                        
                        cursor.execute(f"SELECT COUNT(*) FROM '{name}'")
                        row_count = cursor.fetchone()[0]
                        
                        structure['tables'].append({
                            'name': name,
                            'row_count': row_count,
                            'columns': [
                                {
                                    'name': col[1], 
                                    'type': col[2], 
                                    'nullable': not col[3],
                                    'primary_key': bool(col[5])
                                } 
                                for col in columns
                            ],
                            'create_sql': sql
                        })
                    elif obj_type == 'index':
                        structure['indexes'].append({'name': name, 'sql': sql})
                    elif obj_type == 'trigger':
                        structure['triggers'].append({'name': name, 'sql': sql})
                    elif obj_type == 'view':
                        structure['views'].append({'name': name, 'sql': sql})
                
                # GPKG метадані
                cursor.execute("""
                    SELECT table_name, data_type, identifier, description, srs_id
                    FROM gpkg_contents
                """)
                
                gpkg_contents = cursor.fetchall()
                structure['metadata']['gpkg_contents'] = [
                    {
                        'table_name': row[0],
                        'data_type': row[1], 
                        'identifier': row[2],
                        'description': row[3],
                        'srs_id': row[4]
                    }
                    for row in gpkg_contents
                ]
                
        except Exception as e:
            logger.warning(f"Помилка аналізу SQLite структури: {e}")
            structure['error'] = str(e)
            
        return structure
    
    def _diagnose_layer_comprehensive(self, gpkg_path: Path, layer_name: str, 
                                    max_sample_size: int) -> Dict[str, Any]:
        """Повний аналіз окремого шару"""
        
        layer_diagnosis = {
            'layer_name': layer_name,
            'basic_info': {},
            'geometry_details': {},
            'attributes_analysis': {},
            'osm_tags_analysis': {},
            'sample_data': {},
            'data_quality': {},
            'h3_suitability': {}
        }
        
        try:
            # Читання шару з обмеженням пам'яті
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
            
            if gdf.empty:
                layer_diagnosis['basic_info']['empty'] = True
                return layer_diagnosis
            
            # Базова інформація
            layer_diagnosis['basic_info'] = {
                'total_features': len(gdf),
                'columns_count': len(gdf.columns),
                'memory_usage_mb': round(gdf.memory_usage(deep=True).sum() / (1024*1024), 2),
                'crs': str(gdf.crs) if gdf.crs else None,
                'bounds': list(gdf.total_bounds) if not gdf.empty else None
            }
            
            # Детальний аналіз геометрії
            layer_diagnosis['geometry_details'] = self._analyze_layer_geometry(gdf)
            
            # Аналіз атрибутів
            layer_diagnosis['attributes_analysis'] = self._analyze_layer_attributes(gdf)
            
            # Специфічний аналіз OSM тегів
            layer_diagnosis['osm_tags_analysis'] = self._analyze_osm_tags_in_layer(gdf)
            
            # Вибірка прикладів
            layer_diagnosis['sample_data'] = self._extract_representative_samples(
                gdf, max_sample_size
            )
            
            # Оцінка якості даних
            layer_diagnosis['data_quality'] = self._assess_layer_quality(gdf)
            
            # Оцінка придатності для H3
            layer_diagnosis['h3_suitability'] = self._assess_h3_suitability(gdf)
            
        except Exception as e:
            logger.error(f"Помилка аналізу шару {layer_name}: {e}")
            layer_diagnosis['error'] = str(e)
            
        return layer_diagnosis
    
    def _analyze_layer_geometry(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Детальний аналіз геометрії шару"""
        
        geom_analysis = {
            'geometry_types': {},
            'validity': {},
            'complexity': {},
            'spatial_distribution': {}
        }
        
        try:
            if 'geometry' not in gdf.columns or gdf.geometry.empty:
                return geom_analysis
            
            # Типи геометрій
            geom_types = gdf.geometry.geom_type.value_counts()
            geom_analysis['geometry_types'] = {
                'types_distribution': geom_types.to_dict(),
                'primary_type': geom_types.index[0] if len(geom_types) > 0 else None,
                'is_mixed': len(geom_types) > 1
            }
            
            # Валідність геометрій
            valid_geoms = gdf.geometry.is_valid
            geom_analysis['validity'] = {
                'valid_count': int(valid_geoms.sum()),
                'invalid_count': int((~valid_geoms).sum()),
                'validity_ratio': float(valid_geoms.mean())
            }
            
            # Складність геометрій (для полігонів та ліній)
            if geom_types.index[0] in ['Polygon', 'MultiPolygon', 'LineString', 'MultiLineString']:
                complexities = []
                for geom in gdf.geometry.dropna()[:1000]:  # Обмежуємо для продуктивності
                    if hasattr(geom, 'coords'):
                        complexities.append(len(list(geom.coords)))
                    elif hasattr(geom, 'exterior'):
                        complexities.append(len(list(geom.exterior.coords)))
                
                if complexities:
                    geom_analysis['complexity'] = {
                        'avg_vertices': float(np.mean(complexities)),
                        'max_vertices': int(np.max(complexities)),
                        'min_vertices': int(np.min(complexities))
                    }
            
            # Просторовий розподіл
            bounds = gdf.total_bounds
            geom_analysis['spatial_distribution'] = {
                'bbox': {
                    'minx': float(bounds[0]), 'miny': float(bounds[1]),
                    'maxx': float(bounds[2]), 'maxy': float(bounds[3])
                },
                'bbox_area_deg2': float((bounds[2] - bounds[0]) * (bounds[3] - bounds[1])),
                'centroid': {
                    'lat': float((bounds[1] + bounds[3]) / 2),
                    'lon': float((bounds[0] + bounds[2]) / 2)
                }
            }
            
        except Exception as e:
            geom_analysis['error'] = str(e)
            
        return geom_analysis
    
    def _analyze_layer_attributes(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Детальний аналіз атрибутів шару"""
        
        attr_analysis = {
            'columns_summary': {},
            'data_types': {},
            'completeness': {},
            'uniqueness': {},
            'patterns': {}
        }
        
        try:
            # Аналіз кожної колонки
            for col in gdf.columns:
                if col == 'geometry':
                    continue
                    
                col_info = {
                    'dtype': str(gdf[col].dtype),
                    'null_count': int(gdf[col].isna().sum()),
                    'null_ratio': float(gdf[col].isna().mean()),
                    'unique_count': int(gdf[col].nunique()),
                    'uniqueness_ratio': float(gdf[col].nunique() / len(gdf))
                }
                
                # Додаткова статистика залежно від типу
                if gdf[col].dtype in ['object', 'string']:
                    # Текстові поля
                    non_null = gdf[col].dropna()
                    if len(non_null) > 0:
                        col_info.update({
                            'avg_length': float(non_null.astype(str).str.len().mean()),
                            'max_length': int(non_null.astype(str).str.len().max()),
                            'common_values': non_null.value_counts().head(5).to_dict()
                        })
                        
                        # Детекція OSM тегів
                        if any(osm_pattern in col.lower() for osm_pattern in 
                               ['osm', 'tag', 'key', 'value', 'amenity', 'shop', 'name']):
                            col_info['likely_osm_tag'] = True
                
                elif gdf[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                    # Числові поля
                    non_null = gdf[col].dropna()
                    if len(non_null) > 0:
                        col_info.update({
                            'min_value': float(non_null.min()),
                            'max_value': float(non_null.max()),
                            'mean_value': float(non_null.mean()),
                            'median_value': float(non_null.median())
                        })
                
                attr_analysis['columns_summary'][col] = col_info
            
            # Загальна статистика
            non_geom_cols = [col for col in gdf.columns if col != 'geometry']
            attr_analysis['data_types'] = dict(gdf[non_geom_cols].dtypes.astype(str))
            
            attr_analysis['completeness'] = {
                'fully_complete_columns': len([col for col in non_geom_cols 
                                             if gdf[col].notna().all()]),
                'mostly_empty_columns': len([col for col in non_geom_cols 
                                           if gdf[col].isna().mean() > 0.9]),
                'average_completeness': float(gdf[non_geom_cols].notna().mean().mean())
            }
            
        except Exception as e:
            attr_analysis['error'] = str(e)
            
        return attr_analysis
    
    def _analyze_osm_tags_in_layer(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Специфічний аналіз OSM тегів у шарі"""
        
        osm_analysis = {
            'detected_osm_columns': [],
            'key_osm_tags': {},
            'tag_completeness': {},
            'osm_structure_type': None
        }
        
        try:
            # Ключові OSM колонки
            key_osm_columns = [
                'osm_id', 'osm_type', 'osm_version', 'osm_timestamp',
                'amenity', 'shop', 'building', 'landuse', 'highway', 'railway',
                'natural', 'leisure', 'tourism', 'office', 'name', 'addr:housenumber',
                'addr:street', 'addr:city', 'place', 'barrier', 'man_made'
            ]
            
            # Виявлення OSM колонок
            existing_osm_cols = [col for col in gdf.columns if col in key_osm_columns]
            osm_analysis['detected_osm_columns'] = existing_osm_cols
            
            # Аналіз ключових тегів
            for tag in existing_osm_cols:
                if tag in gdf.columns:
                    non_null = gdf[tag].dropna()
                    if len(non_null) > 0:
                        osm_analysis['key_osm_tags'][tag] = {
                            'count': len(non_null),
                            'ratio': float(len(non_null) / len(gdf)),
                            'unique_values': int(non_null.nunique()),
                            'top_values': non_null.value_counts().head(10).to_dict()
                        }
            
            # Визначення типу OSM структури
            if 'osm_id' in gdf.columns:
                osm_analysis['osm_structure_type'] = 'standard_osm_export'
            elif any(col.startswith('osm_') for col in gdf.columns):
                osm_analysis['osm_structure_type'] = 'processed_osm_data'
            elif any(col in ['amenity', 'shop', 'building'] for col in gdf.columns):
                osm_analysis['osm_structure_type'] = 'osm_themed_export'
            else:
                osm_analysis['osm_structure_type'] = 'unknown_or_non_osm'
            
        except Exception as e:
            osm_analysis['error'] = str(e)
            
        return osm_analysis
    
    def _extract_representative_samples(self, gdf: gpd.GeoDataFrame, 
                                      sample_size: int) -> Dict[str, Any]:
        """Витягування репрезентативних прикладів"""
        
        samples = {
            'random_samples': [],
            'data_rich_samples': [],
            'geometry_samples': {}
        }
        
        try:
            if gdf.empty:
                return samples
            
            # Випадкові приклади
            random_sample = gdf.sample(min(sample_size, len(gdf)))
            for idx, row in random_sample.iterrows():
                sample_data = {}
                for col in gdf.columns:
                    if col == 'geometry':
                        if pd.notna(row[col]):
                            sample_data['geometry_type'] = row[col].geom_type
                            sample_data['geometry_valid'] = row[col].is_valid
                    else:
                        if pd.notna(row[col]):
                            sample_data[col] = str(row[col])[:200]  # Обрізаємо довгі значення
                
                samples['random_samples'].append(sample_data)
            
            # Приклади з найбільшою кількістю даних
            non_geom_cols = [col for col in gdf.columns if col != 'geometry']
            if non_geom_cols:
                data_richness = gdf[non_geom_cols].notna().sum(axis=1)
                rich_indices = data_richness.nlargest(min(sample_size, len(gdf))).index
                
                for idx in rich_indices:
                    row = gdf.loc[idx]
                    sample_data = {}
                    for col in gdf.columns:
                        if col == 'geometry':
                            if pd.notna(row[col]):
                                sample_data['geometry_type'] = row[col].geom_type
                        else:
                            if pd.notna(row[col]):
                                sample_data[col] = str(row[col])[:200]
                    
                    samples['data_rich_samples'].append(sample_data)
            
            # Приклади за типами геометрії
            if 'geometry' in gdf.columns:
                geom_types = gdf.geometry.geom_type.value_counts()
                for geom_type in geom_types.index[:5]:  # Топ 5 типів
                    type_examples = gdf[gdf.geometry.geom_type == geom_type].head(2)
                    samples['geometry_samples'][geom_type] = []
                    
                    for idx, row in type_examples.iterrows():
                        geom_sample = {
                            'geometry_type': geom_type,
                            'is_valid': row.geometry.is_valid if pd.notna(row.geometry) else False
                        }
                        
                        # Додаємо ключові атрибути
                        key_attrs = ['name', 'amenity', 'shop', 'building', 'highway']
                        for attr in key_attrs:
                            if attr in gdf.columns and pd.notna(row[attr]):
                                geom_sample[attr] = str(row[attr])
                        
                        samples['geometry_samples'][geom_type].append(geom_sample)
            
        except Exception as e:
            samples['error'] = str(e)
            
        return samples
    
    def _assess_layer_quality(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Оцінка якості даних у шарі"""
        
        quality = {
            'overall_score': 0.0,
            'geometry_quality': 0.0,
            'data_completeness': 0.0,
            'osm_richness': 0.0,
            'issues': [],
            'strengths': []
        }
        
        try:
            scores = []
            
            # 1. Якість геометрії
            if 'geometry' in gdf.columns and not gdf.geometry.empty:
                valid_ratio = gdf.geometry.is_valid.mean()
                non_null_ratio = gdf.geometry.notna().mean()
                quality['geometry_quality'] = (valid_ratio + non_null_ratio) / 2
                scores.append(quality['geometry_quality'])
                
                if valid_ratio < 0.95:
                    quality['issues'].append(f"Низька валідність геометрій: {valid_ratio:.2%}")
                else:
                    quality['strengths'].append("Високоякісні геометрії")
            
            # 2. Повнота даних
            non_geom_cols = [col for col in gdf.columns if col != 'geometry']
            if non_geom_cols:
                completeness = gdf[non_geom_cols].notna().mean().mean()
                quality['data_completeness'] = completeness
                scores.append(completeness)
                
                if completeness < 0.3:
                    quality['issues'].append(f"Низька повнота даних: {completeness:.2%}")
                elif completeness > 0.7:
                    quality['strengths'].append("Високий рівень заповненості даних")
            
            # 3. Багатство OSM тегів
            osm_cols = [col for col in gdf.columns if col in 
                       ['amenity', 'shop', 'building', 'landuse', 'highway', 'name']]
            if osm_cols:
                osm_completeness = gdf[osm_cols].notna().any(axis=1).mean()
                quality['osm_richness'] = osm_completeness
                scores.append(osm_completeness)
                
                if osm_completeness > 0.5:
                    quality['strengths'].append("Багаті OSM теги")
            
            # Загальна оцінка
            if scores:
                quality['overall_score'] = np.mean(scores)
                
                if quality['overall_score'] > 0.8:
                    quality['strengths'].append("Відмінна якість даних")
                elif quality['overall_score'] < 0.4:
                    quality['issues'].append("Загалом низька якість даних")
            
        except Exception as e:
            quality['error'] = str(e)
            
        return quality
    
    def _assess_h3_suitability(self, gdf: gpd.GeoDataFrame) -> Dict[str, Any]:
        """Оцінка придатності для H3 індексації"""
        
        h3_assessment = {
            'suitable_for_h3': False,
            'recommended_resolutions': [],
            'indexing_strategy': None,
            'expected_performance': {},
            'considerations': []
        }
        
        try:
            if 'geometry' not in gdf.columns or gdf.geometry.empty:
                return h3_assessment
            
            geom_types = gdf.geometry.geom_type.value_counts()
            primary_geom = geom_types.index[0] if len(geom_types) > 0 else None
            
            # Визначення придатності та стратегії
            if primary_geom == 'Point':
                h3_assessment['suitable_for_h3'] = True
                h3_assessment['indexing_strategy'] = 'direct_point_to_cell'
                h3_assessment['recommended_resolutions'] = [8, 9, 10]
                h3_assessment['considerations'].append("Ідеально для H3 - прямі точки")
                
            elif primary_geom in ['Polygon', 'MultiPolygon']:
                h3_assessment['suitable_for_h3'] = True
                h3_assessment['indexing_strategy'] = 'polygon_to_cells_coverage'
                h3_assessment['recommended_resolutions'] = [7, 8, 9]
                h3_assessment['considerations'].append("Потрібне покриття полігонів H3 комірками")
                
            elif primary_geom in ['LineString', 'MultiLineString']:
                h3_assessment['suitable_for_h3'] = True
                h3_assessment['indexing_strategy'] = 'line_intersecting_cells'
                h3_assessment['recommended_resolutions'] = [8, 9]
                h3_assessment['considerations'].append("Індексація через перетинання з H3 комірками")
            
            # Оцінка продуктивності
            feature_count = len(gdf)
            if feature_count < 10000:
                h3_assessment['expected_performance']['indexing_time'] = 'fast'
            elif feature_count < 100000:
                h3_assessment['expected_performance']['indexing_time'] = 'moderate'
            else:
                h3_assessment['expected_performance']['indexing_time'] = 'slow'
                h3_assessment['considerations'].append("Великий обсяг даних - потрібна оптимізація")
            
            # Просторова щільність
            if 'geometry' in gdf.columns:
                bounds = gdf.total_bounds
                area_deg2 = (bounds[2] - bounds[0]) * (bounds[3] - bounds[1])
                density = feature_count / area_deg2 if area_deg2 > 0 else 0
                
                h3_assessment['expected_performance']['spatial_density'] = density
                
                if density > 1000:  # Висока щільність
                    h3_assessment['considerations'].append("Висока просторова щільність - розглянути вищі резолюції")
                elif density < 10:  # Низька щільність
                    h3_assessment['considerations'].append("Низька просторова щільність - можна використати нижчі резолюції")
            
        except Exception as e:
            h3_assessment['error'] = str(e)
            
        return h3_assessment
    
    def _analyze_osm_tags_globally(self, layers_data: Dict) -> Dict[str, Any]:
        """Глобальний аналіз OSM тегів по всіх шарах"""
        
        global_osm = {
            'all_osm_columns': set(),
            'cross_layer_tags': {},
            'tag_distribution': {},
            'semantic_categories': {}
        }
        
        try:
            # Збір всіх OSM колонок
            for layer_name, layer_data in layers_data.items():
                if 'osm_tags_analysis' in layer_data:
                    osm_cols = layer_data['osm_tags_analysis'].get('detected_osm_columns', [])
                    global_osm['all_osm_columns'].update(osm_cols)
            
            global_osm['all_osm_columns'] = list(global_osm['all_osm_columns'])
            
            # Аналіз розподілу тегів по шарах
            for tag in global_osm['all_osm_columns']:
                tag_info = {'layers_with_tag': [], 'total_features': 0}
                
                for layer_name, layer_data in layers_data.items():
                    osm_tags = layer_data.get('osm_tags_analysis', {}).get('key_osm_tags', {})
                    if tag in osm_tags:
                        tag_info['layers_with_tag'].append({
                            'layer': layer_name,
                            'count': osm_tags[tag]['count'],
                            'ratio': osm_tags[tag]['ratio']
                        })
                        tag_info['total_features'] += osm_tags[tag]['count']
                
                if tag_info['layers_with_tag']:
                    global_osm['cross_layer_tags'][tag] = tag_info
            
            # Семантичні категорії
            categories = {
                'retail': ['shop', 'amenity', 'commercial'],
                'transport': ['highway', 'railway', 'public_transport'],
                'buildings': ['building', 'addr:housenumber', 'addr:street'],
                'places': ['place', 'name', 'tourism'],
                'land_use': ['landuse', 'natural', 'leisure']
            }
            
            for category, tags in categories.items():
                category_stats = {'relevant_tags': [], 'total_coverage': 0}
                
                for tag in tags:
                    if tag in global_osm['cross_layer_tags']:
                        category_stats['relevant_tags'].append({
                            'tag': tag,
                            'features': global_osm['cross_layer_tags'][tag]['total_features']
                        })
                        category_stats['total_coverage'] += global_osm['cross_layer_tags'][tag]['total_features']
                
                if category_stats['relevant_tags']:
                    global_osm['semantic_categories'][category] = category_stats
            
        except Exception as e:
            global_osm['error'] = str(e)
            
        return global_osm
    
    def _analyze_geometries_globally(self, layers_data: Dict) -> Dict[str, Any]:
        """Глобальний аналіз геометрій"""
        
        geometry_analysis = {
            'total_features_all_layers': 0,
            'geometry_type_distribution': {},
            'spatial_coverage': {},
            'complexity_statistics': {},
            'quality_summary': {}
        }
        
        try:
            # Збір статистики по всіх шарах
            all_geom_types = Counter()
            total_features = 0
            valid_geometries = 0
            all_bounds = []
            
            for layer_name, layer_data in layers_data.items():
                if 'basic_info' in layer_data:
                    layer_features = layer_data['basic_info'].get('total_features', 0)
                    total_features += layer_features
                    
                    # Збір типів геометрій
                    geom_details = layer_data.get('geometry_details', {})
                    geom_types = geom_details.get('geometry_types', {}).get('types_distribution', {})
                    for geom_type, count in geom_types.items():
                        all_geom_types[geom_type] += count
                    
                    # Валідність
                    validity = geom_details.get('validity', {})
                    valid_geometries += validity.get('valid_count', 0)
                    
                    # Bounds
                    spatial_dist = geom_details.get('spatial_distribution', {})
                    if 'bbox' in spatial_dist:
                        all_bounds.append(spatial_dist['bbox'])
            
            geometry_analysis['total_features_all_layers'] = total_features
            geometry_analysis['geometry_type_distribution'] = dict(all_geom_types)
            
            # Загальне покриття
            if all_bounds:
                minx = min(bbox['minx'] for bbox in all_bounds)
                miny = min(bbox['miny'] for bbox in all_bounds)
                maxx = max(bbox['maxx'] for bbox in all_bounds)
                maxy = max(bbox['maxy'] for bbox in all_bounds)
                
                geometry_analysis['spatial_coverage'] = {
                    'overall_bbox': {'minx': minx, 'miny': miny, 'maxx': maxx, 'maxy': maxy},
                    'coverage_area_deg2': (maxx - minx) * (maxy - miny),
                    'center_point': {'lat': (miny + maxy) / 2, 'lon': (minx + maxx) / 2}
                }
            
            # Підсумок якості
            if total_features > 0:
                geometry_analysis['quality_summary'] = {
                    'overall_validity_ratio': valid_geometries / total_features,
                    'geometric_diversity': len(all_geom_types),
                    'dominant_geometry_type': all_geom_types.most_common(1)[0][0] if all_geom_types else None
                }
            
        except Exception as e:
            geometry_analysis['error'] = str(e)
            
        return geometry_analysis
    
    def _generate_schema_recommendations(self, file_analysis: Dict) -> Dict[str, Any]:
        """Генерація рекомендацій для PostGIS схеми"""
        
        recommendations = {
            'table_structure': {},
            'indexing_strategy': {},
            'h3_integration': {},
            'performance_optimization': {},
            'data_quality_issues': []
        }
        
        try:
            layers_data = file_analysis.get('layers_detailed', {})
            osm_analysis = file_analysis.get('osm_tag_analysis', {})
            geometry_analysis = file_analysis.get('geometry_analysis', {})
            
            # Структура таблиць
            for layer_name, layer_data in layers_data.items():
                table_name = f"osm_{layer_name}"
                
                basic_info = layer_data.get('basic_info', {})
                geom_details = layer_data.get('geometry_details', {})
                osm_tags = layer_data.get('osm_tags_analysis', {})
                h3_suitability = layer_data.get('h3_suitability', {})
                
                table_rec = {
                    'estimated_rows': basic_info.get('total_features', 0),
                    'primary_geometry_type': geom_details.get('geometry_details', {}).get('primary_type'),
                    'recommended_columns': [],
                    'h3_columns': [],
                    'indexes_needed': []
                }
                
                # Базові колонки
                table_rec['recommended_columns'].extend([
                    'id SERIAL PRIMARY KEY',
                    'osm_id BIGINT',
                    f'geom GEOMETRY({table_rec["primary_geometry_type"]}, 4326)' if table_rec["primary_geometry_type"] else 'geom GEOMETRY',
                    'region_name VARCHAR(50)'
                ])
                
                # OSM-специфічні колонки
                detected_osm_cols = osm_tags.get('detected_osm_columns', [])
                for osm_col in detected_osm_cols:
                    if osm_col in ['amenity', 'shop', 'building', 'highway', 'landuse']:
                        table_rec['recommended_columns'].append(f'{osm_col} VARCHAR(100)')
                    elif osm_col == 'name':
                        table_rec['recommended_columns'].append('name VARCHAR(255)')
                    elif osm_col.startswith('addr:'):
                        table_rec['recommended_columns'].append(f'"{osm_col}" VARCHAR(255)')
                
                # H3 колонки
                if h3_suitability.get('suitable_for_h3', False):
                    for res in [7, 8, 9, 10]:
                        table_rec['h3_columns'].append(f'h3_res_{res} VARCHAR(15)')
                
                # Індекси
                table_rec['indexes_needed'].extend([
                    f'CREATE INDEX idx_{table_name}_geom ON {table_name} USING GIST (geom)',
                    f'CREATE INDEX idx_{table_name}_osm_id ON {table_name} (osm_id)'
                ])
                
                # H3 індекси
                for res in [7, 8, 9, 10]:
                    table_rec['indexes_needed'].append(
                        f'CREATE INDEX idx_{table_name}_h3_res_{res} ON {table_name} (h3_res_{res})'
                    )
                
                # Категоріальні індекси
                for cat_col in ['amenity', 'shop', 'building', 'highway']:
                    if cat_col in detected_osm_cols:
                        table_rec['indexes_needed'].append(
                            f'CREATE INDEX idx_{table_name}_{cat_col} ON {table_name} ({cat_col})'
                        )
                
                recommendations['table_structure'][table_name] = table_rec
            
            # Загальна стратегія індексації
            total_features = geometry_analysis.get('total_features_all_layers', 0)
            
            recommendations['indexing_strategy'] = {
                'h3_resolutions': [7, 8, 9, 10],
                'spatial_index_type': 'GIST',
                'partitioning_needed': total_features > 1000000,
                'materialized_views_recommended': total_features > 500000,
                'composite_indexes': [
                    '(region_name, h3_res_8)',
                    '(amenity, h3_res_9)',
                    '(shop, h3_res_9)'
                ]
            }
            
            # H3 інтеграція
            recommendations['h3_integration'] = {
                'precompute_h3_cells': True,
                'h3_functions_needed': [
                    'h3_geo_to_h3(lat, lon, resolution)',
                    'h3_h3_to_children(h3_index, resolution)',
                    'h3_h3_to_parent(h3_index, resolution)',
                    'h3_k_ring(h3_index, k)'
                ],
                'h3_utility_tables': [
                    'h3_ukraine_grid_res_7',
                    'h3_ukraine_grid_res_8', 
                    'h3_ukraine_grid_res_9',
                    'h3_ukraine_grid_res_10'
                ]
            }
            
            # Оптимізація продуктивності
            recommendations['performance_optimization'] = {
                'connection_pooling': True,
                'batch_insert_size': 10000,
                'vacuum_schedule': 'weekly',
                'analyze_schedule': 'daily',
                'cluster_on_h3': True if total_features > 100000 else False
            }
            
            # Проблеми якості даних
            for layer_name, layer_data in layers_data.items():
                quality = layer_data.get('data_quality', {})
                issues = quality.get('issues', [])
                if issues:
                    recommendations['data_quality_issues'].extend([
                        f"Шар {layer_name}: {issue}" for issue in issues
                    ])
            
        except Exception as e:
            recommendations['error'] = str(e)
            
        return recommendations
    
    def run_comprehensive_diagnosis(self, target_file: str = None) -> Dict[str, Any]:
        """Запуск повної діагностики"""
        
        logger.info("🚀 Запуск повної діагностики HOT OSM експортів")
        
        # Знаходимо файли
        gpkg_files = list(self.data_directory.glob("*.gpkg"))
        
        if not gpkg_files:
            logger.error(f"❌ Не знайдено .gpkg файлів в {self.data_directory}")
            return {}
        
        # Вибираємо файл для діагностики
        if target_file:
            target_path = self.data_directory / target_file
            if not target_path.exists():
                logger.error(f"❌ Файл {target_file} не знайдено")
                return {}
            files_to_analyze = [target_path]
        else:
            # Вибираємо найменший файл для швидкої діагностики
            files_to_analyze = sorted(gpkg_files, key=lambda f: f.stat().st_size)[:1]
            logger.info(f"📋 Обрано для діагностики: {files_to_analyze[0].name}")
        
        # Запуск діагностики
        diagnosis_results = {}
        
        for gpkg_file in files_to_analyze:
            file_diagnosis = self.diagnose_single_file(gpkg_file)
            diagnosis_results[gpkg_file.name] = file_diagnosis
        
        # Створення підсумкового звіту
        summary_report = self._create_diagnosis_summary(diagnosis_results)
        
        complete_diagnosis = {
            'diagnosis_timestamp': datetime.now().isoformat(),
            'analyzed_files': list(diagnosis_results.keys()),
            'file_details': diagnosis_results,
            'summary_report': summary_report,
            'next_steps': self._generate_next_steps(summary_report)
        }
        
        return complete_diagnosis
    
    def _create_diagnosis_summary(self, diagnosis_results: Dict) -> Dict[str, Any]:
        """Створення підсумкового звіту діагностики"""
        
        summary = {
            'files_analyzed': len(diagnosis_results),
            'total_layers_found': 0,
            'total_features': 0,
            'common_layer_structure': {},
            'osm_data_richness': {},
            'geometry_overview': {},
            'schema_complexity': {},
            'key_findings': []
        }
        
        try:
            all_layers = set()
            all_osm_tags = set()
            all_geom_types = Counter()
            
            for file_name, file_data in diagnosis_results.items():
                layers_detailed = file_data.get('layers_detailed', {})
                summary['total_layers_found'] += len(layers_detailed)
                
                for layer_name, layer_data in layers_detailed.items():
                    all_layers.add(layer_name)
                    
                    # Підрахунок об'єктів
                    basic_info = layer_data.get('basic_info', {})
                    summary['total_features'] += basic_info.get('total_features', 0)
                    
                    # OSM теги
                    osm_analysis = layer_data.get('osm_tags_analysis', {})
                    detected_cols = osm_analysis.get('detected_osm_columns', [])
                    all_osm_tags.update(detected_cols)
                    
                    # Геометрії
                    geom_details = layer_data.get('geometry_details', {})
                    geom_types = geom_details.get('geometry_types', {}).get('types_distribution', {})
                    for geom_type, count in geom_types.items():
                        all_geom_types[geom_type] += count
            
            summary['common_layer_structure'] = {
                'unique_layers': list(all_layers),
                'layer_count': len(all_layers)
            }
            
            summary['osm_data_richness'] = {
                'unique_osm_tags': list(all_osm_tags),
                'osm_tag_count': len(all_osm_tags),
                'key_retail_tags': [tag for tag in all_osm_tags 
                                  if tag in ['amenity', 'shop', 'building', 'name']]
            }
            
            summary['geometry_overview'] = {
                'geometry_types_found': dict(all_geom_types),
                'dominant_geometry': all_geom_types.most_common(1)[0][0] if all_geom_types else None
            }
            
            # Ключові знахідки
            if len(all_layers) <= 3:
                summary['key_findings'].append(f"Простична структура: {len(all_layers)} шарів")
            else:
                summary['key_findings'].append(f"Складна структура: {len(all_layers)} шарів")
            
            if 'amenity' in all_osm_tags and 'shop' in all_osm_tags:
                summary['key_findings'].append("Відмінно підходить для ретейл аналізу")
            
            if summary['total_features'] > 100000:
                summary['key_findings'].append("Великий обсяг даних - потрібна оптимізація")
            
        except Exception as e:
            summary['error'] = str(e)
            
        return summary
    
    def _generate_next_steps(self, summary: Dict) -> List[str]:
        """Генерація наступних кроків"""
        
        next_steps = []
        
        try:
            total_features = summary.get('total_features', 0)
            osm_richness = summary.get('osm_data_richness', {})
            
            # Базові кроки
            next_steps.append("1. Створити PostGIS схему на основі рекомендацій")
            next_steps.append("2. Розробити ETL пайплайн для імпорту OSM даних")
            
            # Умовні кроки
            if total_features > 500000:
                next_steps.append("3. Налаштувати партиціонування таблиць за регіонами")
                next_steps.append("4. Створити матеріалізовані представлення для агрегацій")
            
            if len(osm_richness.get('key_retail_tags', [])) >= 3:
                next_steps.append("3. Створити спеціалізовані індекси для ретейл аналізу")
            
            next_steps.append("5. Інтегрувати H3 індексацію для всіх резолюцій (7-10)")
            next_steps.append("6. Протестувати імпорт на одній області")
            next_steps.append("7. Масштабувати на всі області України")
            
        except Exception as e:
            next_steps.append(f"Помилка генерації кроків: {e}")
            
        return next_steps
    
    def save_diagnosis_report(self, diagnosis_results: Dict, output_path: str = None):
        """Збереження звіту діагностики"""
        
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"hot_osm_diagnosis_{timestamp}.json"
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(diagnosis_results, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"✅ Звіт діагностики збережено: {output_path}")
            
        except Exception as e:
            logger.error(f"❌ Помилка збереження звіту: {e}")
    
    def print_diagnosis_summary(self, diagnosis_results: Dict):
        """Виведення короткого звіту діагностики"""
        
        print("\n" + "="*80)
        print("🔍 ДІАГНОСТИКА HOT OSM ЕКСПОРТІВ - ПІДСУМОК")
        print("="*80)
        
        summary = diagnosis_results.get('summary_report', {})
        file_details = diagnosis_results.get('file_details', {})
        
        print(f"📅 Час діагностики: {diagnosis_results.get('diagnosis_timestamp', 'N/A')}")
        print(f"📁 Проаналізовано файлів: {summary.get('files_analyzed', 0)}")
        print(f"📊 Знайдено шарів: {summary.get('total_layers_found', 0)}")
        print(f"🔢 Загалом об'єктів: {summary.get('total_features', 0):,}")
        
        # Структура шарів
        print(f"\n📋 СТРУКТУРА ШАРІВ:")
        layer_structure = summary.get('common_layer_structure', {})
        unique_layers = layer_structure.get('unique_layers', [])
        for layer in unique_layers:
            print(f"  • {layer}")
        
        # OSM багатство
        print(f"\n🏷️ OSM ТЕГИ:")
        osm_richness = summary.get('osm_data_richness', {})
        osm_tags = osm_richness.get('unique_osm_tags', [])
        retail_tags = osm_richness.get('key_retail_tags', [])
        
        print(f"  Всього унікальних тегів: {len(osm_tags)}")
        print(f"  Ключові ретейл теги: {', '.join(retail_tags)}")
        
        # Геометрії
        print(f"\n📐 ГЕОМЕТРІЇ:")
        geom_overview = summary.get('geometry_overview', {})
        geom_types = geom_overview.get('geometry_types_found', {})
        for geom_type, count in geom_types.items():
            print(f"  • {geom_type}: {count:,} об'єктів")
        
        # Ключові знахідки
        print(f"\n💡 КЛЮЧОВІ ЗНАХІДКИ:")
        key_findings = summary.get('key_findings', [])
        for finding in key_findings:
            print(f"  ✓ {finding}")
        
        # Детальна інформація по файлах
        print(f"\n📄 ДЕТАЛІ ПО ФАЙЛАХ:")
        for file_name, file_data in file_details.items():
            file_info = file_data.get('file_info', {})
            sqlite_structure = file_data.get('sqlite_structure', {})
            
            print(f"\n  📁 {file_name}")
            print(f"    Розмір: {file_info.get('size_mb', 0):.1f} MB")
            
            tables = sqlite_structure.get('tables', [])
            print(f"    SQLite таблиці ({len(tables)}):")
            for table in tables[:5]:  # Показуємо перші 5
                print(f"      • {table.get('name', 'N/A')}: {table.get('row_count', 0):,} записів")
            
            if len(tables) > 5:
                print(f"      ... та ще {len(tables) - 5} таблиць")
        
        # Рекомендації
        print(f"\n🎯 НАСТУПНІ КРОКИ:")
        next_steps = diagnosis_results.get('next_steps', [])
        for step in next_steps:
            print(f"  {step}")
        
        print(f"\n{'='*80}")


def main():
    """Запуск діагностики HOT OSM експортів"""
    
    data_directory = r"C:\OSMData"
    
    print("🔍 Запуск діагностики HOT OSM експортів...")
    print(f"📁 Директорія: {data_directory}")
    
    analyzer = HOTOSMDiagnosticAnalyzer(data_directory)
    
    try:
        # Запуск повної діагностики
        diagnosis_results = analyzer.run_comprehensive_diagnosis()
        
        if diagnosis_results:
            # Збереження звіту
            analyzer.save_diagnosis_report(diagnosis_results)
            
            # Виведення підсумку
            analyzer.print_diagnosis_summary(diagnosis_results)
            
            print(f"\n✅ Діагностика завершена успішно!")
        else:
            print(f"❌ Діагностика не дала результатів")
            
    except Exception as e:
        print(f"❌ Помилка діагностики: {e}")
        logger.exception("Детальна інформація про помилку:")


if __name__ == "__main__":
    main()