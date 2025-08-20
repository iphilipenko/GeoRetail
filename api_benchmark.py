#!/usr/bin/env python3
"""
API Performance Benchmark для H3 endpoints
Тестує різні комбінації metric_type, resolution та limits
"""

import requests
import time
import json
from typing import Dict, List, Tuple
from dataclasses import dataclass
import pandas as pd

@dataclass
class BenchmarkResult:
    metric_type: str
    resolution: int
    limit: int
    response_time_ms: int
    hexagons_count: int
    response_size_kb: int
    success: bool
    error_message: str = ""

class APIBenchmark:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.endpoint = f"{base_url}/api/v1/visualization/kyiv-h3"
        self.results: List[BenchmarkResult] = []
    
    def test_single_request(self, metric_type: str, resolution: int, limit: int) -> BenchmarkResult:
        """Тестує один API запит"""
        params = {
            'metric_type': metric_type,
            'resolution': resolution,
            'limit': limit
        }
        
        try:
            print(f"🔄 Testing {metric_type} H3-{resolution} limit={limit}...")
            
            start_time = time.time()
            response = requests.get(self.endpoint, params=params, timeout=30)
            end_time = time.time()
            
            response_time_ms = int((end_time - start_time) * 1000)
            
            if response.status_code == 200:
                data = response.json()
                hexagons_count = len(data.get('hexagons', []))
                response_size_kb = len(response.content) // 1024
                
                result = BenchmarkResult(
                    metric_type=metric_type,
                    resolution=resolution,
                    limit=limit,
                    response_time_ms=response_time_ms,
                    hexagons_count=hexagons_count,
                    response_size_kb=response_size_kb,
                    success=True
                )
                print(f"✅ {response_time_ms}ms, {hexagons_count} hexagons, {response_size_kb}KB")
                return result
            else:
                print(f"❌ HTTP {response.status_code}")
                return BenchmarkResult(
                    metric_type=metric_type,
                    resolution=resolution,
                    limit=limit,
                    response_time_ms=response_time_ms,
                    hexagons_count=0,
                    response_size_kb=0,
                    success=False,
                    error_message=f"HTTP {response.status_code}"
                )
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            return BenchmarkResult(
                metric_type=metric_type,
                resolution=resolution,
                limit=limit,
                response_time_ms=0,
                hexagons_count=0,
                response_size_kb=0,
                success=False,
                error_message=str(e)
            )
    
    def run_full_benchmark(self):
        """Запускає повний benchmark всіх комбінацій"""
        
        # Дані з бази про кількість гексагонів
        db_stats = {
            7: 8714,
            8: 30840, 
            9: 78220,
            10: 141502
        }
        
        print("🚀 Запускаємо API Performance Benchmark")
        print("=" * 50)
        
        # Тестові конфігурації
        metrics = ['opportunity', 'competition']
        resolutions = [7, 8, 9, 10]
        
        # Різні limits для тестування
        test_limits = [
            1000,    # Малий dataset
            5000,    # Середній dataset  
            50000,   # Великий dataset
            1000000  # Unlimited (як в legacy)
        ]
        
        total_tests = len(metrics) * len(resolutions) * len(test_limits)
        current_test = 0
        
        for metric in metrics:
            for resolution in resolutions:
                expected_hexagons = db_stats[resolution]
                
                for limit in test_limits:
                    current_test += 1
                    print(f"\n[{current_test}/{total_tests}] ", end="")
                    
                    # Додаємо невелику паузу між запитами
                    if current_test > 1:
                        time.sleep(0.5)
                    
                    result = self.test_single_request(metric, resolution, limit)
                    self.results.append(result)
        
        print(f"\n✅ Benchmark завершено! Протестовано {total_tests} комбінацій")
        
    def analyze_results(self):
        """Аналізує результати benchmark"""
        if not self.results:
            print("❌ Немає результатів для аналізу")
            return
            
        print("\n" + "=" * 80)
        print("📊 АНАЛІЗ РЕЗУЛЬТАТІВ BENCHMARK")
        print("=" * 80)
        
        # Створюємо DataFrame для аналізу
        df_data = []
        for result in self.results:
            if result.success:
                df_data.append({
                    'metric': result.metric_type,
                    'resolution': result.resolution,
                    'limit': result.limit,
                    'time_ms': result.response_time_ms,
                    'hexagons': result.hexagons_count,
                    'size_kb': result.response_size_kb,
                    'time_per_hexagon': result.response_time_ms / max(result.hexagons_count, 1)
                })
        
        if not df_data:
            print("❌ Немає успішних запитів для аналізу")
            return
            
        df = pd.DataFrame(df_data)
        
        # Таблиця результатів
        print("\n🔍 ДЕТАЛЬНІ РЕЗУЛЬТАТИ:")
        print(f"{'Metric':<12} {'Res':<4} {'Limit':<8} {'Time(ms)':<10} {'Hexagons':<10} {'Size(KB)':<10} {'ms/hex':<8}")
        print("-" * 75)
        
        for _, row in df.iterrows():
            print(f"{row['metric']:<12} H3-{row['resolution']:<3} {row['limit']:<8} {row['time_ms']:<10} {row['hexagons']:<10} {row['size_kb']:<10} {row['time_per_hexagon']:.2f}")
        
        # Аналіз по resolutions
        print(f"\n📊 СЕРЕДНІЙ ЧАС ПО RESOLUTIONS:")
        res_analysis = df.groupby('resolution').agg({
            'time_ms': ['mean', 'min', 'max'],
            'hexagons': 'mean',
            'size_kb': 'mean'
        }).round(2)
        print(res_analysis)
        
        # Legacy simulation (8 запитів послідовно)
        print(f"\n⚡ LEGACY vs SMART LOADING ПОРІВНЯННЯ:")
        
        # Legacy: всі 8 комбінацій послідовно з limit=1000000
        legacy_total = 0
        legacy_requests = []
        
        for metric in ['opportunity', 'competition']:
            for res in [7, 8, 9, 10]:
                matching = df[(df['metric'] == metric) & 
                             (df['resolution'] == res) & 
                             (df['limit'] == 1000000)]
                if not matching.empty:
                    time_ms = matching.iloc[0]['time_ms']
                    legacy_total += time_ms
                    legacy_requests.append(f"{metric} H3-{res}: {time_ms}ms")
        
        print(f"\n❌ LEGACY APPROACH (послідовно):")
        for req in legacy_requests:
            print(f"  {req}")
        print(f"  ЗАГАЛОМ: {legacy_total}ms = {legacy_total/1000:.1f} секунд")
        
        # Smart Loading simulation
        print(f"\n✅ SMART LOADING APPROACH:")
        
        # Tier 1: opportunity H3-8 limit=1000
        tier1 = df[(df['metric'] == 'opportunity') & 
                   (df['resolution'] == 8) & 
                   (df['limit'] == 1000)]
        
        if not tier1.empty:
            tier1_time = tier1.iloc[0]['time_ms']
            print(f"  Tier 1 (critical): {tier1_time}ms - opportunity H3-8")
            print(f"  📈 User can interact after {tier1_time/1000:.1f} seconds!")
            
            improvement = ((legacy_total - tier1_time) / legacy_total) * 100
            print(f"  🚀 Improvement: {improvement:.1f}% faster time to interactive!")
        
        # Виявлення bottlenecks
        print(f"\n🎯 ВУЗЬКІ МІСЦЯ:")
        slowest = df.nlargest(3, 'time_ms')
        for _, row in slowest.iterrows():
            print(f"  {row['metric']} H3-{row['resolution']} limit={row['limit']}: {row['time_ms']}ms")
            
        # Рекомендації
        print(f"\n💡 РЕКОМЕНДАЦІЇ ДЛЯ SMART LOADING:")
        
        # Найшвидші комбінації для Tier 1
        fastest_small = df[df['limit'] <= 1000].nsmallest(3, 'time_ms')
        print(f"  Tier 1 candidates (швидкі + малі):")
        for _, row in fastest_small.iterrows():
            print(f"    {row['metric']} H3-{row['resolution']}: {row['time_ms']}ms, {row['hexagons']} hex")
            
        print(f"\n🎯 ВИСНОВКИ:")
        print(f"  • Legacy блокує UI на {legacy_total/1000:.1f} секунд")
        print(f"  • Smart Tier 1 дає взаємодію через {tier1_time/1000:.1f} секунди")
        print(f"  • Потенціал покращення: {improvement:.0f}%")

def main():
    benchmark = APIBenchmark()
    
    print("🧪 H3 API Performance Benchmark")
    print("Тестуємо різні combinations metric_type + resolution + limit")
    print()
    
    # Запускаємо benchmark
    benchmark.run_full_benchmark()
    
    # Аналізуємо результати
    benchmark.analyze_results()
    
    print(f"\n🎯 Benchmark завершено!")

if __name__ == "__main__":
    main()