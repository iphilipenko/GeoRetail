# test_h3_modal_endpoints.py
"""
🧪 Тести для H3 Modal API endpoints
"""

import pytest
from fastapi.testclient import TestClient
import json
from datetime import datetime

# Приклад H3 індексів для Києва
SAMPLE_H3_INDICES = {
    7: "871fb4662ffffff",
    8: "881fb46622fffff", 
    9: "891fb466227ffff",
    10: "8a1fb46622d7fff"
}

class TestHexagonDetails:
    """Тести для /hexagon-details endpoints"""
    
    def test_get_hexagon_details_basic(self, client: TestClient):
        """Тест отримання детальної інформації про гексагон"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(f"/api/v1/hexagon-details/details/{h3_index}?resolution=10")
        
        assert response.status_code == 200
        data = response.json()
        
        # Перевіряємо основну структуру
        assert "location_info" in data
        assert "metrics" in data
        assert "poi_details" in data
        assert "influence_analysis" in data
        assert "neighbor_coverage" in data
        assert "available_analyses" in data
        assert "generated_at" in data
        
        # Перевіряємо location_info
        location = data["location_info"]
        assert location["h3_index"] == h3_index
        assert location["resolution"] == 10
        assert "center_lon" in location
        assert "center_lat" in location
        assert "area_km2" in location
        
        # Перевіряємо neighbor_coverage
        coverage = data["neighbor_coverage"]
        assert "rings" in coverage
        assert "hexagon_count" in coverage
        assert "area_km2" in coverage
        assert "radius_estimate_m" in coverage
    
    def test_analysis_type_pedestrian_competition(self, client: TestClient):
        """Тест аналізу пішохідної конкуренції"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(
            f"/api/v1/hexagon-details/details/{h3_index}"
            "?resolution=10&analysis_type=pedestrian_competition"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Перевіряємо що використовується мала кількість кілець (для пішохідної доступності)
        coverage = data["neighbor_coverage"]
        assert coverage["rings"] <= 3  # Максимум для pedestrian
        assert coverage["area_km2"] <= 1.0  # Приблизно для 0.5 км² target
    
    def test_analysis_type_market_overview(self, client: TestClient):
        """Тест аналізу огляду ринку"""
        h3_index = SAMPLE_H3_INDICES[8]  # Використовуємо менший resolution
        response = client.get(
            f"/api/v1/hexagon-details/details/{h3_index}"
            "?resolution=8&analysis_type=market_overview"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Перевіряємо що використовується більше кілець (для ринкового огляду)
        coverage = data["neighbor_coverage"]
        assert coverage["rings"] >= 2  # Мінімум для market overview
        
    def test_custom_rings(self, client: TestClient):
        """Тест користувацького кількості кілець"""
        h3_index = SAMPLE_H3_INDICES[9]
        custom_rings = 5
        
        response = client.get(
            f"/api/v1/hexagon-details/details/{h3_index}"
            f"?resolution=9&analysis_type=custom&custom_rings={custom_rings}"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Перевіряємо що використовується точно вказана кількість кілець
        coverage = data["neighbor_coverage"]
        assert coverage["rings"] == custom_rings
    
    def test_available_analyses_structure(self, client: TestClient):
        """Тест структури доступних аналізів"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(f"/api/v1/hexagon-details/details/{h3_index}?resolution=10")
        
        assert response.status_code == 200
        data = response.json()
        
        analyses = data["available_analyses"]
        assert len(analyses) >= 4  # Мінімум 4 типи аналізу
        
        # Перевіряємо структуру кожного аналізу
        for analysis in analyses:
            assert "analysis_type" in analysis
            assert "name" in analysis
            assert "description" in analysis
            assert "optimal_rings" in analysis
            assert "estimated_area_km2" in analysis
            assert "hexagon_count" in analysis
            assert "max_rings" in analysis
    
    def test_poi_influence_analysis(self, client: TestClient):
        """Тест аналізу впливу POI та брендів"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(f"/api/v1/hexagon-details/details/{h3_index}?resolution=10")
        
        assert response.status_code == 200
        data = response.json()
        
        # Перевіряємо POI details
        poi_details = data["poi_details"]
        assert isinstance(poi_details, list)
        
        if poi_details:  # Якщо є POI
            first_poi = poi_details[0]
            assert "name" in first_poi
            assert "functional_group" in first_poi
            assert "influence_weight" in first_poi
        
        # Перевіряємо influence analysis
        influence_analysis = data["influence_analysis"]
        assert isinstance(influence_analysis, list)
        
        if influence_analysis:  # Якщо є аналіз впливу
            first_analysis = influence_analysis[0]
            assert "functional_group" in first_analysis
            assert "count" in first_analysis
            assert "brands" in first_analysis

class TestAnalysisPreview:
    """Тести для /analysis-preview endpoint"""
    
    def test_get_analysis_preview(self, client: TestClient):
        """Тест отримання preview аналізів"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(f"/api/v1/hexagon-details/analysis-preview/{h3_index}?resolution=10")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "h3_index" in data
        assert "resolution" in data
        assert "available_analyses" in data
        assert "generated_at" in data
        
        analyses = data["available_analyses"]
        assert len(analyses) >= 4
        
        # Перевіряємо що є всі основні типи
        analysis_types = [a["analysis_type"] for a in analyses]
        assert "pedestrian_competition" in analysis_types
        assert "site_selection" in analysis_types
        assert "market_overview" in analysis_types
        assert "custom" in analysis_types

class TestCoverageCalculator:
    """Тести для /coverage-calculator endpoint"""
    
    def test_coverage_calculator_basic(self, client: TestClient):
        """Тест базового калькулятора покриття"""
        response = client.get("/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=2")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "resolution" in data
        assert "rings" in data
        assert "total_area_km2" in data
        assert "total_hexagon_count" in data
        assert "radius_estimate_m" in data
        assert "coverage_breakdown" in data
        assert "recommendations" in data
        
        # Перевіряємо розрахунки
        assert data["resolution"] == 10
        assert data["rings"] == 2
        assert data["total_hexagon_count"] == 19  # 1 + 3*2*(2+1) = 19
    
    def test_coverage_calculator_different_resolutions(self, client: TestClient):
        """Тест калькулятора для різних resolutions"""
        rings = 1
        
        for resolution in [7, 8, 9, 10]:
            response = client.get(
                f"/api/v1/hexagon-details/coverage-calculator?resolution={resolution}&rings={rings}"
            )
            
            assert response.status_code == 200
            data = response.json()
            
            assert data["resolution"] == resolution
            assert data["rings"] == rings
            assert data["total_hexagon_count"] == 7  # 1 + 3*1*(1+1) = 7
            
            # Площа повинна збільшуватися для менших resolution
            if resolution == 7:
                assert data["total_area_km2"] > 30  # H3-7 має великі гексагони
            elif resolution == 10:
                assert data["total_area_km2"] < 1   # H3-10 має маленькі гексагони
    
    def test_coverage_breakdown(self, client: TestClient):
        """Тест детального breakdown покриття"""
        response = client.get("/api/v1/hexagon-details/coverage-calculator?resolution=9&rings=3")
        
        assert response.status_code == 200
        data = response.json()
        
        breakdown = data["coverage_breakdown"]
        assert len(breakdown) >= 4  # 0, 1, 2, 3 кільця
        
        # Перевіряємо що breakdown правильно структурований
        for i, step in enumerate(breakdown):
            assert "rings" in step
            assert "area_km2" in step
            assert "hexagon_count" in step
            assert "description" in step
            assert step["rings"] == i
    
    def test_coverage_recommendations(self, client: TestClient):
        """Тест рекомендацій по покриттю"""
        # Тест для пішохідного радіусу
        response = client.get("/api/v1/hexagon-details/coverage-calculator?resolution=10&rings=1")
        data = response.json()
        recommendations = data["recommendations"]
        assert recommendations["pedestrian_range"] is True
        
        # Тест для автомобільного радіусу
        response = client.get("/api/v1/hexagon-details/coverage-calculator?resolution=8&rings=4")
        data = response.json()
        recommendations = data["recommendations"]
        assert recommendations["car_accessible"] is True
        assert recommendations["market_overview"] is True

class TestPOIInHexagon:
    """Тести для /poi-in-hexagon endpoint"""
    
    def test_get_poi_in_hexagon(self, client: TestClient):
        """Тест отримання POI в гексагоні"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(f"/api/v1/hexagon-details/poi-in-hexagon/{h3_index}?resolution=10")
        
        assert response.status_code == 200
        data = response.json()
        
        assert "h3_index" in data
        assert "resolution" in data
        assert "neighbor_rings" in data
        assert "coverage_area_km2" in data
        assert "poi_count" in data
        assert "poi_details" in data
        assert "generated_at" in data
    
    def test_poi_with_neighbors(self, client: TestClient):
        """Тест POI з сусідніми гексагонами"""
        h3_index = SAMPLE_H3_INDICES[9]
        neighbor_rings = 2
        
        response = client.get(
            f"/api/v1/hexagon-details/poi-in-hexagon/{h3_index}"
            f"?resolution=9&include_neighbors={neighbor_rings}"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert data["neighbor_rings"] == neighbor_rings
        assert data["coverage_area_km2"] > 0
        
        # POI count повинен бути >= 0
        assert data["poi_count"] >= 0
        assert len(data["poi_details"]) == data["poi_count"]

class TestCompetitiveAnalysis:
    """Тести для /competitive-analysis endpoint"""
    
    def test_competitive_analysis(self, client: TestClient):
        """Тест конкурентного аналізу"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(
            f"/api/v1/hexagon-details/competitive-analysis/{h3_index}"
            "?resolution=10&radius_rings=2"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        assert "h3_index" in data
        assert "resolution" in data
        assert "analysis_radius_rings" in data
        assert "coverage_area_km2" in data
        assert "competitors_found" in data
        assert "total_competitive_pressure" in data
        assert "competitors" in data
        assert "competitive_analysis" in data
        assert "generated_at" in data
    
    def test_competitive_pressure_calculation(self, client: TestClient):
        """Тест розрахунку конкурентного тиску"""
        h3_index = SAMPLE_H3_INDICES[9]
        response = client.get(
            f"/api/v1/hexagon-details/competitive-analysis/{h3_index}"
            "?resolution=9&radius_rings=3"
        )
        
        assert response.status_code == 200
        data = response.json()
        
        # Конкурентний тиск повинен бути числом (може бути 0)
        assert isinstance(data["total_competitive_pressure"], (int, float))
        assert data["competitors_found"] >= 0
        
        # Якщо є конкуренти, повинна бути інформація про них
        if data["competitors_found"] > 0:
            assert len(data["competitors"]) > 0
            assert len(data["competitive_analysis"]) > 0

class TestErrorHandling:
    """Тести обробки помилок"""
    
    def test_invalid_h3_index(self, client: TestClient):
        """Тест з неправильним H3 індексом"""
        invalid_h3 = "invalid_h3_index"
        response = client.get(f"/api/v1/hexagon-details/details/{invalid_h3}?resolution=10")
        
        # Повинна повертатися помилка 404 або 500
        assert response.status_code in [404, 500]
    
    def test_invalid_resolution(self, client: TestClient):
        """Тест з неправильним resolution"""
        h3_index = SAMPLE_H3_INDICES[10]
        
        # Занадто малий resolution
        response = client.get(f"/api/v1/hexagon-details/details/{h3_index}?resolution=5")
        assert response.status_code == 422  # Validation error
        
        # Занадто великий resolution
        response = client.get(f"/api/v1/hexagon-details/details/{h3_index}?resolution=15")
        assert response.status_code == 422  # Validation error
    
    def test_invalid_analysis_type(self, client: TestClient):
        """Тест з неправильним типом аналізу"""
        h3_index = SAMPLE_H3_INDICES[10]
        response = client.get(
            f"/api/v1/hexagon-details/details/{h3_index}"
            "?resolution=10&analysis_type=invalid_type"
        )
        
        assert response.status_code == 422  # Validation error
    
    def test_custom_rings_validation(self, client: TestClient):
        """Тест валідації custom rings"""
        h3_index = SAMPLE_H3_INDICES[10]
        
        # Занадто багато кілець
        response = client.get(
            f"/api/v1/hexagon-details/details/{h3_index}"
            "?resolution=10&analysis_type=custom&custom_rings=15"
        )
        assert response.status_code == 422  # Validation error
        
        # Від'ємна кількість кілець
        response = client.get(
            f"/api/v1/hexagon-details/details/{h3_index}"
            "?resolution=10&analysis_type=custom&custom_rings=-1"
        )
        assert response.status_code == 422  # Validation error

# Допоміжні функції для тестів
@pytest.fixture
def client():
    """Фікстура для FastAPI test client"""
    from fastapi.testclient import TestClient
    from src.main import app  # Імпортуємо головний FastAPI app
    
    return TestClient(app)

@pytest.fixture
def sample_hexagon_data():
    """Фікстура з прикладом даних гексагона"""
    return {
        "h3_index": SAMPLE_H3_INDICES[10],
        "resolution": 10,
        "analysis_type": "site_selection"
    }
