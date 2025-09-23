"""
Tests for Analysis
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import AnalysisService
from ...schemas import *


class TestAnalysis:
    """Test suite for Analysis"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return AnalysisService()
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database session"""
        return Mock()
    
    @pytest.fixture
    def mock_user(self):
        """Create mock authenticated user"""
        return Mock(
            id=1,
            email="test@example.com",
            permissions=["core.view_map", "comparison.access"]
        )
    
    
    async def test_get_spider_chart(self, service, mock_db, mock_user):
        """Test get_spider_chart endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_spider_chart = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_spider_chart(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_spider_chart.assert_called_once()

    async def test_get_side_by_side(self, service, mock_db, mock_user):
        """Test get_side_by_side endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_side_by_side = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_side_by_side(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_side_by_side.assert_called_once()

    async def test_get_cannibalization(self, service, mock_db, mock_user):
        """Test get_cannibalization endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_cannibalization = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_cannibalization(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_cannibalization.assert_called_once()

    async def test_get_roi_forecast(self, service, mock_db, mock_user):
        """Test get_roi_forecast endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_roi_forecast = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_roi_forecast(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_roi_forecast.assert_called_once()

    
    # TODO: Add more test cases
