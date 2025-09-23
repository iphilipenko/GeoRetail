"""
Tests for Metrics
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import MetricsService
from ...schemas import *


class TestMetrics:
    """Test suite for Metrics"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return MetricsService()
    
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
            permissions=["core.view_map", "explorer.access"]
        )
    
    
    async def test_get_bivariate(self, service, mock_db, mock_user):
        """Test get_bivariate endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_bivariate = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_bivariate(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_bivariate.assert_called_once()

    async def test_get_available(self, service, mock_db, mock_user):
        """Test get_available endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_available = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_available(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_available.assert_called_once()

    async def test_get_calculate(self, service, mock_db, mock_user):
        """Test get_calculate endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_calculate = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_calculate(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_calculate.assert_called_once()

    
    # TODO: Add more test cases
