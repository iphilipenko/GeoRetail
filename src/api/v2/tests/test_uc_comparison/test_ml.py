"""
Tests for Ml
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import MlService
from ...schemas import *


class TestMl:
    """Test suite for Ml"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return MlService()
    
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
    
    
    async def test_get_predict_revenue(self, service, mock_db, mock_user):
        """Test get_predict_revenue endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_predict_revenue = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_predict_revenue(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_predict_revenue.assert_called_once()

    async def test_get_confidence_scores(self, service, mock_db, mock_user):
        """Test get_confidence_scores endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_confidence_scores = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_confidence_scores(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_confidence_scores.assert_called_once()

    async def test_get_similar_locations(self, service, mock_db, mock_user):
        """Test get_similar_locations endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_similar_locations = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_similar_locations(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_similar_locations.assert_called_once()

    
    # TODO: Add more test cases
