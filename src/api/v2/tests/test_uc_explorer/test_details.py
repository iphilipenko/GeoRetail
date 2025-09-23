"""
Tests for Details
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import DetailsService
from ...schemas import *


class TestDetails:
    """Test suite for Details"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return DetailsService()
    
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
    
    
    async def test_get_territory(self, service, mock_db, mock_user):
        """Test get_territory endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_territory = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_territory(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_territory.assert_called_once()

    async def test_get_hexagon(self, service, mock_db, mock_user):
        """Test get_hexagon endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_hexagon = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_hexagon(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_hexagon.assert_called_once()

    async def test_get_statistics(self, service, mock_db, mock_user):
        """Test get_statistics endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_statistics = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_statistics(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_statistics.assert_called_once()

    
    # TODO: Add more test cases
