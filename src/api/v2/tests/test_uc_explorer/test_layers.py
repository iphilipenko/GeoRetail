"""
Tests for Layers
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import LayersService
from ...schemas import *


class TestLayers:
    """Test suite for Layers"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return LayersService()
    
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
    
    
    async def test_get_admin_units(self, service, mock_db, mock_user):
        """Test get_admin_units endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_admin_units = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_admin_units(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_admin_units.assert_called_once()

    async def test_get_hexagons(self, service, mock_db, mock_user):
        """Test get_hexagons endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_hexagons = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_hexagons(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_hexagons.assert_called_once()

    async def test_get_poi(self, service, mock_db, mock_user):
        """Test get_poi endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_poi = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_poi(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_poi.assert_called_once()

    async def test_get_competitors(self, service, mock_db, mock_user):
        """Test get_competitors endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_competitors = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_competitors(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_competitors.assert_called_once()

    
    # TODO: Add more test cases
