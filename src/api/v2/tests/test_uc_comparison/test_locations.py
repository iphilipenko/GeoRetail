"""
Tests for Locations
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import LocationsService
from ...schemas import *


class TestLocations:
    """Test suite for Locations"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return LocationsService()
    
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
    
    
    async def test_get_add(self, service, mock_db, mock_user):
        """Test get_add endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_add = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_add(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_add.assert_called_once()

    async def test_get_remove(self, service, mock_db, mock_user):
        """Test get_remove endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_remove = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_remove(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_remove.assert_called_once()

    async def test_get_list(self, service, mock_db, mock_user):
        """Test get_list endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_list = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_list(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_list.assert_called_once()

    
    # TODO: Add more test cases
