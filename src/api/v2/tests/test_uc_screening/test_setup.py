"""
Tests for Setup
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import SetupService
from ...schemas import *


class TestSetup:
    """Test suite for Setup"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return SetupService()
    
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
            permissions=["core.view_map", "screening.access"]
        )
    
    
    async def test_get_templates(self, service, mock_db, mock_user):
        """Test get_templates endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_templates = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_templates(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_templates.assert_called_once()

    async def test_get_criteria(self, service, mock_db, mock_user):
        """Test get_criteria endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_criteria = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_criteria(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_criteria.assert_called_once()

    async def test_get_filters(self, service, mock_db, mock_user):
        """Test get_filters endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_filters = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_filters(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_filters.assert_called_once()

    
    # TODO: Add more test cases
