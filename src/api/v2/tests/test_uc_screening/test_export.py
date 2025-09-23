"""
Tests for Export
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Import modules to test
from ..endpoints import router
from ..services import ExportService
from ...schemas import *


class TestExport:
    """Test suite for Export"""
    
    @pytest.fixture
    def service(self):
        """Create service instance for testing"""
        return ExportService()
    
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
    
    
    async def test_get_shortlist(self, service, mock_db, mock_user):
        """Test get_shortlist endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_shortlist = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_shortlist(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_shortlist.assert_called_once()

    async def test_get_add_to_project(self, service, mock_db, mock_user):
        """Test get_add_to_project endpoint"""
        # Arrange
        expected_data = [{"id": 1, "name": "Test"}]
        service.get_add_to_project = AsyncMock(return_value=expected_data)
        
        # Act
        result = await service.get_add_to_project(
            db=mock_db,
            user_id=mock_user.id,
            limit=10,
            offset=0
        )
        
        # Assert
        assert result == expected_data
        service.get_add_to_project.assert_called_once()

    
    # TODO: Add more test cases
